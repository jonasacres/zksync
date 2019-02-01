package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.SocketClosedException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.backedfs.BackedFS;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.PageQueue.EverythingQueueItem;
import com.acrescrypto.zksync.net.PageQueue.PageQueueItem;
import com.acrescrypto.zksync.net.PageQueue.QueueItem;
import com.acrescrypto.zksync.net.PageQueue.InodeContentsQueueItem;
import com.acrescrypto.zksync.net.PageQueue.RevisionQueueItem;
import com.acrescrypto.zksync.net.PeerConnection.PeerCapabilityException;
import com.acrescrypto.zksync.utility.Util;

public class PeerConnectionTest {
	class DummySwarm extends PeerSwarm {
		LinkedList<PeerAdvertisement> receivedAds = new LinkedList<PeerAdvertisement>();
		PeerConnection closedConnection;
		byte[] announcedTag;
		
		public DummySwarm(ZKArchiveConfig config) throws IOException {
			super(config);
		}
		
		@Override
		public void announceTag(byte[] tag) {
			announcedTag = tag;
		}
		
		@Override
		public void addPeerAdvertisement(PeerAdvertisement ad) {
			receivedAds.add(ad);
		}
		
		@Override
		public void closedConnection(PeerConnection connection) {
			closedConnection = connection;
		}
	}
	
	class DummyPeerMessageOutgoing extends PeerMessageOutgoing {
		public DummyPeerMessageOutgoing(PeerConnection connection, byte cmd, InputStream txPayload) {
			super(connection, cmd, txPayload);
		}
		
		@Override protected void runTxThread() {}
	}
	
	class DummySocket extends PeerSocket {
		int peerType = PeerConnection.PEER_TYPE_FULL;
		LinkedList<DummyPeerMessageOutgoing> messages = new LinkedList<DummyPeerMessageOutgoing>();
		boolean closed;
		
		public DummySocket(DummySwarm swarm) { super(swarm); }
		@Override public PeerAdvertisement getAd() { return null; }
		@Override public void write(byte[] data, int offset, int length) {}
		@Override public int read(byte[] data, int offset, int length) { return 0; }
		@Override public boolean isLocalRoleClient() { return false; }
		@Override public void _close() { closed = true; }
		@Override public boolean isClosed() { return closed; }
		@Override public void handshake(PeerConnection conn) {}
		@Override public int getPeerType() { return peerType; }
		@Override public byte[] getSharedSecret() { return null; }
		@Override public String getAddress() { return "127.0.0.1"; }
		@Override public int getPort() { return 1234; }
		@Override public synchronized DummyPeerMessageOutgoing makeOutgoingMessage(byte cmd, InputStream stream) {
			DummyPeerMessageOutgoing msg = new DummyPeerMessageOutgoing(connection, cmd, stream);
			messages.add(msg);
			this.notifyAll();
			return msg;
		}
		
		public synchronized DummyPeerMessageOutgoing popMessage() {
			while(messages.isEmpty()) {
				try {
					this.wait();
				} catch (InterruptedException e) {}
			}
			
			return messages.poll();
		}
	}
	
	class DummyTCPPeerSocketListener extends TCPPeerSocketListener {
		Socket connectedPeer;
		
		public DummyTCPPeerSocketListener(ZKMaster master, int port) throws IOException {
			super(master, port);
		}
		
		@Override
		protected void peerThread(Socket peerSocketRaw) {
			super.peerThread(peerSocketRaw);
			if(!peerSocketRaw.isClosed()) connectedPeer = peerSocketRaw;
		}
	}
	
	class DummyPeerMessageIncoming extends PeerMessageIncoming {
		public DummyPeerMessageIncoming(byte cmd) {
			this(cmd, 0);
		}
		
		public DummyPeerMessageIncoming(byte cmd, int msgId) {
			super(conn, cmd, (byte) 0, msgId);
		}
		
		public DummyPeerMessageIncoming(byte cmd, int msgId, byte[] data) {
			super(conn, cmd, (byte) 0, msgId);
			receivedData(FLAG_FINAL, data);
		}
		
		@Override protected void processThread() {} // we'll call handler from main thread manually
	}
	
	interface QueueItemTest {
		boolean test(QueueItem item);
	}
	
	CryptoSupport crypto;
	ZKMaster master;
	ZKArchive archive;
	
	DummySwarm swarm;
	DummySocket socket;
	PeerConnection conn;
	
	@SuppressWarnings("deprecation")
	void blindSwarmCache() {
		swarm.config.setStorage(new BackedFS(new RAMFS(), new RAMFS()));
	}
	
	void blindPeer() {
		socket.peerType = PeerConnection.PEER_TYPE_BLIND;
	}
	
	void assertReceivedCmd(byte cmd) {
		assertTrue(Util.waitUntil(100, ()->!socket.messages.isEmpty()));
		assertEquals(cmd, socket.messages.getFirst().cmd);
	}
	
	void assertReceivedPayload(byte[] payload) throws IOException {
		assertTrue(Util.waitUntil(100, ()->!socket.messages.isEmpty()));
		byte[] received = new byte[payload.length];
		socket.messages.getFirst().txPayload.read(received);
		assertTrue(Arrays.equals(payload, received));
	}
	
	void assertReceivedBytes(byte[] expectedNextBytes) throws IOException {
		byte[] received = new byte[expectedNextBytes.length];
		socket.messages.getFirst().txPayload.read(received);
		assertTrue(Arrays.equals(expectedNextBytes, received));
	}
	
	void assertReceivedAd(PeerAdvertisement ad) {
		assertTrue(swarm.receivedAds.contains(ad));
	}

	void assertUnreceivedAd(PeerAdvertisement ad) {
		assertFalse(swarm.receivedAds.contains(ad));
	}

	void assertFinished() throws IOException {
		assertTrue(socket.messages.getLast().txPayload.available() <= 0);
	}
	
	void assertNoMessage() {
		assertFalse(Util.waitUntil(100, ()->!socket.messages.isEmpty()));
	}
	
	void assertNoQueuedItemLike(QueueItemTest test) {
		synchronized(conn.queue) {
			Iterator<QueueItem> itr = conn.queue.itemsByPriority.iterator();
			boolean found = false;
			while(!found && itr.hasNext()) {
				found = test.test(itr.next());
			}
			
			assertFalse(found);
		}
	}
	
	void assertQueuedItemLike(QueueItemTest test) {
		synchronized(conn.queue) {
			Iterator<QueueItem> itr = conn.queue.itemsByPriority.iterator();
			boolean found = false;
			while(!found && itr.hasNext()) {
				found = test.test(itr.next());
			}
			
			assertTrue(found);
		}
	}
	
	TCPPeerAdvertisement makeTCPPeer(int index) {
		PublicDHKey pubKey = crypto.makePrivateDHKey().publicKey();
		byte[] encryptedArchiveId = archive.getConfig().getEncryptedArchiveId(pubKey.getBytes());
		return new TCPPeerAdvertisement(pubKey, "10.0.0."+index, 1000+index, encryptedArchiveId);
	}
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		crypto = master.getCrypto();
		
		swarm = new DummySwarm(archive.getConfig());
		socket = new DummySocket(swarm);
		conn = new PeerConnection(socket);
		conn.setLocalPaused(true);
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_TIPS);
		socket.messages.clear();
	}
	
	@After
	public void afterEach() throws IOException {
		swarm.close();
		socket.close();
		conn.close();
		archive.close();
		master.close();
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}

	@Test
	public void testConstructWithAdConnectsToAdvertisement() throws IOException, UnsupportedProtocolException, ProtocolViolationException, BlacklistedException, UnconnectableAdvertisementException {
		DummyTCPPeerSocketListener listener = new DummyTCPPeerSocketListener(master, 0);
		
		assertTrue(Util.waitUntil(100, ()->listener.listenSocket != null));
		listener.advertise(swarm);
		assertNull(listener.connectedPeer);
		
		byte[] encryptedArchiveId = swarm.config.getEncryptedArchiveId(listener.getIdentityKey().publicKey().getBytes());
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(listener.getIdentityKey().publicKey(), "localhost", listener.getPort(), encryptedArchiveId, 0).resolve();
		PeerConnection conn = new PeerConnection(swarm, ad);
		
		assertTrue(Util.waitUntil(100, ()->listener.connectedPeer != null));
		assertNotNull(listener.connectedPeer);
		assertNotNull(conn.queue);
		assertEquals(PeerConnection.PEER_TYPE_FULL, conn.getPeerType());
		
		conn.close();
		listener.close();
	}
	
	@Test
	public void testConstructWithAdConnectsToAdvertisementSetsPeerTypeBlindIfAdvertiserIsBlind() throws IOException, UnsupportedProtocolException, ProtocolViolationException, BlacklistedException, UnconnectableAdvertisementException {
		ArchiveAccessor roAccessor = archive.getConfig().getAccessor().makeSeedOnly();
		ZKArchiveConfig roConfig = ZKArchiveConfig.openExisting(roAccessor, archive.getConfig().getArchiveId());
		DummySwarm roSwarm = new DummySwarm(roConfig);
		DummyTCPPeerSocketListener listener = new DummyTCPPeerSocketListener(master, 0);
		
		assertTrue(Util.waitUntil(100, ()->listener.listenSocket != null));
		listener.advertise(roSwarm);
		assertNull(listener.connectedPeer);
		
		byte[] encryptedArchiveId = swarm.config.getEncryptedArchiveId(listener.getIdentityKey().publicKey().getBytes());
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(listener.getIdentityKey().publicKey(), "localhost", listener.getPort(), encryptedArchiveId, 0).resolve();
		PeerConnection conn = new PeerConnection(swarm, ad);
		
		assertTrue(Util.waitUntil(100, ()->listener.connectedPeer != null));
		assertNotNull(listener.connectedPeer);
		assertNotNull(conn.queue);
		assertEquals(PeerConnection.PEER_TYPE_BLIND, conn.getPeerType());
		
		roConfig.close();
		roSwarm.close();
		conn.close();
		listener.close();
	}
	
	@Test
	public void testConstructWithAdConnectsToAdvertisementSetsPeerTypeBlindIfLocalIsBlind() throws IOException, UnsupportedProtocolException, ProtocolViolationException, BlacklistedException, UnconnectableAdvertisementException {
		ArchiveAccessor roAccessor = archive.getConfig().getAccessor().makeSeedOnly();
		ZKArchiveConfig roConfig = ZKArchiveConfig.openExisting(roAccessor, archive.getConfig().getArchiveId());
		DummySwarm roSwarm = new DummySwarm(roConfig);
		DummyTCPPeerSocketListener listener = new DummyTCPPeerSocketListener(master, 0);

		assertTrue(Util.waitUntil(100, ()->listener.listenSocket != null));
		listener.advertise(swarm);
		assertNull(listener.connectedPeer);
		
		byte[] encryptedArchiveId = swarm.config.getEncryptedArchiveId(listener.getIdentityKey().publicKey().getBytes());
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(listener.getIdentityKey().publicKey(), "localhost", listener.getPort(), encryptedArchiveId, 0).resolve();
		PeerConnection conn = new PeerConnection(roSwarm, ad);
		
		assertTrue(Util.waitUntil(100, ()->listener.connectedPeer != null));
		assertNotNull(listener.connectedPeer);
		assertNotNull(conn.queue);
		assertEquals(PeerConnection.PEER_TYPE_BLIND, conn.getPeerType());

		roConfig.close();
		roSwarm.close();
		conn.close();
		listener.close();
	}
	
	@Test
	public void testGetSocketReturnsSocket() {
		assertEquals(socket, conn.getSocket());
	}
	
	@Test
	public void testAnnounceTag() throws IOException {
		conn.announceTag(1234);
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_TAGS);
		assertReceivedPayload(ByteBuffer.allocate(8).putLong(1234).array());
		assertFinished();
	}
	
	@Test
	public void testAnnounceTagAddsToQueueIfEverythingRequested() throws IOException {
		MutableBoolean passed = new MutableBoolean();
		
		conn.sendEverything();
		conn.setLocalPaused(true);
		conn.queue.itemsByPriority.clear();
		conn.announceTag(Util.shortTag(archive.getConfig().tag()));
		
		conn.queue.itemsByPriority.forEach((item)->{
			if(!(item instanceof PageQueueItem)) return;
			PageQueueItem pqItem = (PageQueueItem) item;
			if(Arrays.equals(archive.getConfig().tag(), pqItem.tag)) passed.setTrue();
		});
		
		assertTrue(passed.booleanValue());
	}
	
	@Test
	public void testAnnounceTagDoesNotAddToQueueIfEverythingNotRequested() {
		MutableBoolean passed = new MutableBoolean();
		
		conn.setLocalPaused(true);
		conn.queue.itemsByPriority.clear();
		conn.announceTag(Util.shortTag(archive.getConfig().tag()));
		
		conn.queue.itemsByPriority.forEach((item)->{
			if(!(item instanceof PageQueueItem)) return;
			PageQueueItem pqItem = (PageQueueItem) item;
			if(Arrays.equals(archive.getConfig().tag(), pqItem.tag)) passed.setTrue();
		});
		
		assertFalse(passed.booleanValue());
	}
	
	@Test
	public void testAnnounceShortTags() throws IOException {
		int numTags = 16;
		LinkedList<Long> tags = new LinkedList<>();
		ByteBuffer payload = ByteBuffer.allocate(numTags * 8);
		
		for(int i = 0; i < numTags; i++) {
			long shortTag = crypto.defaultPrng().getLong();
			tags.add(shortTag);
			payload.putLong(shortTag);
		}
		
		assertFalse(payload.hasRemaining());
		conn.announceShortTags(tags);
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_TAGS);
		assertReceivedPayload(payload.array());
		assertFinished();
	}
	
	@Test
	public void testAnnounceTags() throws IOException {
		int numTags = 16;
		LinkedList<byte[]> tags = new LinkedList<byte[]>();
		ByteBuffer payload = ByteBuffer.allocate(numTags * 8);
		
		for(int i = 0; i < numTags; i++) {
			byte[] tag = crypto.rng(crypto.hashLength());
			tags.add(tag);
			payload.putLong(Util.shortTag(tag));
		}
		
		assertFalse(payload.hasRemaining());
		conn.announceTags(tags);
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_TAGS);
		assertReceivedPayload(payload.array());
		assertFinished();
	}
	
	@Test
	public void testAnnounceSelfWithAdSendsAnnouncement() throws UnconnectableAdvertisementException, IOException {
		PeerAdvertisement ad = makeTCPPeer(0);
		conn.announceSelf(ad);
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_SELF_AD);
		assertReceivedBytes(ByteBuffer.allocate(2).putShort((short) ad.serialize().length).array());
		assertReceivedBytes(ad.serialize());
		assertFinished();
	}
	
	@Test
	public void testAnnounceSelfWithoutAdDoesntAnnounceIfNoTcpListener() {
		conn.announceSelf();
		assertNoMessage();
	}
	
	@Test
	public void testAnnounceSelfWithoutAdDoesntAnnounceIfNoSwarmListener() throws IOException {
		master.getGlobalConfig().set("net.swarm.enabled", true);
		conn.announceSelf();
		assertNoMessage();
	}
	
	@Test
	public void testAnnounceSelfWIthoutAdAnnouncesLocalAdFromSwarmListener() throws IOException, UnconnectableAdvertisementException {
		master.getTCPListener().startListening(0);
		master.getTCPListener().advertise(swarm);
		TCPPeerAdvertisement ad = master.getTCPListener().listenerForSwarm(swarm).localAd();
		conn.announceSelf(ad);
		
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_SELF_AD);
		assertReceivedBytes(ByteBuffer.allocate(2).putShort((short) ad.serialize().length).array());
		assertReceivedBytes(ad.serialize());
		assertFinished();
	}
	
	@Test
	public void testAnnouncePeer() throws UnconnectableAdvertisementException, IOException {
		PeerAdvertisement ad = makeTCPPeer(0);
		conn.announcePeer(ad);
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_PEERS);
		assertReceivedBytes(ByteBuffer.allocate(2).putShort((short) ad.serialize().length).array());
		assertReceivedBytes(ad.serialize());
		assertFinished();
	}
	
	@Test
	public void testAnnouncePeers() throws UnconnectableAdvertisementException, IOException {
		int numPeers = 16;
		LinkedList<PeerAdvertisement> ads = new LinkedList<PeerAdvertisement>();
		
		for(int i = 0; i < numPeers; i++) {
			PeerAdvertisement ad = makeTCPPeer(i);
			ads.add(ad);
		}
		
		conn.announcePeers(ads);
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_PEERS);
		for(PeerAdvertisement ad : ads) {
			byte[] serialized = ad.serialize();
			assertReceivedBytes(ByteBuffer.allocate(2).putShort((short) serialized.length).array());
			assertReceivedBytes(serialized);
		}
		assertFinished();
	}
	
	@Test
	public void testAnnounceTips() throws IOException {
		ZKFS fs = archive.openBlank();
		fs.write("path", "test".getBytes());
		fs.commit();
		
		fs = archive.openBlank();
		fs.write("path", "fork".getBytes());
		fs.commit();

		fs = archive.openBlank();
		fs.write("path", "fork two".getBytes());
		fs.commit();

		conn.announceTips();
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_TIPS);
		for(RevisionTag tag : archive.getConfig().getRevisionList().branchTips()) {
			assertReceivedBytes(tag.serialize());
		}
		assertFinished();
	}
	
	@Test
	public void testAnnounceRevisionDetails() throws IOException {
		RevisionTag tag = archive.openBlank().commit();
		conn.announceRevisionDetails(tag);
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_REVISION_DETAILS);
		assertReceivedBytes(tag.serialize());
		assertReceivedBytes(RevisionTag.blank(archive.getConfig()).getBytes());
		assertFinished();
	}
	
	@Test
	public void testRequestAll() throws IOException {
		conn.requestAll();
		assertReceivedCmd(PeerConnection.CMD_REQUEST_ALL);
		assertFinished();
	}
	
	@Test
	public void testRequestAllCancel() throws IOException {
		conn.requestAllCancel();
		assertReceivedCmd(PeerConnection.CMD_REQUEST_ALL_CANCEL);
		assertFinished();
	}
	
	@Test
	public void testRequestPageTag() throws IOException {
		conn.requestPageTag(4321, 1234);
		assertReceivedCmd(PeerConnection.CMD_REQUEST_PAGE_TAGS);
		assertReceivedBytes(Util.serializeInt(4321));
		assertReceivedBytes(Util.serializeLong(1234));
		assertFinished();
	}
	
	@Test
	public void testRequestPageTags() throws IOException {
		int numTags = 16;
		ArrayList<Long> pageTags = new ArrayList<>(numTags);
		for(int i = 0; i < numTags; i++) {
			// page tags of various lengths; prove that we only use the first 8 bytes (short tag)
			pageTags.add(Util.shortTag(crypto.rng(RefTag.REFTAG_SHORT_SIZE)));
		}
		
		conn.requestPageTags(Integer.MAX_VALUE, pageTags);
		assertReceivedCmd(PeerConnection.CMD_REQUEST_PAGE_TAGS);
		assertReceivedBytes(Util.serializeInt(Integer.MAX_VALUE));
		for(Long shortTag : pageTags) {
			ByteBuffer shortTagBytes = ByteBuffer.allocate(RefTag.REFTAG_SHORT_SIZE);
			shortTagBytes.putLong(shortTag);
			assertReceivedBytes(shortTagBytes.array());
		}
		assertFinished();
	}
	
	@Test
	public void testRequestInodes() throws PeerCapabilityException, IOException {
		int numInodes = 16;
		RefTag refTag = new RefTag(archive, crypto.rng(crypto.hashLength()), 1, 1);
		RevisionTag revTag = new RevisionTag(refTag, 0, 0);
		ArrayList<Long> inodeIds = new ArrayList<>(numInodes);
		
		for(int i = 0; i < numInodes; i++) {
			inodeIds.add(crypto.defaultPrng().getLong());
		}
		
		conn.requestInodes(Integer.MIN_VALUE, revTag, inodeIds);
		assertReceivedCmd(PeerConnection.CMD_REQUEST_INODES);
		assertReceivedBytes(Util.serializeInt(Integer.MIN_VALUE));
		assertReceivedBytes(revTag.getBytes());
		for(Long inodeId : inodeIds) assertReceivedBytes(Util.serializeLong(inodeId));
		assertFinished();
	}
	
	@Test
	public void testRequestInodesThrowsExceptionIfNotFullPeer() throws PeerCapabilityException {
		blindPeer();
		try {
			RefTag refTag = new RefTag(archive, crypto.rng(crypto.hashLength()), 1, 1);
			RevisionTag revTag = new RevisionTag(refTag, 0, 0);
			ArrayList<Long> inodeIds = new ArrayList<>(1);
			inodeIds.add(crypto.defaultPrng().getLong());
			conn.requestInodes(0, revTag, inodeIds);
			fail();
		} catch(PeerCapabilityException exc) {
			assertNoMessage();
		}
	}
	
	@Test
	public void testRequestRevisionContents() throws PeerCapabilityException, IOException {
		int numTags = 16;
		ArrayList<RevisionTag> tags = new ArrayList<>(numTags);
		
		for(int i = 0; i < numTags; i++) {
			RefTag refTag = new RefTag(archive, crypto.rng(crypto.hashLength()), 1, 1);
			RevisionTag revTag = new RevisionTag(refTag, 0, 0); 
			tags.add(revTag);
		}
		
		conn.requestRevisionContents(-1, tags);
		assertReceivedCmd(PeerConnection.CMD_REQUEST_REVISION_CONTENTS);
		assertReceivedBytes(Util.serializeInt(-1));
		for(RevisionTag tag : tags) assertReceivedBytes(tag.getBytes());
		assertFinished();
	}
	
	@Test
	public void testRequestRevisionContentsThrowsExceptionIfNotFullPeer() {
		blindPeer();
		try {
			ArrayList<RevisionTag> tags = new ArrayList<>(1);
			RefTag refTag = new RefTag(archive, crypto.rng(crypto.hashLength()), 1, 1);
			RevisionTag revTag = new RevisionTag(refTag, 0, 0);
			tags.add(revTag);
			conn.requestRevisionContents(0, tags);
			fail();
		} catch(PeerCapabilityException exc) {
			assertNoMessage();
		}
	}
	
	@Test
	public void testRequestRevisionDetailsSingle() throws PeerCapabilityException, IOException {
		RefTag refTag = new RefTag(archive, crypto.rng(crypto.hashLength()), 1, 1);
		RevisionTag revTag = new RevisionTag(refTag, 0, 0);
		conn.requestRevisionDetails(10, revTag);
		assertReceivedCmd(PeerConnection.CMD_REQUEST_REVISION_DETAILS);
		assertReceivedBytes(Util.serializeInt(10));
		assertReceivedBytes(revTag.getBytes());
	}
	
	@Test
	public void testRequestRevisionDetailsSingleThrowsExceptionIfNotFullPeer() throws PeerCapabilityException {
		blindPeer();
		try {
			conn.requestRevisionDetails(10, RevisionTag.blank(archive.getConfig()));
		} catch(PeerCapabilityException exc) {
			assertNoMessage();
		}
	}
	
	@Test
	public void testRequestRevisionDetailsMultiple() throws PeerCapabilityException, IOException {
		ArrayList<RevisionTag> tags = new ArrayList<>();
		for(int i = 0; i < 16; i++) {
			RefTag refTag = new RefTag(archive, crypto.rng(crypto.hashLength()), 1, 1);
			tags.add(new RevisionTag(refTag, 0, 0));
		}
		conn.requestRevisionDetails(20, tags);
		assertReceivedCmd(PeerConnection.CMD_REQUEST_REVISION_DETAILS);
		assertReceivedBytes(Util.serializeInt(20));
		for(RevisionTag tag : tags) {
			assertReceivedBytes(tag.getBytes());
		}
	}

	@Test
	public void testRequestRevisionDetailsMultipleThrowsExceptionIfNotFullPeer() throws PeerCapabilityException {
		blindPeer();
		ArrayList<RevisionTag> list = new ArrayList<>();
		list.add(RevisionTag.blank(archive.getConfig()));
		
		try {
			conn.requestRevisionDetails(10, list);
		} catch(PeerCapabilityException exc) {
			assertNoMessage();
		}
	}

	@Test
	public void testSetPausedEnabled() throws IOException {
		conn.setPaused(true);
		assertReceivedCmd(PeerConnection.CMD_SET_PAUSED);
		assertReceivedBytes(new byte[] {0x01});
		assertFinished();
	}
	
	@Test
	public void testSetPausedDisabled() throws IOException {
		conn.setPaused(false);
		assertReceivedCmd(PeerConnection.CMD_SET_PAUSED);
		assertReceivedBytes(new byte[] {0x00});
		assertFinished();
	}
	
	@Test
	public void testHandleIgnoresNegativeCommands() throws ProtocolViolationException {
		for(byte i = Byte.MIN_VALUE; i < 0; i++) {
			assertFalse(conn.handle(new DummyPeerMessageIncoming(i, 0, new byte[0])));
		}
	}
	
	@Test
	public void testHandleIgnoresUnsupportedCommands() throws ProtocolViolationException {
		for(byte i = PeerConnection.MAX_SUPPORTED_CMD+1; i < Byte.MAX_VALUE; i++) {
			assertFalse(conn.handle(new DummyPeerMessageIncoming(i, 0, new byte[0])));
		}
	}
	
	@Test
	public void testHandleAnnouncePeersCallSwarmAddPeerAdvertisement() throws UnconnectableAdvertisementException, ProtocolViolationException {
		PeerAdvertisement[] ads = new PeerAdvertisement[16];
		for(int i = 0; i < ads.length; i++) {
			ads[i] = makeTCPPeer(i);
		}
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_PEERS);
		for(PeerAdvertisement ad : ads) {
			byte[] serialized = ad.serialize();
			msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) serialized.length).array());
			msg.receivedData((byte) 0, serialized);
		}
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		
		conn.handle(msg);
		for(PeerAdvertisement ad : ads) {
			assertReceivedAd(ad);
		}
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleAnnouncePeersTriggersViolationForNegativeAdLength() throws ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_PEERS);
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) -1).array());
		conn.handle(msg);
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleAnnouncePeersTriggersViolationForZeroAdLength() throws ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_PEERS);
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) 0).array());
		conn.handle(msg);
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleAnnouncePeersTriggersViolationForExcessiveAdLength() throws ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_PEERS);
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) (PeerMessage.MESSAGE_SIZE+1)).array());
		conn.handle(msg);
	}
	
	@Test
	public void testHandleAnnouncePeersWorksForSeedOnly() throws ProtocolViolationException, IOException, UnconnectableAdvertisementException {
		blindPeer();
		testHandleAnnouncePeersCallSwarmAddPeerAdvertisement();
	}
	
	@Test
	public void testHandleAnnouncePeersToleratesIndecipherableAds() throws ProtocolViolationException, UnconnectableAdvertisementException {
		PeerAdvertisement[] ads = new PeerAdvertisement[2];
		ads[0] = makeTCPPeer(0);
		ads[1] = makeTCPPeer(1);
		
		ByteBuffer buf = ByteBuffer.allocate(2);
		buf.put((byte) -1);
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_PEERS);
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) ads[0].serialize().length).array());
		msg.receivedData((byte) 0, ads[0].serialize());
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) buf.capacity()).array());
		msg.receivedData((byte) 0, buf.array());
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) ads[1].serialize().length).array());
		msg.receivedData((byte) 0, ads[1].serialize());
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		
		conn.handle(msg);
		assertReceivedAd(ads[0]);
		assertReceivedAd(ads[1]);
	}
	
	@Test
	public void testHandleAnnouncePeersSkipsAdsForBlacklistedPeers() throws ProtocolViolationException, UnconnectableAdvertisementException, IOException {
		TCPPeerAdvertisement[] ads = new TCPPeerAdvertisement[3];
		for(int i = 0; i < ads.length; i++) {
			ads[i] = makeTCPPeer(i).resolve();
		}
		
		master.getBlacklist().add(ads[1].ipAddress, 60000);
		assertTrue(ads[1].isBlacklisted(master.getBlacklist()));
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_PEERS);
		for(PeerAdvertisement ad : ads) {
			msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) ad.serialize().length).array());
			msg.receivedData((byte) 0, ad.serialize());
		}
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		
		conn.handle(msg);
		assertReceivedAd(ads[0]);
		assertUnreceivedAd(ads[1]);
		assertReceivedAd(ads[2]);
	}
	
	@Test
	public void testHandleAnnounceSelfAdCallSwarmAddPeerAdvertisement() throws UnconnectableAdvertisementException, ProtocolViolationException {
		TCPPeerAdvertisement ad = makeTCPPeer(0);
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_SELF_AD);
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) ad.serialize().length).array());
		msg.receivedData((byte) 0, ad.serialize());
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		
		assertEquals(0, swarm.receivedAds.size());
		conn.handle(msg);
		assertEquals(1, swarm.receivedAds.size());
		assertEquals(ad.port, ((TCPPeerAdvertisement) swarm.receivedAds.getFirst()).port);
		assertEquals(socket.getAddress(), ((TCPPeerAdvertisement) swarm.receivedAds.getFirst()).host);
		assertTrue(Arrays.equals(ad.pubKey.getBytes(), ((TCPPeerAdvertisement) swarm.receivedAds.getFirst()).pubKey.getBytes()));
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleAnnounceSelfAdTriggersViolationForNegativeAdLength() throws ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_SELF_AD);
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) -1).array());
		conn.handle(msg);
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleAnnounceSelfAdTriggersViolationForZeroAdLength() throws ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_SELF_AD);
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) 0).array());
		conn.handle(msg);
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleAnnounceSelfAdTriggersViolationForExcessiveAdLength() throws ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_SELF_AD);
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) (PeerMessage.MESSAGE_SIZE+1)).array());
		conn.handle(msg);
	}
	
	@Test
	public void testHandleAnnounceSelfAdWorksForSeedOnly() throws ProtocolViolationException, IOException, UnconnectableAdvertisementException {
		blindPeer();
		testHandleAnnounceSelfAdCallSwarmAddPeerAdvertisement();
	}
	
	@Test
	public void testHandleAnnounceSelfAdToleratesIndecipherableAds() throws ProtocolViolationException, UnconnectableAdvertisementException {
		ByteBuffer buf = ByteBuffer.allocate(2);
		buf.put((byte) -1);
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_SELF_AD);
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) buf.capacity()).array());
		msg.receivedData((byte) 0, buf.array());
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		
		conn.handle(msg);
		assertEquals(0, swarm.receivedAds.size());
	}
	
	@Test
	public void testHandleAnnounceSelfAdSkipsAdsForBlacklistedPeers() throws ProtocolViolationException, UnconnectableAdvertisementException, IOException {
		TCPPeerAdvertisement ad = makeTCPPeer(0);
		master.getBlacklist().add("127.0.0.1", 60000);
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_SELF_AD);
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) ad.serialize().length).array());
		msg.receivedData((byte) 0, ad.serialize());
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		
		conn.handle(msg);
		assertEquals(0, swarm.receivedAds.size());
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleAnnounceSelfAdDoesntAllowMultipleAds() throws UnconnectableAdvertisementException, ProtocolViolationException {
		TCPPeerAdvertisement ad = makeTCPPeer(0);
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_SELF_AD);
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) ad.serialize().length).array());
		msg.receivedData((byte) 0, ad.serialize());
		msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort((short) ad.serialize().length).array());
		msg.receivedData((byte) 0, ad.serialize());
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		
		conn.handle(msg);
	}
	
	@Test
	public void testHandleAnnounceTagsUpdatesAnnouncedTagsList() throws ProtocolViolationException {
		byte[] tagList = crypto.rng(8*32);
		ByteBuffer buf = ByteBuffer.wrap(tagList);

		while(buf.hasRemaining()) {
			assertFalse(conn.hasFile(buf.getLong()));
		}

		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_TAGS);
		msg.receivedData(PeerMessage.FLAG_FINAL, tagList);
		conn.handle(msg);
		
		while(buf.hasRemaining()) {
			assertTrue(conn.hasFile(buf.getLong()));
		}
	}
	
	@Test
	public void testHandleAnnounceTagsWorksForSeedOnly() throws ProtocolViolationException, IOException, UnconnectableAdvertisementException {
		blindPeer();
		testHandleAnnounceTagsUpdatesAnnouncedTagsList();
	}
	
	@Test
	public void testHandleAnnounceTipsAddsBranchTipsToRevisionTree() throws ProtocolViolationException, IOException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_TIPS);
		RevisionTag[] tags = new RevisionTag[16];

		for(int i = 0; i < tags.length; i++) {
			RefTag refTag = new RefTag(archive, crypto.rng(crypto.hashLength()), RefTag.REF_TYPE_2INDIRECT, 2);
			tags[i] = new RevisionTag(refTag, 0, 0);
			msg.receivedData((byte) 0, tags[i].serialize());
			assertFalse(archive.getConfig().getRevisionList().branchTips().contains(tags[i]));
		}
		
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		archive.getConfig().getRevisionList().read();
		for(int i = 0; i < tags.length; i++) {
			assertTrue(archive.getConfig().getRevisionList().branchTips().contains(tags[i]));
		}
	}
	
	@Test
	public void testHandleAnnounceTipsTriggersViolationWhenForgedRefTagSent() throws ProtocolViolationException, IOException {
		RefTag refTag = new RefTag(archive.getConfig(), crypto.rng(crypto.hashLength()), RefTag.REF_TYPE_2INDIRECT, 2);
		RevisionTag fakeTag = new RevisionTag(refTag, 0, 0);
		fakeTag.getBytes()[8] ^= 0x40;
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_TIPS);
		msg.receivedData(PeerMessage.FLAG_FINAL, fakeTag.getBytes());
		assertFalse(archive.getConfig().getRevisionList().branchTips().contains(fakeTag));
		try {
			conn.handle(msg);
			fail();
		} catch(ProtocolViolationException exc) {
		}
		assertFalse(archive.getConfig().getRevisionList().branchTips().contains(fakeTag));
	}
	
	@Test
	public void testHandleAnnounceTipsWorksForSeedOnly() throws ProtocolViolationException, IOException, UnconnectableAdvertisementException {
		blindPeer();
		testHandleAnnounceTipsAddsBranchTipsToRevisionTree();
	}
	
	@Test
	public void testHandleAnnounceRevisionDetailsAddsInfoToRevisionTree() throws ProtocolViolationException, IOException {
		RevisionTag tag = conn.socket.swarm.config.getArchive().openBlank().commit().getFS().commit();
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_REVISION_DETAILS);
		msg.receivedData((byte) 0, tag.getBytes());
		for(RevisionTag parent : tag.getFS().getRevisionInfo().getParents()) {
			msg.receivedData((byte) 0, parent.getBytes());
		}
		
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		
		conn.socket.swarm.config.getRevisionTree().clear();
		conn.socket.swarm.config.getCacheStorage().purge();
		assertFalse(conn.socket.swarm.config.getRevisionTree().hasParentsForTag(tag));
		conn.handle(msg);
		assertTrue(conn.socket.swarm.config.getRevisionTree().hasParentsForTag(tag));
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleAnnounceRevisionDetailsTriggersViolationIfParentsNotCorrect() throws IOException, ProtocolViolationException {
		RevisionTag tag = conn.socket.swarm.config.getArchive().openBlank().commit().getFS().commit();
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_REVISION_DETAILS);
		msg.receivedData((byte) 0, tag.getBytes());
		msg.receivedData(PeerMessage.FLAG_FINAL, RevisionTag.blank(archive.getConfig()).getBytes());
		conn.socket.swarm.config.getRevisionTree().clear();
		conn.handle(msg);
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleAnnounceRevisionDetailsTriggersViolationIfNotFullPeer() throws ProtocolViolationException {
		blindPeer();
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_REVISION_DETAILS);
		msg.receivedData((byte) 0, RevisionTag.blank(archive.getConfig()).getBytes());
		conn.handle(msg);
	}
	
	@Test
	public void testHandleRequestAllCausesPageQueueToSendEverything() throws ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_ALL);
		assertNoQueuedItemLike((item) -> (item instanceof EverythingQueueItem));
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		assertQueuedItemLike((item) -> (item instanceof EverythingQueueItem));
	}
	
	@Test
	public void testHandleRequestAllWorksForSeedOnly() throws ProtocolViolationException, IOException, UnconnectableAdvertisementException {
		blindPeer();
		testHandleRequestAllCausesPageQueueToSendEverything();
	}
	
	@Test
	public void testHandleRequestAllCancelCausesPageQueueToStopSendingEverything() throws ProtocolViolationException, IOException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_ALL_CANCEL);
		conn.sendEverything();
		assertQueuedItemLike((item) -> (item instanceof EverythingQueueItem));
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		assertNoQueuedItemLike((item) -> (item instanceof EverythingQueueItem));
	}
	
	@Test
	public void testHandleRequestAllCancelWorksForSeedOnly() throws ProtocolViolationException, IOException, UnconnectableAdvertisementException {
		blindPeer();
		testHandleRequestAllCancelCausesPageQueueToStopSendingEverything();
	}
	
	@Test
	public void testRequestInodesAddsRequestedInodesToPageQueue() throws ProtocolViolationException, IOException {
		ZKFS fs = archive.openBlank();
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_INODES);
		Inode[] inodes = new Inode[16];

		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());

		for(int i = 0; i < inodes.length; i++) {
			fs.write("file"+i, new byte[archive.getConfig().getPageSize()]);
			inodes[i] = fs.inodeForPath("file"+i);
		}
		
		msg.receivedData((byte) 0, fs.commit().getBytes());
		
		for(Inode inode : inodes) {
			msg.receivedData((byte) 0, Util.serializeLong(inode.getStat().getInodeId()));
		}

		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		for(Inode inode : inodes) {
			assertQueuedItemLike((_item) -> {
				if(!(_item instanceof InodeContentsQueueItem)) return false;
				InodeContentsQueueItem item = (InodeContentsQueueItem) _item;
				return inode.getStat().getInodeId() == item.tree.getInodeId();
			});
		}
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testRequestInodesTriggersViolationIfNotFullPeer() throws ProtocolViolationException, IOException {
		blindPeer();
		testRequestInodesAddsRequestedInodesToPageQueue();
	}
	
	@Test
	public void testHandleRequestInodesToleratesNonexistentInodeIds() throws IOException, ProtocolViolationException {
		ZKFS fs = archive.openBlank();
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_INODES);
		Inode[] inodes = new Inode[16];
		
		// priority
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());
		
		for(int i = 0; i < inodes.length; i++) {
			fs.write("file"+i, new byte[archive.getConfig().getPageSize()]);
			inodes[i] = fs.inodeForPath("file"+i);
		}
		
		// rev tag
		msg.receivedData((byte) 0, fs.commit().getBytes());
		
		for(int i = 0; i < inodes.length; i++) {
			if(i != inodes.length/2) {
				msg.receivedData((byte) 0, Util.serializeLong(inodes[i].getStat().getInodeId()));
			} else {
				msg.receivedData((byte) 0, Util.serializeLong(fs.getInodeTable().nextInodeId()));
			}
		}
		
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		for(int i = 0; i < inodes.length; i++) {
			if(i == inodes.length/2) continue;
			final Inode inode = inodes[i];
			
			assertQueuedItemLike((_item) -> {
				if(!(_item instanceof InodeContentsQueueItem)) return false;
				InodeContentsQueueItem item = (InodeContentsQueueItem) _item;
				return item.tree != null && inode.getStat().getInodeId() == item.tree.getInodeId();
			});
		}
	}
	
	@Test
	public void testHandleRequestInodesToleratesNonexistentRevTags() throws IOException, ProtocolViolationException {
		RefTag refTag = new RefTag(archive, crypto.rng(archive.getConfig().refTagSize()));
		RevisionTag fakeTag = new RevisionTag(refTag, 0, 1);
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_INODES);
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array()); // priority
		msg.receivedData((byte) 0, fakeTag.getBytes());
		msg.receivedData((byte) 0, crypto.rng(8*16)); // random inode IDs
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		assertNoMessage();
	}
	
	@Test
	public void testHandleRequestInodesDoesntTriggerViolationOnInvalidRevTags() throws IOException, ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_INODES);
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array()); // priority
		msg.receivedData((byte) 0, crypto.rng(archive.getConfig().refTagSize())); // fake revtag
		msg.receivedData((byte) 0, crypto.rng(8*16)); // random inode IDs
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		assertNoMessage();
	}
	
	@Test
	public void testHandleRequestInodesToleratesDeletedInodeIds() throws IOException, ProtocolViolationException {
		ZKFS fs = archive.openBlank();
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_INODES);
		Inode[] inodes = new Inode[16];
		
		// priority
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());
		
		for(int i = 0; i < inodes.length; i++) {
			fs.write("file"+i, new byte[archive.getConfig().getPageSize()]);
			inodes[i] = fs.inodeForPath("file"+i);
		}
		
		fs.commit();
		fs.unlink("file" + (inodes.length/2));
		
		// rev tag
		msg.receivedData((byte) 0, fs.commit().getBytes());
		
		for(int i = 0; i < inodes.length; i++) {
			msg.receivedData((byte) 0, Util.serializeLong(inodes[i].getStat().getInodeId()));
		}
		
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		for(int i = 0; i < inodes.length; i++) {
			final Inode inode = inodes[i];
			QueueItemTest test = (_item)->{
					if(!(_item instanceof InodeContentsQueueItem)) return false;
					InodeContentsQueueItem item = (InodeContentsQueueItem) _item;
					return item.tree != null && inode.getStat().getInodeId() == item.tree.getInodeId();
				};
			if(i == inodes.length/2) {
				assertNoQueuedItemLike(test);
			} else {
				assertQueuedItemLike(test);
			}
		}
	}
	
	@Test
	public void testHandleRequestRevisionContentsAddsRequestedRevTagToPageQueue() throws IOException, ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_REVISION_CONTENTS);
		RevisionTag[] tags = new RevisionTag[16];
		
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());
		for(int i = 0; i < tags.length; i++) {
			ZKFS fs = archive.openBlank();
			fs.write("file"+i, "content".getBytes());
			tags[i] = fs.commit();
			msg.receivedData((byte) 0, tags[i].getBytes());
		}
		
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		for(RevisionTag tag : tags) {
			assertQueuedItemLike((_item) -> {
				if(!(_item instanceof RevisionQueueItem)) return false;
				RevisionQueueItem item = (RevisionQueueItem) _item;
				return tag.equals(item.revTag);
			});
		}
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleRequestRevisionContentsTriggersViolationIfNotFullPeer() throws IOException, ProtocolViolationException {
		blindPeer();
		testHandleRequestRevisionContentsAddsRequestedRevTagToPageQueue();
	}
	
	@Test
	public void testHandleRequestRevisionContentsToleratesNonexistentRevTags() throws IOException, ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_REVISION_CONTENTS);
		RevisionTag[] tags = new RevisionTag[16];
		
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());
		for(int i = 0; i < tags.length; i++) {
			ZKFS fs = archive.openBlank();
			fs.write("file"+i, "content".getBytes());
			tags[i] = fs.commit();
			msg.receivedData((byte) 0, tags[i].getBytes());
			if(i == tags.length/2) {
				RefTag refTag = new RefTag(archive, crypto.rng(archive.getConfig().refTagSize()));
				RevisionTag fakeRevTag = new RevisionTag(refTag, 0, 0);
				msg.receivedData((byte) 0, fakeRevTag.getBytes());
			}
		}
		
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		for(RevisionTag tag : tags) {
			assertQueuedItemLike((_item) -> {
				if(!(_item instanceof RevisionQueueItem)) return false;
				RevisionQueueItem item = (RevisionQueueItem) _item;
				return tag.equals(item.revTag);
			});
		}
	}
	
	@Test
	public void testHandleRequestPageTagsAddsRequestedShortTagsToPageQueue() throws ProtocolViolationException, IOException {
		byte[][] tags = new byte[16][];
		ZKFS fs = archive.openBlank();		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_PAGE_TAGS);
		fs.write("file", new byte[tags.length*archive.getConfig().getPageSize()]);
		fs.commit();
		PageTree treee = new PageTree(fs.inodeForPath("file"));
		
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());
		for(int i = 0; i < tags.length; i++) {
			tags[i] = treee.getPageTag(i);
			msg.receivedData((byte) 0, ByteBuffer.allocate(8).putLong(Util.shortTag(tags[i])).array());
		}
		
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		for(byte[] tag : tags) {
			assertQueuedItemLike((_item) -> {
				if(!(_item instanceof PageQueueItem)) return false;
				PageQueueItem item = (PageQueueItem) _item;
				return Arrays.equals(tag, item.tag);
			});
		}
	}
	
	@Test
	public void testHandleRequestPageTagsToleratesNonexistentTags() throws ProtocolViolationException, IOException {
		byte[][] tags = new byte[16][];
		ZKFS fs = archive.openBlank();		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_PAGE_TAGS);
		fs.write("file", new byte[tags.length*archive.getConfig().getPageSize()]);
		fs.commit();
		PageTree tree = new PageTree(fs.inodeForPath("file"));
		
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());
		for(int i = 0; i < tags.length; i++) {
			tags[i] = tree.getPageTag(i);
			msg.receivedData((byte) 0, ByteBuffer.allocate(8).putLong(Util.shortTag(tags[i])).array());
			if(i == tags.length/2) msg.receivedData((byte) 0, ByteBuffer.allocate(8).put(crypto.rng(8)).array());
		}
		
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		for(byte[] tag : tags) {
			assertQueuedItemLike((_item) -> {
				if(!(_item instanceof PageQueueItem)) return false;
				PageQueueItem item = (PageQueueItem) _item;
				return Arrays.equals(tag, item.tag);
			});
		}
	}
	
	@Test
	public void testHandleRequestTagsWorksForSeedOnly() throws ProtocolViolationException, IOException {
		blindPeer();
		testHandleRequestPageTagsToleratesNonexistentTags();
	}
	
	@Test
	public void testHandleRequestRevisionDetailsRespondsWithRequestedInformation() throws IOException, ProtocolViolationException {
		RevisionTag tag = conn.socket.swarm.config.getArchive().openBlank().commit().getFS().commit();
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_REVISION_DETAILS);
		msg.receivedData((byte) 0, Util.serializeInt(0));
		msg.receivedData(PeerMessage.FLAG_FINAL, tag.getBytes());
		conn.handle(msg);
		
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_REVISION_DETAILS);
		assertReceivedBytes(tag.getBytes());
		assertReceivedBytes(tag.getInfo().getParents().get(0).getBytes());
		assertFinished();
	}
	
	@Test
	public void testHandleRequestRevisionDetailsToleratesTruncatedRevTags() throws IOException, ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_REVISION_DETAILS);
		msg.receivedData((byte) 0, Util.serializeInt(0));
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[RevisionTag.sizeForConfig(archive.getConfig())-1]);
		conn.handle(msg);
	}
	
	@Test
	public void testHandleRequestRevisionDetailsIgnoresRequestIfRevisionNotFound() throws IOException, ProtocolViolationException {
		RevisionTag tag = conn.socket.swarm.config.getArchive().openBlank().commit().getFS().commit();
		conn.socket.swarm.config.getRevisionTree().clear();
		conn.socket.swarm.config.getCacheStorage().purge();
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_REVISION_DETAILS);
		msg.receivedData((byte) 0, Util.serializeInt(0));
		msg.receivedData(PeerMessage.FLAG_FINAL, tag.getBytes());
		conn.handle(msg);
		assertNoMessage();
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleRequestRevisionDetailsTriggersViolationIfNotFullPeer() throws IOException, ProtocolViolationException {
		blindPeer();
		testHandleRequestRevisionDetailsRespondsWithRequestedInformation();
	}
	
	@Test
	public void testHandleSendPageAddsChunksToChunkAccumulator() throws IOException, ProtocolViolationException {
		blindSwarmCache();
		ZKFS fs = archive.openBlank();
		fs.write("file", new byte[archive.getConfig().getPageSize()]);
		byte[] tag = new PageTree(fs.inodeForPath("file")).getPageTag(0);
		
		ByteBuffer buf = ByteBuffer.wrap(archive.getStorage().read(Page.pathForTag(tag)));
		int numChunks = (int) Math.ceil(((double) buf.limit())/PeerMessage.FILE_CHUNK_SIZE);
		ChunkAccumulator accumulator = socket.swarm.accumulatorForTag(tag);
		DummyPeerMessageIncoming msg;
		
		assertFalse(accumulator.finished);
		
		msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_SEND_PAGE);
		msg.receivedData((byte) 0, tag);
		for(int i = 0; i < numChunks/2; i++) {
			byte[] chunk = new byte[Math.min(buf.remaining(), PeerMessage.FILE_CHUNK_SIZE)];
			buf.get(chunk);
			msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(i).array());
			msg.receivedData((byte) 0, chunk);
		}
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		
		conn.handle(msg);
		assertFalse(accumulator.finished);

		msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_SEND_PAGE, 1);
		msg.receivedData((byte) 0, tag);
		for(int i = numChunks/2; i < numChunks; i++) {
			byte[] chunk = new byte[Math.min(buf.remaining(), PeerMessage.FILE_CHUNK_SIZE)];
			buf.get(chunk);
			msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(i).array());
			msg.receivedData((byte) 0, chunk);
		}
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		
		conn.handle(msg);
		assertTrue(accumulator.isFinished());
		assertTrue(Arrays.equals(tag, swarm.announcedTag));
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleSendPageTriggersViolationIfChunkHasNegativeOffset() throws IOException, ProtocolViolationException {
		blindSwarmCache();
		ZKFS fs = archive.openBlank();
		fs.write("file", new byte[archive.getConfig().getPageSize()]);
		byte[] tag = new PageTree(fs.inodeForPath("file")).getPageTag(0);
		
		ByteBuffer buf = ByteBuffer.wrap(archive.getStorage().read(Page.pathForTag(tag)));
		byte[] chunk = new byte[PeerMessage.MESSAGE_SIZE];
		buf.get(chunk);
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_SEND_PAGE);
		
		msg.receivedData((byte) 0, tag);
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(-1).array());
		msg.receivedData(PeerMessage.FLAG_FINAL, chunk);
		conn.handle(msg);
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleSendPageTriggersViolationIfChunkHasExcessiveOffset() throws IOException, ProtocolViolationException {
		blindSwarmCache();
		ZKFS fs = archive.openBlank();
		fs.write("file", new byte[archive.getConfig().getPageSize()]);
		byte[] tag = new PageTree(fs.inodeForPath("file")).getPageTag(0);
		
		ByteBuffer buf = ByteBuffer.wrap(archive.getStorage().read(Page.pathForTag(tag)));
		int numChunks = (int) Math.ceil(((double) buf.limit())/PeerMessage.FILE_CHUNK_SIZE);
		byte[] chunk = new byte[PeerMessage.MESSAGE_SIZE];
		buf.get(chunk);
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_SEND_PAGE);
		
		msg.receivedData((byte) 0, tag);
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(numChunks).array());
		msg.receivedData(PeerMessage.FLAG_FINAL, chunk);
		conn.handle(msg);
	}
	
	@Test
	public void testHandleSendPageCountersExistingPagesWithAnnounce() throws IOException, ProtocolViolationException {
		ZKFS fs = archive.openBlank();
		fs.write("file", new byte[archive.getConfig().getPageSize()]);
		byte[] tag = new PageTree(fs.inodeForPath("file")).getPageTag(0);
		
		ByteBuffer buf = ByteBuffer.wrap(archive.getStorage().read(Page.pathForTag(tag)));
		byte[] chunk = new byte[PeerMessage.MESSAGE_SIZE];
		buf.get(chunk);
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_SEND_PAGE);
		
		msg.receivedData((byte) 0, tag);
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());
		msg.receivedData(PeerMessage.FLAG_FINAL, chunk);
		conn.handle(msg);
		
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_TAGS);
		assertReceivedPayload(ByteBuffer.allocate(8).putLong(Util.shortTag(tag)).array());
		assertFinished();
	}
	
	@Test
	public void testHandleSendPageWorksForSeedPeers() throws IOException, ProtocolViolationException {
		blindPeer();
		testHandleSendPageAddsChunksToChunkAccumulator();
	}
	
	@Test
	public void testHandleSetPausedSetsPausedToFalseIfPausedByteIsZero() throws ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_SET_PAUSED);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[] { 0x01 });
		conn.setLocalPaused(false);
		conn.handle(msg);
		assertTrue(conn.isPaused());
		
		msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_SET_PAUSED, 1);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[] { 0x00 });
		
		conn.handle(msg);
		assertFalse(conn.isPaused());
	}

	@Test
	public void testHandleSetPausedSetsPausedToTrueIfPausedByteIsOne() throws ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_SET_PAUSED);
		conn.setLocalPaused(false);
		assertFalse(conn.isPaused());
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[] { 0x01 });
		conn.handle(msg);
		assertTrue(conn.isPaused());
	}
	
	@Test
	public void testHandleSetPausedTriggersViolationIfPausedByteNotZeroOrOne() throws ProtocolViolationException {
		for(int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++) {
			DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_SET_PAUSED, i);
			if(i == 0 || i == 1) continue;
			msg.receivedData(PeerMessage.FLAG_FINAL, new byte[] { (byte) i });
			try {
				conn.handle(msg);
				fail();
			} catch(ProtocolViolationException exc) {}
		}
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleSetPausedTriggersViolationIfMultipleBytesAreSent() throws ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_SET_PAUSED);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[] { 0x00, 0x00 });
		conn.handle(msg);
	}
	
	@Test
	public void testHandleSetPausedWorksForSeedPeers() throws ProtocolViolationException {
		blindPeer();
		conn.setLocalPaused(false);
		testHandleSetPausedSetsPausedToFalseIfPausedByteIsZero();
		testHandleSetPausedSetsPausedToTrueIfPausedByteIsOne();
	}
	
	@Test
	public void testIsPausableReturnsFalseIfCommandIsCmdAccessProof() {
		assertFalse(conn.isPausable(PeerConnection.CMD_ACCESS_PROOF));
	}

	@Test
	public void testIsPausableReturnsFalseIfCommandIsCmdAnnouncePeers() {
		assertFalse(conn.isPausable(PeerConnection.CMD_ANNOUNCE_PEERS));
	}
	
	@Test
	public void testIsPausableReturnsFalseIfCommandIsCmdAnnounceSelfAd() {
		assertFalse(conn.isPausable(PeerConnection.CMD_ANNOUNCE_SELF_AD));
	}
	
	@Test
	public void testIsPausableReturnsFalseIfCommandIsCmdAnnounceTags() {
		assertFalse(conn.isPausable(PeerConnection.CMD_ANNOUNCE_TAGS));
	}

	@Test
	public void testIsPausableReturnsFalseIfCommandIsCmdAnnounceTips() {
		assertFalse(conn.isPausable(PeerConnection.CMD_ANNOUNCE_TIPS));
	}
	
	@Test
	public void testIsPausableReturnsFalseIfCommandIsCmdRequestAll() {
		assertFalse(conn.isPausable(PeerConnection.CMD_REQUEST_ALL));
	}
	
	@Test
	public void testIsPausableReturnsFalseIfCommandIsCmdRequestInodes() {
		assertFalse(conn.isPausable(PeerConnection.CMD_REQUEST_INODES));
	}
	
	@Test
	public void testIsPausableReturnsFalseIfCommandIsCmdRequestRevisionContents() {
		assertFalse(conn.isPausable(PeerConnection.CMD_REQUEST_REVISION_CONTENTS));
	}
	
	@Test
	public void testIsPausableReturnsFalseIfCommandIsCmdRequestPageTags() {
		assertFalse(conn.isPausable(PeerConnection.CMD_REQUEST_PAGE_TAGS));
	}
	
	@Test
	public void testIsPausableReturnsTrueIfCommandIsCmdSendPage() {
		assertTrue(conn.isPausable(PeerConnection.CMD_SEND_PAGE));
	}
	
	@Test
	public void testIsPausableReturnsFalseIfCommandIsSetPaused() {
		assertFalse(conn.isPausable(PeerConnection.CMD_SET_PAUSED));
	}
	
	@Test
	public void testWantsFileReturnsTrueIfRemotePeerHasNotAnnouncedTag() {
		assertTrue(conn.wantsFile(crypto.rng(crypto.hashLength())));
	}
	
	@Test
	public void testWantsFileReturnsFalseIfRemotePeerHasNotAnnouncedTag() throws ProtocolViolationException {
		byte[] hash = crypto.rng(crypto.hashLength());
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_TAGS);
		msg.receivedData(PeerMessage.FLAG_FINAL, hash); // extra bytes are harmless
		conn.handle(msg);
		assertFalse(conn.wantsFile(hash));
	}
	
	@Test
	public void testHasFileReturnsFalseIfRemotePeerHasNotAnnouncedTag() {
		byte[] hash = crypto.rng(crypto.hashLength());
		assertFalse(conn.hasFile(Util.shortTag(hash)));
	}
	
	@Test
	public void testHasFileReturnsTrueIfRemotePeerHasAnnouncedTag() throws ProtocolViolationException {
		byte[] hash = crypto.rng(crypto.hashLength());
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_TAGS);
		msg.receivedData(PeerMessage.FLAG_FINAL, hash); // extra bytes are harmless
		conn.handle(msg);
		assertTrue(conn.hasFile(Util.shortTag(hash)));
	}
	
	@Test
	public void testBlacklistAddsSocketAddressToBlacklist() {
		assertFalse(master.getBlacklist().contains(socket.getAddress()));
		conn.blacklist();
		assertTrue(master.getBlacklist().contains(socket.getAddress()));
	}
	
	@Test
	public void testCloseTerminatesSocket() {
		assertFalse(socket.isClosed());
		conn.close();
		assertTrue(socket.isClosed());
	}
	
	@Test
	public void testCloseRemovesConnectionFromPeerSwarm() {
		assertNull(swarm.closedConnection);
		conn.close();
		assertEquals(swarm.closedConnection, conn);
	}
	
	@Test
	public void testWaitForUnpauseDoesNotBlockIfNotPaused() throws SocketClosedException {
		conn.setLocalPaused(false);
		conn.waitForUnpause();
	}
	
	@Test
	public void testWaitForUnpauseBlocksUntilRemoteUnpaused() throws ProtocolViolationException {
		class Holder { boolean waited; }
		Holder holder = new Holder();
		
		Thread thread = new Thread(()->{
			try {
				conn.waitForUnpause();
				holder.waited = true;
			} catch (SocketClosedException e) {
				e.printStackTrace();
			}
		});
		
		conn.setLocalPaused(false);
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming(PeerConnection.CMD_SET_PAUSED);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[] {0x01});
		conn.handle(msg);
		
		thread.start();
		try { Thread.sleep(10); } catch (InterruptedException e) {}
		assertFalse(holder.waited);
		
		msg = new DummyPeerMessageIncoming(PeerConnection.CMD_SET_PAUSED);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[] {0x00});
		conn.handle(msg);
		
		assertTrue(Util.waitUntil(100, ()->holder.waited));
	}
	
	@Test
	public void testWaitForUnpauseBlocksUntilLocalUnpaused() {
		class Holder { boolean waited; }
		Holder holder = new Holder();
		
		Thread thread = new Thread(()->{
			try {
				conn.waitForUnpause();
				holder.waited = true;
			} catch (SocketClosedException e) {
				e.printStackTrace();
			}
		});
		
		conn.setLocalPaused(true);
		thread.start();
		try { Thread.sleep(10); } catch (InterruptedException e) {}
		assertFalse(holder.waited);
		conn.setLocalPaused(false);
		assertTrue(Util.waitUntil(100, ()->holder.waited));
	}
	
	@Test
	public void testPageQueueThreadSendsIfUnpaused() throws ProtocolViolationException, IOException {
		ZKFS fs = archive.openBlank();
		fs.write("file", new byte[archive.getConfig().getPageSize()]);
		fs.commit();
		
		int pagesExpected = archive.getStorage().opendir("/").listRecursive(Directory.LIST_OPT_OMIT_DIRECTORIES).length;
		
		conn.setLocalPaused(false);
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming(PeerConnection.CMD_REQUEST_ALL);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		HashSet<Long> pagesSeen = new HashSet<Long>();
		int pagesReceived = 0;
		int pageSize = archive.getConfig().getSerializedPageSize();
		int numChunks = (int) Math.ceil(((double) pageSize)/PeerMessage.FILE_CHUNK_SIZE);
		int finalChunkSize = pageSize % PeerMessage.FILE_CHUNK_SIZE;
		
		while(pagesReceived < pagesExpected) {
			byte[] pageData = new byte[pageSize];
			DummyPeerMessageOutgoing out = socket.popMessage();
			byte[] tag = new byte[crypto.hashLength()];
			IOUtils.read(out.txPayload, tag);
			
			assertFalse(pagesSeen.contains(Util.shortTag(tag)));
			pagesSeen.add(Util.shortTag(tag));
			
			while(out.txPayload.available() >= 0) {
				byte[] indexRaw = new byte[4];
				int r = IOUtils.read(out.txPayload, indexRaw);
				if(r == 0) {
					try { Thread.sleep(1); } catch(InterruptedException exc) {}
					continue;
				}
				
				int index = ByteBuffer.wrap(indexRaw).getInt();
				int length = (index == numChunks - 1) ? finalChunkSize : PeerMessage.FILE_CHUNK_SIZE;
				int offset = index * PeerMessage.FILE_CHUNK_SIZE;
				
				int readLen = IOUtils.read(out.txPayload, pageData, offset, length);
				assertEquals(readLen, length);
			}
			
			byte[] expectedPageData = archive.getStorage().read(Page.pathForTag(tag));
			assertTrue(Arrays.equals(expectedPageData, pageData));
			pagesReceived++;
		}
	}
	
	@Test
	public void testPageQueueThreadDoesNotSendIfLocalPaused() throws IOException, ProtocolViolationException {
		ZKFS fs = archive.openBlank();
		fs.write("file", new byte[archive.getConfig().getPageSize()]);
		fs.commit();
		
		conn.setLocalPaused(true);
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming(PeerConnection.CMD_REQUEST_ALL);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		assertNoMessage();
	}
	
	@Test
	public void testPageQueueThreadDoesNotSendIfRemotePaused() throws IOException, ProtocolViolationException {
		ZKFS fs = archive.openBlank();
		fs.write("file", new byte[archive.getConfig().getPageSize()]);
		fs.commit();
		
		conn.setLocalPaused(false);
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming(PeerConnection.CMD_SET_PAUSED);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[] { 0x01 });
		conn.handle(msg);
		
		msg = new DummyPeerMessageIncoming(PeerConnection.CMD_REQUEST_ALL);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		assertNoMessage();
	}
	
	@Test
	public void testPageQueueThreadSkipsTagsThatHaveBeenAnnounced() throws IOException, ProtocolViolationException {
		ZKFS fs = archive.openBlank();
		fs.write("file", new byte[archive.getConfig().getPageSize()]);
		fs.commit();
		
		conn.setLocalPaused(false);
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming(PeerConnection.CMD_ANNOUNCE_TAGS);
		msg.receivedData(PeerMessage.FLAG_FINAL, archive.getConfig().tag()); // extra tag bytes are harmless here
		conn.handle(msg);
		
		msg = new DummyPeerMessageIncoming(PeerConnection.CMD_REQUEST_ALL);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		while(Util.waitUntil(100, ()->!socket.messages.isEmpty())) {
			DummyPeerMessageOutgoing out = socket.popMessage();
			byte[] tag = new byte[crypto.hashLength()];
			IOUtils.read(out.txPayload, tag);
			assertFalse(Arrays.equals(archive.getConfig().tag(), tag));
			while(out.txPayload.available() >= 0) {
				byte[] skipBuf = new byte[4 + PeerMessage.FILE_CHUNK_SIZE];
				if(IOUtils.read(out.txPayload, skipBuf) == 0) {
					try { Thread.sleep(1); } catch(InterruptedException exc) {}
				}
			}
		}
	}
	
	@Test
	public void testSendsTipsAtInitialization() throws IOException {
		socket.close();
		socket = new DummySocket(swarm);
		PeerConnection conn2 = new PeerConnection(socket);

		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_TIPS);
		conn2.close();
	}

	@Test
	public void testSendsTagsAtInitialization() throws IOException {
		socket.close();
		socket = new DummySocket(swarm);
		PeerConnection conn2 = new PeerConnection(socket);

		socket.messages.pollFirst();
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_TAGS);
		
		conn2.close();
	}
}
