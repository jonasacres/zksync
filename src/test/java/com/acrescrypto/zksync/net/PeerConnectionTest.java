package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.SocketClosedException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ObfuscatedRefTag;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.PageMerkle;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.PageQueue.EverythingQueueItem;
import com.acrescrypto.zksync.net.PageQueue.PageQueueItem;
import com.acrescrypto.zksync.net.PageQueue.QueueItem;
import com.acrescrypto.zksync.net.PageQueue.RefTagContentsQueueItem;
import com.acrescrypto.zksync.net.PageQueue.RevisionQueueItem;
import com.acrescrypto.zksync.net.PeerConnection.PeerCapabilityException;
import com.acrescrypto.zksync.net.PeerMessageOutgoing.MessageSegment;
import com.acrescrypto.zksync.utility.Util;

public class PeerConnectionTest {
	class DummySwarm extends PeerSwarm {
		LinkedList<PeerAdvertisement> receivedAds = new LinkedList<PeerAdvertisement>();
		PeerConnection closedConnection;
		
		public DummySwarm(ZKArchiveConfig config) throws IOException {
			super(config);
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
	
	class DummyPageQueue extends PageQueue {
		public DummyPageQueue(ZKArchive archive) {
			super(archive);
		}
	}
	
	class DummySocket extends PeerSocket {
		int peerType = PeerConnection.PEER_TYPE_FULL;
		MessageSegment segment;
		boolean closed;
		
		public DummySocket(DummySwarm swarm) { this.swarm = swarm; }
		@Override public PeerAdvertisement getAd() { return null; }
		@Override public void write(byte[] data, int offset, int length) {}
		@Override public int read(byte[] data, int offset, int length) { return 0; }
		@Override public boolean isLocalRoleClient() { return false; }
		@Override public void close() { closed = true; }
		@Override public boolean isClosed() { return closed; }
		@Override public void handshake() {}
		@Override public int getPeerType() { return peerType; }
		@Override public byte[] getSharedSecret() { return null; }
		@Override public void dataReady(MessageSegment msg) { segment = msg; }
		@Override public String getAddress() { return "127.0.0.1"; }
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
	
	void assertReceivedCmd(int cmd) {
		assertTrue(Util.waitUntil(100, ()->socket.segment != null));
		socket.segment.content.position(8);
		assertEquals(cmd, socket.segment.content.get());
		socket.segment.content.position(PeerMessage.HEADER_LENGTH);
	}
	
	void assertReceivedPayload(byte[] payload) {
		assertTrue(Util.waitUntil(100, ()->socket.segment != null));
		socket.segment.content.position(PeerMessage.HEADER_LENGTH);
		byte[] withoutHeader = new byte[socket.segment.content.limit() - PeerMessage.HEADER_LENGTH];
		socket.segment.content.get(withoutHeader);
		assertTrue(Arrays.equals(payload, withoutHeader));
	}
	
	void assertReceivedBytes(byte[] expectedNextBytes) {
		byte[] nextBytes = new byte[expectedNextBytes.length];
		socket.segment.content.get(nextBytes);
		assertTrue(Arrays.equals(expectedNextBytes, nextBytes));
	}
	
	void assertReceivedAd(PeerAdvertisement ad) {
		assertTrue(swarm.receivedAds.contains(ad));
	}

	void assertUnreceivedAd(PeerAdvertisement ad) {
		assertFalse(swarm.receivedAds.contains(ad));
	}

	void assertFinished() {
		assertFalse(socket.segment.content.hasRemaining());
	}
	
	void assertNoMessage() {
		assertFalse(Util.waitUntil(100, ()->socket.segment != null));
	}
	
	void assertNoQueuedItemLike(QueueItemTest test) {
		Iterator<QueueItem> itr = conn.queue.itemsByPriority.iterator();
		boolean found = false;
		while(!found && itr.hasNext()) {
			found = test.test(itr.next());
		}
		
		assertFalse(found);
	}
	
	void assertQueuedItemLike(QueueItemTest test) {
		Iterator<QueueItem> itr = conn.queue.itemsByPriority.iterator();
		boolean found = false;
		while(!found && itr.hasNext()) {
			found = test.test(itr.next());
		}
		
		assertTrue(found);
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
		conn = new PeerConnection(socket, PeerConnection.PEER_TYPE_FULL);
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
	}

	@Test
	public void testConstructWithAdConnectsToAdvertisement() throws IOException, UnsupportedProtocolException, ProtocolViolationException, BlacklistedException, UnconnectableAdvertisementException {
		DummyTCPPeerSocketListener listener = new DummyTCPPeerSocketListener(master, 0);
		assertTrue(Util.waitUntil(100, ()->listener.listenSocket != null));
		listener.advertise(swarm);
		assertNull(listener.connectedPeer);
		PeerConnection conn = new PeerConnection(swarm, listener.listenerForSwarm(swarm).localAd());
		assertTrue(Util.waitUntil(100, ()->listener.connectedPeer != null));
		
		assertNotNull(listener.connectedPeer);
		assertNotNull(conn.queue);
		assertEquals(PeerConnection.PEER_TYPE_FULL, conn.peerType);
	}
	
	@Test
	public void testConstructWithAdConnectsToAdvertisementSetsPeerTypeBlindIfAdvertiserIsBlind() throws IOException, UnsupportedProtocolException, ProtocolViolationException, BlacklistedException, UnconnectableAdvertisementException {
		ArchiveAccessor roAccessor = archive.getConfig().getAccessor().makeSeedOnly();
		ZKArchiveConfig roConfig = new ZKArchiveConfig(roAccessor, archive.getConfig().getArchiveId());
		DummySwarm roSwarm = new DummySwarm(roConfig);

		DummyTCPPeerSocketListener listener = new DummyTCPPeerSocketListener(master, 0);
		assertTrue(Util.waitUntil(100, ()->listener.listenSocket != null));
		listener.advertise(roSwarm);
		assertNull(listener.connectedPeer);
		PeerConnection conn = new PeerConnection(swarm, listener.listenerForSwarm(roSwarm).localAd());
		assertTrue(Util.waitUntil(100, ()->listener.connectedPeer != null));
		
		assertNotNull(listener.connectedPeer);
		assertNotNull(conn.queue);
		assertEquals(PeerConnection.PEER_TYPE_BLIND, conn.peerType);
	}
	
	@Test
	public void testConstructWithAdConnectsToAdvertisementSetsPeerTypeBlindIfLocalIsBlind() throws IOException, UnsupportedProtocolException, ProtocolViolationException, BlacklistedException, UnconnectableAdvertisementException {
		ArchiveAccessor roAccessor = archive.getConfig().getAccessor().makeSeedOnly();
		ZKArchiveConfig roConfig = new ZKArchiveConfig(roAccessor, archive.getConfig().getArchiveId());
		DummySwarm roSwarm = new DummySwarm(roConfig);

		DummyTCPPeerSocketListener listener = new DummyTCPPeerSocketListener(master, 0);
		assertTrue(Util.waitUntil(100, ()->listener.listenSocket != null));
		listener.advertise(swarm);
		assertNull(listener.connectedPeer);
		PeerConnection conn = new PeerConnection(roSwarm, listener.listenerForSwarm(swarm).localAd());
		assertTrue(Util.waitUntil(100, ()->listener.connectedPeer != null));
		
		assertNotNull(listener.connectedPeer);
		assertNotNull(conn.queue);
		assertEquals(PeerConnection.PEER_TYPE_BLIND, conn.peerType);
	}
	
	@Test
	public void testConstructWithSocketSetsPeerType() {
		PeerConnection blind = new PeerConnection(socket, PeerConnection.PEER_TYPE_BLIND);
		PeerConnection full = new PeerConnection(socket, PeerConnection.PEER_TYPE_FULL);
		
		assertEquals(PeerConnection.PEER_TYPE_BLIND, blind.peerType);
		assertEquals(PeerConnection.PEER_TYPE_FULL, full.peerType);
		
		assertEquals(socket, blind.getSocket());
	}
	
	@Test
	public void testGetSocketReturnsSocket() {
		assertEquals(socket, conn.getSocket());
	}
	
	@Test
	public void testAnnounceTag() {
		conn.announceTag(1234);
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_TAGS);
		assertReceivedPayload(ByteBuffer.allocate(8).putLong(1234).array());
		assertFinished();
	}
	
	@Test
	public void testAnnounceTags() {
		int numTags = 16;
		LinkedList<RefTag> tags = new LinkedList<RefTag>();
		ByteBuffer payload = ByteBuffer.allocate(numTags * 8);
		
		for(int i = 0; i < numTags; i++) {
			RefTag junkTag = new RefTag(archive, crypto.rng(crypto.hashLength()), 1, 1);
			tags.add(junkTag);
			payload.put(junkTag.getShortHashBytes());
		}
		
		assertFalse(payload.hasRemaining());
		conn.announceTags(tags);
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_TAGS);
		assertReceivedPayload(payload.array());
		assertFinished();
	}
	
	@Test
	public void testAnnounceSelf() throws UnconnectableAdvertisementException {
		PeerAdvertisement ad = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "localhost", 1234);
		conn.announceSelf(ad);
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_SELF_AD);
		assertReceivedBytes(ByteBuffer.allocate(2).putShort((short) ad.serialize().length).array());
		assertReceivedBytes(ad.serialize());
		assertFinished();
	}
	
	@Test
	public void testAnnouncePeer() throws UnconnectableAdvertisementException {
		PeerAdvertisement ad = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "localhost", 1234);
		conn.announcePeer(ad);
		assertReceivedCmd(PeerConnection.CMD_ANNOUNCE_PEERS);
		assertReceivedBytes(ByteBuffer.allocate(2).putShort((short) ad.serialize().length).array());
		assertReceivedBytes(ad.serialize());
		assertFinished();
	}
	
	@Test
	public void testAnnouncePeers() throws UnconnectableAdvertisementException {
		int numPeers = 16;
		LinkedList<PeerAdvertisement> ads = new LinkedList<PeerAdvertisement>();
		
		for(int i = 0; i < numPeers; i++) {
			PeerAdvertisement ad = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "10.0.0."+i, 1000+i);
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
		for(RefTag tag : archive.getRevisionTree().plainBranchTips()) {
			assertReceivedBytes(tag.obfuscate().serialize());
		}
		assertFinished();
	}
	
	@Test
	public void testRequestAll() {
		conn.requestAll();
		assertReceivedCmd(PeerConnection.CMD_REQUEST_ALL);
		assertFinished();
	}
	
	@Test
	public void testRequestPageTag() {
		conn.requestPageTag(1234);
		assertReceivedCmd(PeerConnection.CMD_REQUEST_PAGE_TAGS);
		assertReceivedBytes(ByteBuffer.allocate(8).putLong(1234).array());
		assertFinished();
	}
	
	@Test
	public void testRequestPageTags() {
		byte[][] pageTags = new byte[16][];
		for(int i = 0; i < pageTags.length; i++) {
			// page tags of various lengths; prove that we only use the first 8 bytes (short tag)
			pageTags[i] = crypto.rng(RefTag.REFTAG_SHORT_SIZE + (int) (Math.random()*16.0));
		}
		
		conn.requestPageTags(pageTags);
		assertReceivedCmd(PeerConnection.CMD_REQUEST_PAGE_TAGS);
		for(byte[] tag : pageTags) {
			byte[] shortTag = new byte[RefTag.REFTAG_SHORT_SIZE];
			System.arraycopy(tag, 0, shortTag, 0, shortTag.length);
			assertReceivedBytes(shortTag);
		}
		assertFinished();
	}
	
	@Test
	public void testRequestRefTags() throws PeerCapabilityException {
		RefTag[] tags = new RefTag[16];
		
		for(int i = 0; i < tags.length; i++) {
			tags[i] = new RefTag(archive, crypto.rng(crypto.hashLength()), 1, 1);
		}
		
		conn.requestRefTags(tags);
		assertReceivedCmd(PeerConnection.CMD_REQUEST_REF_TAGS);
		for(RefTag tag : tags) assertReceivedBytes(tag.getBytes());
		assertFinished();
	}
	
	@Test
	public void testRequestRefTagsThrowsExceptionIfNotFullPeer() throws PeerCapabilityException {
		conn = new PeerConnection(socket, PeerConnection.PEER_TYPE_BLIND);
		try {
			conn.requestRefTags(new RefTag[] { new RefTag(archive, crypto.rng(crypto.hashLength()), 1, 1) } );
			fail();
		} catch(PeerCapabilityException exc) {
			assertNoMessage();
		}
	}
	
	@Test
	public void testRequestRevisionContents() throws PeerCapabilityException {
		RefTag[] tags = new RefTag[16];
		
		for(int i = 0; i < tags.length; i++) {
			tags[i] = new RefTag(archive, crypto.rng(crypto.hashLength()), 1, 1);
		}
		
		conn.requestRevisionContents(tags);
		assertReceivedCmd(PeerConnection.CMD_REQUEST_REVISION_CONTENTS);
		for(RefTag tag : tags) assertReceivedBytes(tag.getBytes());
		assertFinished();
	}
	
	@Test
	public void testRequestRevisionContentsThrowsExceptionIfNotFullPeer() {
		conn = new PeerConnection(socket, PeerConnection.PEER_TYPE_BLIND);
		try {
			conn.requestRevisionContents(new RefTag[] { new RefTag(archive, crypto.rng(crypto.hashLength()), 1, 1) } );
			fail();
		} catch(PeerCapabilityException exc) {
			assertNoMessage();
		}
	}
	
	@Test
	public void testSetPausedEnabled() {
		conn.setPaused(true);
		assertReceivedCmd(PeerConnection.CMD_SET_PAUSED);
		assertReceivedBytes(new byte[] {0x01});
		assertFinished();
	}
	
	@Test
	public void testSetPausedDisabled() {
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
			ads[i] = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "10.0.0."+i, 1000+i);
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
		conn.peerType = PeerConnection.PEER_TYPE_BLIND;
		testHandleAnnouncePeersCallSwarmAddPeerAdvertisement();
	}
	
	@Test
	public void testHandleAnnouncePeersToleratesIndecipherableAds() throws ProtocolViolationException, UnconnectableAdvertisementException {
		PeerAdvertisement[] ads = new PeerAdvertisement[2];
		ads[0] = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "10.0.0.0", 1000);
		ads[1] = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "10.0.0.1", 1001);
		
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
			ads[i] = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "10.0.0."+i, 1000+i);
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
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "10.0.0.1", 1000);
		
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
		conn.peerType = PeerConnection.PEER_TYPE_BLIND;
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
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "10.0.0.1", 1000);
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
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "10.0.0.1", 1000);
		
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
	public void testHandleAnnounceTagsSetsReceivedTags() throws ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_TAGS);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		assertFalse(conn.receivedTags);
		conn.handle(msg);
		assertTrue(conn.receivedTags);
	}
	
	@Test
	public void testHandleAnnounceTagsWorksForSeedOnly() throws ProtocolViolationException, IOException, UnconnectableAdvertisementException {
		conn.peerType = PeerConnection.PEER_TYPE_BLIND;
		testHandleAnnounceTagsUpdatesAnnouncedTagsList();
	}
	
	@Test
	public void testHandleAnnounceTipsAddsBranchTipsToRevisionTree() throws ProtocolViolationException, IOException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_TIPS);
		RefTag[] tags = new RefTag[16];
		ObfuscatedRefTag[] obfTags = new ObfuscatedRefTag[16];

		for(int i = 0; i < tags.length; i++) {
			tags[i] = new RefTag(archive, crypto.rng(crypto.hashLength()), RefTag.REF_TYPE_2INDIRECT, 2);
			obfTags[i] = tags[i].obfuscate();
			msg.receivedData((byte) 0, obfTags[i].serialize());
			assertFalse(archive.getRevisionTree().branchTips().contains(obfTags[i]));
		}
		
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		archive.getRevisionTree().read();
		for(int i = 0; i < tags.length; i++) {
			assertTrue(archive.getRevisionTree().plainBranchTips().contains(tags[i]));
			assertTrue(archive.getRevisionTree().branchTips().contains(obfTags[i]));
		}
	}
	
	@Test
	public void testHandleAnnounceTipsTriggersViolationWhenForgedRefTagSent() throws ProtocolViolationException, IOException {
		RefTag fakeTag = new RefTag(archive, crypto.rng(crypto.hashLength()), RefTag.REF_TYPE_2INDIRECT, 2);
		ObfuscatedRefTag obfTag = fakeTag.obfuscate();
		byte[] raw = obfTag.serialize();
		raw[8] ^= 0x40;
		obfTag = new ObfuscatedRefTag(archive, raw);
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_ANNOUNCE_TIPS);
		msg.receivedData(PeerMessage.FLAG_FINAL, obfTag.serialize());
		assertFalse(archive.getRevisionTree().branchTips().contains(obfTag));
		try {
			conn.handle(msg);
			fail();
		} catch(ProtocolViolationException exc) {
		}
		assertFalse(archive.getRevisionTree().branchTips().contains(obfTag));
	}
	
	@Test
	public void testHandleAnnounceTipsWorksForSeedOnly() throws ProtocolViolationException, IOException, UnconnectableAdvertisementException {
		conn.peerType = PeerConnection.PEER_TYPE_BLIND;
		testHandleAnnounceTipsAddsBranchTipsToRevisionTree();
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
		conn.peerType = PeerConnection.PEER_TYPE_BLIND;
		testHandleRequestAllCausesPageQueueToSendEverything();
	}
	
	@Test
	public void testRequestRefTagsAddsRequestedRefTagsToPageQueue() throws ProtocolViolationException, IOException {
		ZKFS fs = archive.openBlank();
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_REF_TAGS);
		RefTag[] tags = new RefTag[16];

		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());

		for(int i = 0; i < tags.length; i++) {
			fs.write("file"+i, new byte[archive.getConfig().getPageSize()]);
			tags[i] = fs.inodeForPath("file"+i).getRefTag();
			msg.receivedData((byte) 0, tags[i].getBytes());
		}

		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		for(RefTag tag : tags) {
			assertQueuedItemLike((_item) -> {
				if(!(_item instanceof RefTagContentsQueueItem)) return false;
				RefTagContentsQueueItem item = (RefTagContentsQueueItem) _item;
				return tag.equals(item.merkle.getRefTag());
			});
		}
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testRequestRefTagsTriggersViolationIfNotFullPeer() throws ProtocolViolationException, IOException {
		conn.peerType = PeerConnection.PEER_TYPE_BLIND;
		testRequestRefTagsAddsRequestedRefTagsToPageQueue();
	}
	
	@Test
	public void testHandleRequestRefTagsToleratesNonexistentRefTags() throws IOException, ProtocolViolationException {
		ZKFS fs = archive.openBlank();
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_REF_TAGS);
		RefTag[] tags = new RefTag[16];

		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());

		for(int i = 0; i < tags.length/2; i++) {
			fs.write("file"+i, new byte[archive.getConfig().getPageSize()]);
			tags[i] = fs.inodeForPath("file"+i).getRefTag();
			msg.receivedData((byte) 0, tags[i].getBytes());
		}
		
		msg.receivedData((byte) 0, crypto.rng(tags[0].getBytes().length));
		
		for(int i = tags.length/2; i < tags.length; i++) {
			fs.write("file"+i, new byte[archive.getConfig().getPageSize()]);
			tags[i] = fs.inodeForPath("file"+i).getRefTag();
			msg.receivedData((byte) 0, tags[i].getBytes());
		}

		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		for(RefTag tag : tags) {
			assertQueuedItemLike((_item) -> {
				if(!(_item instanceof RefTagContentsQueueItem)) return false;
				RefTagContentsQueueItem item = (RefTagContentsQueueItem) _item;
				return item.merkle != null && tag.equals(item.merkle.getRefTag());
			});
		}
	}
	
	@Test
	public void testHandleRequestRevisionContentsAddsRequestedRevTagToPageQueue() throws IOException, ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_REVISION_CONTENTS);
		RefTag[] tags = new RefTag[16];
		
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());
		for(int i = 0; i < tags.length; i++) {
			ZKFS fs = archive.openBlank();
			fs.write("file"+i, "content".getBytes());
			tags[i] = fs.commit();
			msg.receivedData((byte) 0, tags[i].getBytes());
		}
		
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		for(RefTag tag : tags) {
			assertQueuedItemLike((_item) -> {
				if(!(_item instanceof RevisionQueueItem)) return false;
				RevisionQueueItem item = (RevisionQueueItem) _item;
				return tag.equals(item.revTag);
			});
		}
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleRequestRevisionContentsTriggersViolationIfNotFullPeer() throws IOException, ProtocolViolationException {
		conn.peerType = PeerConnection.PEER_TYPE_BLIND;
		testHandleRequestRevisionContentsAddsRequestedRevTagToPageQueue();
	}
	
	@Test
	public void testHandleRequestRevisionContentsToleratesNonexistentRevTags() throws IOException, ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_REVISION_CONTENTS);
		RefTag[] tags = new RefTag[16];
		
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());
		for(int i = 0; i < tags.length; i++) {
			ZKFS fs = archive.openBlank();
			fs.write("file"+i, "content".getBytes());
			tags[i] = fs.commit();
			msg.receivedData((byte) 0, tags[i].getBytes());
			if(i == tags.length/2) msg.receivedData((byte) 0, crypto.rng(tags[i].getBytes().length));
		}
		
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		conn.handle(msg);
		
		for(RefTag tag : tags) {
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
		PageMerkle merkle = new PageMerkle(fs.inodeForPath("file").getRefTag());
		
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());
		for(int i = 0; i < tags.length; i++) {
			tags[i] = merkle.getPageTag(i);
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
	public void testHandleRequestTagsToleratesNonexistentTags() throws ProtocolViolationException, IOException {
		byte[][] tags = new byte[16][];
		ZKFS fs = archive.openBlank();		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_REQUEST_PAGE_TAGS);
		fs.write("file", new byte[tags.length*archive.getConfig().getPageSize()]);
		fs.commit();
		PageMerkle merkle = new PageMerkle(fs.inodeForPath("file").getRefTag());
		
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0).array());
		for(int i = 0; i < tags.length; i++) {
			tags[i] = merkle.getPageTag(i);
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
		conn.peerType = PeerConnection.PEER_TYPE_BLIND;
		testHandleRequestTagsToleratesNonexistentTags();
	}
	
	@Test
	public void testHandleSendPageAddsChunksToChunkAccumulator() throws IOException, ProtocolViolationException {
		ZKFS fs = archive.openBlank();
		fs.write("file", new byte[archive.getConfig().getPageSize()]);
		byte[] tag = new PageMerkle(fs.inodeForPath("file").getRefTag()).getPageTag(0);
		
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
		assertTrue(accumulator.finished);
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testHandleSendPageTriggersViolationIfChunkHasNegativeOffset() throws IOException, ProtocolViolationException {
		ZKFS fs = archive.openBlank();
		fs.write("file", new byte[archive.getConfig().getPageSize()]);
		byte[] tag = new PageMerkle(fs.inodeForPath("file").getRefTag()).getPageTag(0);
		
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
		ZKFS fs = archive.openBlank();
		fs.write("file", new byte[archive.getConfig().getPageSize()]);
		byte[] tag = new PageMerkle(fs.inodeForPath("file").getRefTag()).getPageTag(0);
		
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
	public void testHandleSendPageWorksForSeedPeers() throws IOException, ProtocolViolationException {
		conn.peerType = PeerConnection.PEER_TYPE_BLIND;
		testHandleSendPageAddsChunksToChunkAccumulator();
	}
	
	@Test
	public void testHandleSetPausedSetsPausedToFalseIfPausedByteIsZero() throws ProtocolViolationException {
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming((byte) PeerConnection.CMD_SET_PAUSED);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[] { 0x01 });
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
		conn.peerType = PeerConnection.PEER_TYPE_BLIND;
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
	public void testIsPausableReturnsFalseIfCommandIsCmdRequestRefTags() {
		assertFalse(conn.isPausable(PeerConnection.CMD_REQUEST_REF_TAGS));
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
		conn.waitForUnpause();
	}
	
	@Test
	public void testWaitForUnpauseBlocksUntilUnpaused() throws ProtocolViolationException {
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
		
		DummyPeerMessageIncoming msg = new DummyPeerMessageIncoming(PeerConnection.CMD_SET_PAUSED);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[] {0x01});
		conn.handle(msg);
		
		thread.start();
		try { Thread.sleep(10); } catch (InterruptedException e) {}
		
		msg = new DummyPeerMessageIncoming(PeerConnection.CMD_SET_PAUSED);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[] {0x00});
		conn.handle(msg);
		
		assertTrue(Util.waitUntil(100, ()->holder.waited));
	}
}
