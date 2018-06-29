package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.exceptions.ClosedException;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.Util;

import ch.qos.logback.classic.Level;

public class PeerSwarmTest {
	class DummyAdvertisement extends PeerAdvertisement {
		String address;
		boolean blacklisted, explode;
		byte type = -1;
		public DummyAdvertisement() { address = "localhost"; }
		public DummyAdvertisement(String address) {
			this.address = address;
			this.type = TYPE_TCP_PEER;
		}
		@Override public void blacklist(Blacklist blacklist) throws IOException {}
		@Override public boolean isBlacklisted(Blacklist blacklist) throws IOException { return blacklisted; }
		@Override public byte[] serialize() { return new byte[0]; }
		@Override public boolean matchesAddress(String address) { return this.address.equals(address); }
		@Override public byte getType() { return type; }
		@Override public boolean isReachable() { return true; }
		@Override public DummyConnection connect(PeerSwarm swarm) throws IOException  {
			if(explode) throw new RuntimeException("kerblooie");
			return new DummyConnection(new DummySocket(address, swarm));
		}
		@Override public int hashCode() { return address.hashCode(); }
		@Override public boolean equals(Object other) {
			if(!(other instanceof DummyAdvertisement)) return false;
			return address.equals(((DummyAdvertisement) other).address);
		}
	}
	
	class ExplodingDummyAdvertisement extends DummyAdvertisement { // could replace with DummyAdvertisement.explode
		public ExplodingDummyAdvertisement() { super("kaboom"); }
		@Override public DummyConnection connect(PeerSwarm swarm) {
			exploded = true;
			throw new RuntimeException();
		}
	}
	
	class DummySocket extends PeerSocket {
		protected String address = "dummy";
		protected DummyAdvertisement ad;
		public DummySocket(String address, PeerSwarm swarm) {
			this.address = address;
			this.swarm = swarm;
			synchronized(connectedAddresses) { connectedAddresses.add(address); }
		}
		
		@Override public DummyAdvertisement getAd() { return ad; }
		@Override public void write(byte[] data, int offset, int length) {}
		@Override public int read(byte[] data, int offset, int length) { return 0; }
		@Override public boolean isLocalRoleClient() { return false; }
		@Override public void _close() {}
		@Override public boolean isClosed() { return false; }
		@Override public byte[] getSharedSecret() { return null; }
		@Override public String getAddress() { return address; }
		@Override public void handshake(PeerConnection conn) {}
		@Override public int getPeerType() { return -1; }
	}
	
	class DummyConnection extends PeerConnection {
		DummySocket socket;
		PeerAdvertisement seenAd;
		int requestedPriority;
		boolean requestedAll, requestedAllCancel, requestedPause, requestedPauseValue;
		long requestedTag, requestedInodeId;
		RefTag requestedRefTag, requestedRevTag;
		
		public DummyConnection(DummySocket socket) throws IOException {
			super(socket);
			this.socket = socket;
		}
		
		@Override public DummySocket getSocket() { return socket; }
		@Override public void close() {
			this.closed = true;
			try { socket.close(); } catch(IOException exc) {}
			queue.close();
		}
		
		@Override protected void pageQueueThread() {}
		
		@Override public void announceSelf(PeerAdvertisement ad) { this.seenAd = ad; }
		@Override public void requestPageTag(int priority, long tag) {
			requestedPriority = priority;
			this.requestedTag = tag;
		}
		@Override public void requestPageTags(int priority, Collection<Long> tags) {
			requestedPriority = priority;
			for(Long tag : tags) {
				this.requestedTag = tag;
				break;
			}
		}
		@Override public void requestInodes(int priority, RefTag revTag, Collection<Long> inodeIds) {
			requestedPriority = priority;
			this.requestedRevTag = revTag;
			for(Long inodeId : inodeIds) {
				this.requestedInodeId = inodeId;
				break;
			}
		}
		@Override public void requestRevisionContents(int priority, Collection<RefTag> tips) {
			requestedPriority = priority;
			for(RefTag tip : tips) {
				this.requestedRevTag = tip;
				break;
			}
		}
		
		@Override public void setPaused(boolean paused) {
			requestedPause = true;
			requestedPauseValue = paused;
		}
		
		@Override public void requestAll() { this.requestedAll = true; }
		@Override public void requestAllCancel() { this.requestedAllCancel = true; }
	}
	
	static ZKMaster master;
	ZKArchive archive;
	byte[] pageTag;
	
	PeerSwarm swarm;
	DummyConnection connection;
	ArrayList<String> connectedAddresses = new ArrayList<String>();
	boolean exploded;
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		master = ZKMaster.openBlankTestVolume();
	}
	
	@Before
	public void before() throws IOException {
		connectedAddresses.clear();
		
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		
		ZKFS fs = archive.openBlank();
		fs.write("file", new byte[archive.getConfig().getPageSize()]);
		PageTree tree = new PageTree(fs.inodeForPath("file"));
		pageTag = tree.getPageTag(0);

		swarm = archive.getConfig().getSwarm();
		exploded = false;
		connection = new DummyConnection(new DummySocket("127.0.0.1", swarm));
		connection.socket.ad = new DummyAdvertisement();
	}
	
	@After
	public void after() {
		connection.close();
		archive.close();
		try { Thread.sleep(1); } catch(InterruptedException exc) {} // give exploding ads a chance to percolate through before turning logging back on
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(PeerSwarm.class)).setLevel(Level.WARN);
		Util.setCurrentTimeNanos(-1);
	}
	
	@AfterClass
	public static void afterAll() {
		master.close();
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testBlank() {}
	
	@Test
	public void testOpenedConnectionAddsOpenConnection() {
		swarm.openedConnection(connection);
		assertTrue(swarm.connections.contains(connection));
	}
	
	@Test
	public void testOpenedConnectionDoesntCloseConnectionIfSwarmIsNotClosed() {
		swarm.openedConnection(connection);
		assertFalse(connection.closed);
	}
	
	@Test
	public void testOpenedConnectionClosesConnectionIfSwarmIsClosed() {
		assertFalse(connection.closed);
		swarm.close();
		swarm.openedConnection(connection);
		assertTrue(connection.closed);
	}
	
	@Test
	public void testOpenedConnectionToleratesSocketsWithNoAdvertisement() {
		connection.socket.ad = null;
		swarm.openedConnection(connection);
	}
	
	@Test
	public void testOpenedConnectionAddsSocketAdvertisementToListOfConnectedAds() {
		assertFalse(swarm.connectedAds.contains(connection.socket.ad));
		swarm.openedConnection(connection);
		assertTrue(swarm.connectedAds.contains(connection.socket.ad));
	}
	
	@Test
	public void testOpenedConnectionAddsSocketAdvertisementToListOfKnownAds() {
		assertFalse(swarm.knownAds.contains(connection.socket.ad));
		swarm.openedConnection(connection);
		assertTrue(swarm.knownAds.contains(connection.socket.ad));
	}
	
	@Test
	public void testAddPeerMarksPeerAdAsKnown() {
		connection.socket.ad.type = PeerAdvertisement.TYPE_TCP_PEER;
		assertFalse(swarm.knownAds.contains(connection.socket.ad));
		swarm.addPeerAdvertisement(connection.socket.ad);
		assertTrue(swarm.knownAds.contains(connection.socket.ad));
	}
	
	@Test
	public void testAddPeerIgnoresPeerAdIfNotSupported() {
		assertFalse(PeerSocket.adSupported(connection.socket.ad));
		assertFalse(swarm.knownAds.contains(connection.socket.ad));
		swarm.addPeerAdvertisement(connection.socket.ad);
		assertFalse(swarm.knownAds.contains(connection.socket.ad));
	}
	
	@Test
	public void testAddPeerIgnoresPeerAdIfCapacityReached() {
		for(int i = 0; i < swarm.maxPeerListSize; i++) {
			DummyAdvertisement ad = new DummyAdvertisement();
			ad.type = PeerAdvertisement.TYPE_TCP_PEER;
			ad.address = "10.0.1." + i;
			swarm.addPeerAdvertisement(ad);
			assertTrue(swarm.knownAds.contains(ad));
		}

		connection.socket.ad.type = PeerAdvertisement.TYPE_TCP_PEER;
		swarm.addPeerAdvertisement(connection.socket.ad);
		assertFalse(swarm.knownAds.contains(connection.socket.ad));
	}
	
	@Test
	public void testDisconnectAddressClosesAllConnectionsFromAddress() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("192.168.0.1", swarm));
			swarm.openedConnection(conns[i]);
			assertFalse(conns[i].closed);
		}
		
		swarm.disconnectAddress("192.168.0.1", 0);
		for(DummyConnection conn : conns) {
			assertTrue(conn.closed);
		}
	}
	
	@Test
	public void testDisconnectAddressDoesntCloseOtherAddresses() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket(i % 2 == 0 ? "192.168.0.1" : "10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			assertFalse(conns[i].closed);
		}
		
		swarm.disconnectAddress("192.168.0.1", 0);
		for(DummyConnection conn : conns) {
			assertTrue(conn.closed == conn.getSocket().getAddress().equals("192.168.0.1"));
		}
	}
	
	@Test
	public void testDisconnectAddressToleratesNonconnectedAddress() {
		swarm.disconnectAddress("192.168.0.1", 0);
	}
	
	@Test
	public void testCloseDisconnectsAllPeerConnections() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			assertFalse(conns[i].closed);
		}
		
		swarm.close();
		for(DummyConnection conn : conns) {
			assertTrue(conn.closed);
		}
	}
	
	@Test
	public void testAdvertiseSelfRelaysAdToAllConnections() throws IOException {
		DummyAdvertisement ad = new DummyAdvertisement();
		DummyConnection[] conns = new DummyConnection[16];
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			assertNull(conns[i].seenAd);
		}
		
		swarm.advertiseSelf(ad);
		for(DummyConnection conn : conns) {
			assertTrue(conn.seenAd == ad);
		}
	}
	
	@Test
	public void testAutomaticallyConnectsToAdvertisedPeers() throws InterruptedException {
		DummyAdvertisement ad = new DummyAdvertisement("some-ad");
		assertFalse(connectedAddresses.contains(ad.address));
		swarm.addPeerAdvertisement(ad);
		long breakTime = System.currentTimeMillis() + 200;
		while(!connectedAddresses.contains(ad.address) && System.currentTimeMillis() < breakTime) {
			Thread.sleep(1);
		}
		assertTrue(connectedAddresses.contains(ad.address));
	}
	
	@Test
	public void testEmbaroesUnconnectablePeers() throws InterruptedException {
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(PeerSwarm.class)).setLevel(Level.OFF);
		DummyAdvertisement ad = new DummyAdvertisement("some-ad");
		Util.setCurrentTimeNanos(0);
		ad.explode = true;
		assertFalse(connectedAddresses.contains(ad.address));
		swarm.addPeerAdvertisement(ad);
		assertFalse(Util.waitUntil(200, ()->connectedAddresses.contains(ad.address)));
		ad.explode = false;
		Util.setCurrentTimeNanos(1000l*1000l*PeerSwarm.EMBARGO_EXPIRE_TIME_MILLIS);
		assertTrue(Util.waitUntil(200, ()->connectedAddresses.contains(ad.address)));
	}
	
	@Test
	public void testStopsConnectingToAdsWhenMaxSocketCountReached() throws InterruptedException {
		int initial = connectedAddresses.size();
		for(int i = 0; i < 2*swarm.maxSocketCount; i++) {
			swarm.addPeerAdvertisement(new DummyAdvertisement("ad-"+i));
		}
		
		Util.waitUntil(2000, ()->connectedAddresses.size() - initial >= swarm.maxSocketCount);
		assertEquals(swarm.maxSocketCount, connectedAddresses.size() - initial);
	}
	
	@Test
	public void testDoesNotDuplicateConnectionsToAds() throws InterruptedException {
		int initial = connectedAddresses.size();
		for(int i = 0; i < swarm.maxSocketCount; i++) {
			swarm.addPeerAdvertisement(new DummyAdvertisement("ad-1"));
		}
		
		long breakTime = System.currentTimeMillis() + 200;
		while(connectedAddresses.size() < 1 + initial && System.currentTimeMillis() < breakTime) {
			Thread.sleep(1);
		}
		
		Thread.sleep(100);
		assertEquals(1 + initial, connectedAddresses.size());
	}
	
	@Test
	public void testConnectionThreadDoesNotDieFromExceptions() throws InterruptedException {
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(PeerSwarm.class)).setLevel(Level.OFF);
		ExplodingDummyAdvertisement exploding = new ExplodingDummyAdvertisement();
		assertFalse(exploded);
		swarm.addPeerAdvertisement(exploding);
		
		long breakTime = System.currentTimeMillis() + 200;
		while(!exploded && System.currentTimeMillis() < breakTime) {
			Thread.sleep(1);
		}
		
		assertTrue(exploded);
		
		DummyAdvertisement ad = new DummyAdvertisement("some-ad");
		assertFalse(connectedAddresses.contains(ad.address));
		swarm.addPeerAdvertisement(ad);
		breakTime = System.currentTimeMillis() + 200;
		while(!connectedAddresses.contains(ad.address) && System.currentTimeMillis() < breakTime) {
			Thread.sleep(1);
		}
		
		assertTrue(connectedAddresses.contains(ad.address));
	}
	
	@Test
	public void testConnectionThreadStopsWhenClosed() throws InterruptedException {
		swarm.close();
		DummyAdvertisement ad = new DummyAdvertisement("some-ad");
		swarm.addPeerAdvertisement(ad);
		Thread.sleep(100);
		assertFalse(connectedAddresses.contains(ad.address));
	}
	
	@Test
	public void testWaitForPageDoesNotBlockIfWeAlreadyHaveThePage() throws IOException, InterruptedException {
		class Holder { boolean waited; }
		Holder holder = new Holder();
		
		Thread thread = new Thread(()->{
			try {
				swarm.waitForPage(pageTag);
				holder.waited = true;
			} catch (ClosedException e) {
				e.printStackTrace();
			}
		});
		assertFalse(holder.waited);
		thread.start();
		
		long endTime = System.currentTimeMillis() + 10;
		while(!holder.waited && System.currentTimeMillis() < endTime) Thread.sleep(1);
		assertTrue(holder.waited);
	}
	
	@Test
	public void testWaitForPageBlocksUntilPageReceived() throws IOException, InterruptedException {
		class Holder { boolean waited; }
		Holder holder = new Holder();
		
		byte[] tag = archive.getCrypto().rng(archive.getCrypto().hashLength());
		
		Thread thread = new Thread(()->{
			try {
				swarm.waitForPage(tag);
				holder.waited = true;
			} catch (ClosedException e) {
				e.printStackTrace();
			}
		});
		assertFalse(holder.waited);
		thread.start();
		
		long endTime = System.currentTimeMillis() + 100;
		while(!holder.waited && System.currentTimeMillis() < endTime) Thread.sleep(1);
		assertFalse(holder.waited);
		swarm.receivedPage(tag);
		endTime = System.currentTimeMillis() + 100;
		while(!holder.waited && System.currentTimeMillis() < endTime) Thread.sleep(1);
		assertTrue(holder.waited);
	}
	
	@Test
	public void testAccumulatorForTagCreatesAnAccumulatorForNewTags() throws IOException {
		byte[] tag = master.getCrypto().rng(master.getCrypto().hashLength());
		ChunkAccumulator accumulator = swarm.accumulatorForTag(tag);
		assertNotNull(accumulator);
		assertTrue(Arrays.equals(tag, accumulator.tag));
	}
	
	@Test
	public void testAccumulatorForTagReturnsExistingAccumulatorForNewTags() throws IOException {
		byte[] tag = master.getCrypto().rng(master.getCrypto().hashLength());
		ChunkAccumulator accumulator1 = swarm.accumulatorForTag(tag);
		ChunkAccumulator accumulator2 = swarm.accumulatorForTag(tag);
		assertTrue(accumulator1 == accumulator2);
	}
	
	@Test
	public void testAccumulatorForTagResetsWhenReceivePageCalled() throws IOException {
		byte[] tag = master.getCrypto().rng(master.getCrypto().hashLength());
		ChunkAccumulator accumulator1 = swarm.accumulatorForTag(tag);
		swarm.receivedPage(tag);
		ChunkAccumulator accumulator2 = swarm.accumulatorForTag(tag);
		assertTrue(accumulator1 != accumulator2);
		assertTrue(Arrays.equals(tag, accumulator2.tag));
	}
	
	@Test
	public void testRequestShortTagSendsRequestToAllCurrentPeers() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		long shortTag = 1234;
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			assertNotEquals(shortTag, conns[i].requestedTag);
		}
		
		swarm.requestTag(4321, shortTag);
		for(DummyConnection conn : conns) {
			assertEquals(shortTag, conn.requestedTag);
			assertEquals(4321, conn.requestedPriority);
		}
	}
	
	@Test
	public void testRequestShortTagSendsRequestToAllNewPeers() throws IOException {
		long shortTag = 1234;
		DummyConnection conn = new DummyConnection(new DummySocket("10.0.1.1", swarm));
		swarm.requestTag(42, shortTag);
		swarm.openedConnection(conn);
		assertEquals(shortTag, conn.requestedTag);
		assertEquals(42, conn.requestedPriority);
	}
	
	@Test
	public void testRequestTagLongSendsRequestToAllCurrentPeers() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		byte[] tag = archive.getCrypto().rng(archive.getCrypto().hashLength());
		long shortTag = Util.shortTag(tag);
		
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			assertNotEquals(shortTag, conns[i].requestedTag);
		}
		
		swarm.requestTag(0, tag);
		for(DummyConnection conn : conns) {
			assertEquals(shortTag, conn.requestedTag);
		}
	}
	
	@Test
	public void testRequestTagLongSendsRequestToAllNewPeers() throws IOException {
		DummyConnection conn = new DummyConnection(new DummySocket("10.0.1.1", swarm));
		byte[] tag = archive.getCrypto().rng(archive.getCrypto().hashLength());
		long shortTag = Util.shortTag(tag);
		swarm.requestTag(0, tag);
		swarm.openedConnection(conn);
		assertEquals(shortTag, conn.requestedTag);
	}
	
	@Test
	public void testRequestAllSendsRequestToAllCurrentPeers() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			assertFalse(conns[i].requestedAll);
		}
		
		swarm.requestAll();
		for(DummyConnection conn : conns) {
			assertTrue(conn.requestedAll);
		}
	}
	
	@Test
	public void testRequestAllSendsRequestToAllNewPeers() throws IOException {
		DummyConnection conn = new DummyConnection(new DummySocket("10.0.1.1", swarm));
		swarm.requestAll();
		assertFalse(conn.requestedAll);
		swarm.openedConnection(conn);
		assertTrue(conn.requestedAll);
	}
	
	@Test
	public void testStopRequestingAllSendsRequestAllCancelToCurrentPeers() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			assertFalse(conns[i].requestedAll);
		}
		
		swarm.stopRequestingAll();
		for(DummyConnection conn : conns) {
			assertTrue(conn.requestedAllCancel);
		}
	}
	
	@Test
	public void testStopRequestingAllAvoidsSendAllRequestToNewPeers() throws IOException {
		DummyConnection conn = new DummyConnection(new DummySocket("10.0.1.1", swarm));
		swarm.requestAll();
		swarm.stopRequestingAll();
		swarm.openedConnection(conn);
		assertFalse(conn.requestedAll);
	}
	
	@Test
	public void testRequestInodeSendsRequestInodeToAllCurrentPeers() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		RefTag revTag = new RefTag(archive, archive.getCrypto().rng(archive.getConfig().refTagSize()));
		long inodeId = archive.getCrypto().defaultPrng().getLong();
		
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			assertFalse(conns[i].requestedAll);
		}
		
		swarm.requestInode(-44332211, revTag, inodeId);
		for(DummyConnection conn : conns) {
			assertEquals(revTag, conn.requestedRevTag);
			assertEquals(inodeId, conn.requestedInodeId);
			assertEquals(-44332211, conn.requestedPriority);
		}
	}
	
	@Test
	public void testRequestInodeSendsRequestInodeToAllNewPeers() throws IOException {
		RefTag tag = new RefTag(archive, archive.getCrypto().rng(archive.getConfig().refTagSize()));
		long inodeId = archive.getCrypto().defaultPrng().getLong();
		DummyConnection conn = new DummyConnection(new DummySocket("10.0.1.1", swarm));
		swarm.requestInode(Integer.MAX_VALUE, tag, inodeId);
		swarm.openedConnection(conn);
		assertEquals(tag, conn.requestedRevTag);
		assertEquals(inodeId, conn.requestedInodeId);
		assertEquals(Integer.MAX_VALUE, conn.requestedPriority);
	}
	
	@Test
	public void testRequestRevisionSendsRequestRevisionContentsToAllCurrentPeers() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		RefTag tag = new RefTag(archive, archive.getCrypto().rng(archive.getConfig().refTagSize()));
		
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			assertFalse(conns[i].requestedAll);
		}
		
		swarm.requestRevision(Integer.MIN_VALUE, tag);
		for(DummyConnection conn : conns) {
			assertEquals(tag, conn.requestedRevTag);
			assertEquals(Integer.MIN_VALUE, conn.requestedPriority);
		}
	}

	@Test
	public void testRequestRevisionSendsRequestRevisionContentsToAllNewPeers() throws IOException {
		RefTag tag = new RefTag(archive, archive.getCrypto().rng(archive.getConfig().refTagSize()));
		DummyConnection conn = new DummyConnection(new DummySocket("10.0.1.1", swarm));
		swarm.requestRevision(11235813, tag);
		swarm.openedConnection(conn);
		assertEquals(tag, conn.requestedRevTag);
		assertEquals(11235813, conn.requestedPriority);
	}
	
	@Test
	public void testSetPausedTrueSendsPausedTrueToAllCurrentPeers() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			conns[i].requestedPause = false; // clear pause request
		}
		
		swarm.setPaused(true);
		for(DummyConnection conn : conns) {
			assertTrue(conn.requestedPause);
			assertTrue(conn.requestedPauseValue);
		}
	}
	
	@Test
	public void testSetPausedTrueSendsPausedTrueToAllNewPeers() throws IOException {
		DummyConnection conn = new DummyConnection(new DummySocket("10.0.1.1", swarm));
		swarm.setPaused(true);
		swarm.openedConnection(conn);
		assertTrue(conn.requestedPause);
		assertTrue(conn.requestedPauseValue);
	}

	@Test
	public void testSetPausedFalseSendsPausedFalseToAllPeers() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			conns[i].requestedPause = false; // clear pause request
		}
		
		swarm.setPaused(false);
		for(DummyConnection conn : conns) {
			assertTrue(conn.requestedPause);
			assertFalse(conn.requestedPauseValue);
		}
	}

	@Test
	public void testSetPausedFalseSendsPausedFalseToAllNewPeers() throws IOException {
		DummyConnection conn = new DummyConnection(new DummySocket("10.0.1.1", swarm));
		swarm.setPaused(false);
		swarm.openedConnection(conn);
		assertTrue(conn.requestedPause);
		assertFalse(conn.requestedPauseValue);
	}
}
