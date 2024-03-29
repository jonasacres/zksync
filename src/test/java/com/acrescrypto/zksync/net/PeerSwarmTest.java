package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.StorageTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.Util;

import ch.qos.logback.classic.Level;

public class PeerSwarmTest {
	class DummyAdvertisement extends PeerAdvertisement {
		String address;
		boolean blacklisted, explode;
		byte type = -1;
		public DummyAdvertisement() {
			address = "localhost";
			this.pubKey = master.getCrypto().makePrivateDHKey().publicKey();
		}
		
		public DummyAdvertisement(String address) {
			this.address = address;
			this.type = TYPE_TCP_PEER;
			this.pubKey = master.getCrypto().makePrivateDHKey().publicKey();
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
		@Override public String routingInfo() { return ""; }
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
			super(swarm);
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
		RevisionTag requestedRefTag, requestedRevTag, requestedDetailsTag, requestedRevTagStructure;
		
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
		@Override public void requestInodes(int priority, RevisionTag revTag, Collection<Long> inodeIds) {
			requestedPriority = priority;
			this.requestedRevTag = revTag;
			for(Long inodeId : inodeIds) {
				this.requestedInodeId = inodeId;
				break;
			}
		}
		@Override public void requestRevisionContents(int priority, Collection<RevisionTag> tips) {
			requestedPriority = priority;
			for(RevisionTag tip : tips) {
				this.requestedRevTag = tip;
				break;
			}
		}
		
		@Override public void requestRevisionDetails(int priority, Collection<RevisionTag> tags) {
			requestedPriority = priority;
			for(RevisionTag tag : tags) {
				this.requestedDetailsTag = tag;
				break;
			}
		}
		
		@Override public void requestRevisionStructure(int priority, Collection<RevisionTag> tags) {
			requestedPriority = priority;
			for(RevisionTag tag : tags) {
				this.requestedRevTagStructure = tag;
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
	StorageTag pageTag;
	
	PeerSwarm swarm;
	DummyConnection connection;
	ArrayList<String> connectedAddresses = new ArrayList<String>();
	boolean exploded;
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		TestUtils.startDebugMode();
		master = ZKMaster.openBlankTestVolume();
	}
	
	@Before
	public void beforeEach() throws IOException {
		connectedAddresses.clear();
		
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		
		try(ZKFS fs = archive.openBlank()) {
			fs.write("file", new byte[archive.getConfig().getPageSize()]);
			PageTree tree = new PageTree(fs.inodeForPath("file"));
			pageTag = tree.getPageTag(0);
	
			swarm = archive.getConfig().getSwarm();
			exploded = false;
			connection = new DummyConnection(new DummySocket("127.0.0.1", swarm));
			connection.socket.ad = new DummyAdvertisement();
		}
	}
	
	@After
	public void afterEach() {
		connection.close();
		archive.close();
		try { Thread.sleep(1); } catch(InterruptedException exc) {} // give exploding ads a chance to percolate through before turning logging back on
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(PeerSwarm.class)).setLevel(Level.WARN);
		Util.setCurrentTimeNanos(-1);
	}
	
	@AfterClass
	public static void afterAll() {
		master.close();
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	public StorageTag makeRandomStorageTag(int i) {
		return new StorageTag(archive.getCrypto(), archive.getCrypto().hash(Util.serializeInt(i)));
	}
	
	public RefTag makeRandomRefTag(int i) {
		return new RefTag(archive, makeRandomStorageTag(i), RefTag.REF_TYPE_INDIRECT, 1);
	}
	
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
	public void testSoftEmbaroesUnconnectablePeers() throws InterruptedException {
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(PeerSwarm.class)).setLevel(Level.OFF);
		DummyAdvertisement ad = new DummyAdvertisement("some-ad");
		Util.setCurrentTimeNanos(0);
		ad.explode = true;
		assertFalse(connectedAddresses.contains(ad.address));
		swarm.addPeerAdvertisement(ad);
		assertFalse(Util.waitUntil(200, ()->connectedAddresses.contains(ad.address)));
		ad.explode = false;
		Util.setCurrentTimeMillis(PeerSwarm.EMBARGO_SOFT_EXPIRE_TIME_MS);
		assertTrue(Util.waitUntil(200, ()->connectedAddresses.contains(ad.address)));
	}

	@Test
	public void testHardEmbaroesConsistentlyUnconnectablePeers() throws InterruptedException {
		((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(PeerSwarm.class)).setLevel(Level.OFF);
		DummyAdvertisement ad = new DummyAdvertisement("some-ad");
		Util.setCurrentTimeNanos(0);
		ad.explode = true;
		assertFalse(connectedAddresses.contains(ad.address));
		swarm.addPeerAdvertisement(ad);
		while(ad.failCount < PeerSwarm.EMBARGO_FAIL_COUNT_THRESHOLD) {
			int count = ad.failCount;
			Util.setCurrentTimeMillis(Util.currentTimeMillis() + PeerSwarm.EMBARGO_SOFT_EXPIRE_TIME_MS);
			Util.waitUntil(50, ()->ad.failCount > count);
		}
		assertFalse(Util.waitUntil(200, ()->connectedAddresses.contains(ad.address)));
		ad.explode = false;
		Util.setCurrentTimeMillis(Util.currentTimeMillis() + PeerSwarm.EMBARGO_EXPIRE_TIME_MS);
		assertTrue(Util.waitUntil(200, ()->connectedAddresses.contains(ad.address)));
	}

	@Test
	public void testStopsConnectingToAdsWhenMaxSocketCountReached() throws InterruptedException {
		int initial = connectedAddresses.size();
		for(int i = 0; i < 2*swarm.getMaxSocketCount(); i++) {
			swarm.addPeerAdvertisement(new DummyAdvertisement("ad-"+i));
		}
		
		Util.waitUntil(2000, ()->connectedAddresses.size() - initial >= swarm.getMaxSocketCount());
		assertEquals(swarm.getMaxSocketCount(), connectedAddresses.size() - initial);
	}
	
	@Test
	public void testDoesNotDuplicateConnectionsToAds() throws InterruptedException {
		int initial = connectedAddresses.size();
		for(int i = 0; i < swarm.getMaxSocketCount(); i++) {
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
				swarm.waitForPage(0, pageTag);
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}
			holder.waited = true;
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
		
		byte[] tagBytes = archive.getCrypto().hash(Util.serializeInt(1));
		StorageTag tag = new StorageTag(archive.getCrypto(), tagBytes);
		
		Thread thread = new Thread(()->{
			try {
				swarm.waitForPage(0, tag);
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}
			holder.waited = true;
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
	public void testWaitForPageAutomaticallyMakesRequest() throws IOException {
		byte[] tagBytes = archive.getCrypto().hash(Util.serializeInt(1));
		StorageTag tag = new StorageTag(archive.getCrypto(), tagBytes);
		
		DummyConnection[] conns = new DummyConnection[16];
		long shortTag = tag.shortTag();
		
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
		}
		
		new Thread(()->{
			try {
				swarm.waitForPage(0, tag);
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}
		}).start();
		
		for(DummyConnection conn : conns) {
			assertTrue(Util.waitUntil(50, ()->conn.requestedTag == shortTag));
		}
		
		swarm.receivedPage(tag);
	}
	
	@Test
	public void testWaitForPageRepeatsRequest() throws IOException {
		swarm.waitPageRetryTimeMs = 50;
		byte[] tagBytes = archive.getCrypto().hash(Util.serializeInt(1));
		StorageTag tag = new StorageTag(archive.getCrypto(), tagBytes);
		
		DummyConnection[] conns = new DummyConnection[16];
		long shortTag = tag.shortTag();
		
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
		}
		
		new Thread(()->{
			try {
				swarm.waitForPage(0, tag);
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}
		}).start();
		
		for(DummyConnection conn : conns) {
			assertTrue(Util.waitUntil(swarm.waitPageRetryTimeMs+10, ()->conn.requestedTag == shortTag));
		}
		
		for(DummyConnection conn : conns) {
			conn.requestedTag = -1;
		}
		
		for(DummyConnection conn : conns) {
			assertTrue(Util.waitUntil(swarm.waitPageRetryTimeMs+10, ()->conn.requestedTag == shortTag));
		}
		
		swarm.receivedPage(tag);
	}
	
	@Test
	public void testAccumulatorForTagCreatesAnAccumulatorForNewTags() throws IOException {
		byte[] tagBytes = archive.getCrypto().hash(Util.serializeInt(1));
		StorageTag tag = new StorageTag(archive.getCrypto(), tagBytes);
		ChunkAccumulator accumulator = swarm.accumulatorForTag(tag);
		assertNotNull(accumulator);
		assertEquals(tag, accumulator.tag);
	}
	
	@Test
	public void testAccumulatorForTagReturnsExistingAccumulatorForNewTags() throws IOException {
		byte[] tagBytes = archive.getCrypto().hash(Util.serializeInt(1));
		StorageTag tag = new StorageTag(archive.getCrypto(), tagBytes);
		ChunkAccumulator accumulator1 = swarm.accumulatorForTag(tag);
		ChunkAccumulator accumulator2 = swarm.accumulatorForTag(tag);
		assertTrue(accumulator1 == accumulator2);
	}
	
	@Test
	public void testAccumulatorForTagResetsWhenReceivePageCalled() throws IOException {
		byte[] tagBytes = archive.getCrypto().hash(Util.serializeInt(1));
		StorageTag tag = new StorageTag(archive.getCrypto(), tagBytes);
		ChunkAccumulator accumulator1 = swarm.accumulatorForTag(tag);
		swarm.receivedPage(tag);
		ChunkAccumulator accumulator2 = swarm.accumulatorForTag(tag);
		assertTrue(accumulator1 != accumulator2);
		assertEquals(tag, accumulator2.tag);
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
		byte[] tagBytes = archive.getCrypto().hash(Util.serializeInt(1));
		StorageTag tag = new StorageTag(archive.getCrypto(), tagBytes);
		long shortTag = tag.shortTag();
		
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
		byte[] tagBytes = archive.getCrypto().hash(Util.serializeInt(1));
		StorageTag tag = new StorageTag(archive.getCrypto(), tagBytes);
		long shortTag = tag.shortTag();
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
		RefTag refTag = makeRandomRefTag(0);
		RevisionTag revTag = new RevisionTag(refTag, 0, 0, false);
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
		RefTag refTag = makeRandomRefTag(0);
		RevisionTag revTag = new RevisionTag(refTag, 0, 0, false);
		long inodeId = archive.getCrypto().defaultPrng().getLong();
		DummyConnection conn = new DummyConnection(new DummySocket("10.0.1.1", swarm));
		swarm.requestInode(Integer.MAX_VALUE, revTag, inodeId);
		swarm.openedConnection(conn);
		assertEquals(revTag, conn.requestedRevTag);
		assertEquals(inodeId, conn.requestedInodeId);
		assertEquals(Integer.MAX_VALUE, conn.requestedPriority);
	}
	
	@Test
	public void testRequestRevisionSendsRequestRevisionContentsToAllCurrentPeers() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		RefTag refTag = makeRandomRefTag(0);
		RevisionTag revTag = new RevisionTag(refTag, 0, 0, false);
		
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			assertFalse(conns[i].requestedAll);
		}
		
		swarm.requestRevision(Integer.MIN_VALUE, revTag);
		for(DummyConnection conn : conns) {
			assertEquals(revTag, conn.requestedRevTag);
			assertEquals(Integer.MIN_VALUE, conn.requestedPriority);
		}
	}

	@Test
	public void testRequestRevisionSendsRequestRevisionContentsToAllNewPeers() throws IOException {
		RefTag refTag = makeRandomRefTag(0);
		RevisionTag revTag = new RevisionTag(refTag, 0, 0, false);
		DummyConnection conn = new DummyConnection(new DummySocket("10.0.1.1", swarm));
		swarm.requestRevision(11235813, revTag);
		swarm.openedConnection(conn);
		assertEquals(revTag, conn.requestedRevTag);
		assertEquals(11235813, conn.requestedPriority);
	}
	
	@Test
	public void testRequestRevisionStructureSendsRequestRevisionStructureToAllCurrentPeers() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		RefTag refTag = makeRandomRefTag(0);
		RevisionTag revTag = new RevisionTag(refTag, 0, 0, false);
		
		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			assertFalse(conns[i].requestedAll);
		}
		
		swarm.requestRevisionStructure(Integer.MIN_VALUE, revTag);
		for(DummyConnection conn : conns) {
			assertEquals(revTag, conn.requestedRevTagStructure);
			assertEquals(Integer.MIN_VALUE, conn.requestedPriority);
		}
	}

	@Test
	public void testRequestRevisionStructureSendsRequestRevisionStructureToAllNewPeers() throws IOException {
		RefTag refTag = makeRandomRefTag(0);
		RevisionTag revTag = new RevisionTag(refTag, 0, 0, false);
		DummyConnection conn = new DummyConnection(new DummySocket("10.0.1.1", swarm));
		swarm.requestRevisionStructure(11235813, revTag);
		swarm.openedConnection(conn);
		assertEquals(revTag, conn.requestedRevTagStructure);
		assertEquals(11235813, conn.requestedPriority);
	}

	
	@Test
	public void testRequestRevisionDetailsSendsRequestRevisionDetailsToAllCurrentPeers() throws IOException {
		DummyConnection[] conns = new DummyConnection[16];
		RefTag refTag = makeRandomRefTag(0);
		RevisionTag revTag = new RevisionTag(refTag, 0, 0, false);

		for(int i = 0; i < conns.length; i++) {
			conns[i] = new DummyConnection(new DummySocket("10.0.1." + i, swarm));
			swarm.openedConnection(conns[i]);
			assertFalse(conns[i].requestedAll);
		}
		
		swarm.requestRevisionDetails(Integer.MIN_VALUE, revTag);
		for(DummyConnection conn : conns) {
			assertEquals(revTag, conn.requestedDetailsTag);
			assertEquals(Integer.MIN_VALUE, conn.requestedPriority);
		}
	}
	
	@Test
	public void testRequestRevisionDetailsSendsRequestRevisionDetailsToAllNewPeers() throws IOException {
		RefTag refTag = makeRandomRefTag(0);
		RevisionTag revTag = new RevisionTag(refTag, 0, 0, false);
		DummyConnection conn = new DummyConnection(new DummySocket("10.0.1.1", swarm));
		swarm.requestRevisionDetails(-48151623, revTag);
		swarm.openedConnection(conn);
		assertEquals(revTag, conn.requestedDetailsTag);
		assertEquals(-48151623, conn.requestedPriority);
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
