package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;

public class PeerSwarmTest {
	class DummyAdvertisement extends PeerAdvertisement {
		String address;
		boolean blacklisted;
		int type = -1;
		@Override public boolean isBlacklisted(Blacklist blacklist) throws IOException { return blacklisted; }
		@Override public byte[] serialize() { return null; }
		@Override public boolean matchesAddress(String address) { return this.address.equals(address); }
		@Override public int getType() { return type; }
	}
	
	class DummySocket extends PeerSocket {
		protected String address = "dummy";
		protected DummyAdvertisement ad;
		public DummySocket(String address, PeerSwarm swarm) {
			this.address = address;
			this.swarm = swarm;
		}
		
		@Override public DummyAdvertisement getAd() { return ad; }
		@Override public void write(byte[] data, int offset, int length) {}
		@Override public int read(byte[] data, int offset, int length) { return 0; }
		@Override public boolean isClient() { return false; }
		@Override public void close() {}
		@Override public boolean isClosed() { return false; }
		@Override public byte[] getSharedSecret() { return null; }
		@Override public String getAddress() { return address; }
	}
	
	class DummyConnection extends PeerConnection {
		DummySocket socket;
		PeerAdvertisement seenAd;
		boolean closed;
		
		public DummyConnection(DummySocket socket) {
			super(socket);
			this.socket = socket;
		}
		
		@Override public DummySocket getSocket() { return socket; }
		@Override public void close() { this.closed = true; }
		@Override public void announceSelf(PeerAdvertisement ad) { this.seenAd = ad; }
	}
	
	static ZKMaster master;
	static ZKArchive archive;
	PeerSwarm swarm;
	DummyConnection connection;
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
	}
	
	@Before
	public void before() {
		swarm = new PeerSwarm(archive.getConfig());
		connection = new DummyConnection(new DummySocket("127.0.0.1", swarm));
		connection.socket.ad = new DummyAdvertisement();
	}
	
	@After
	public void after() {
		swarm.close();
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
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
	public void testDisconnectAddressClosesAllConnectionsFromAddress() {
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
	public void testDisconnectAddressDoesntCloseOtherAddresses() {
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
	public void testCloseDisconnectsAllPeerConnections() {
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
	public void testAdvertiseSelfRelaysAdToAllConnections() {
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
	
	// automatically connects to advertised peers
	// stops connecting to advertised peers when maxSocketCount reached
	// connection thread does not die from exceptions
	// connection thread stops when closed
	
	// waitForPage does not block if we already have the page
	// waitForPage blocks until a page is announced via receivedPage
	
	// accumulatorForTag creates an accumulator for new tags
	// accumulatorForTag returns an existing accumulator for active tags
	// accumulatorForTag no longer considers a tag active after receivedPage has been called for that tag
	
	// receivedPage announces tag to peers
}
