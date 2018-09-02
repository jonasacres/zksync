package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.net.ConnectException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerSocketListenerTest {
	class DummySwarm extends PeerSwarm {
		TCPPeerAdvertisement announced;
		PeerConnection opened;
		
		public DummySwarm(ZKArchiveConfig config) throws IOException { super(config); }
		@Override public void advertiseSelf(PeerAdvertisement ad) { announced = (TCPPeerAdvertisement) ad; }
		@Override public void openedConnection(PeerConnection connection) {
			if(closed) connection.close();
			opened = connection;
		}
	}
	
	public final static int TEST_PORT = 44583; // chosen randomly in hopes of not stepping on active local ports on test system
	
	static CryptoSupport crypto;
	static ZKMaster master;
	static ZKArchive archive;
	
	TCPPeerSocketListener listener;
	DummySwarm swarm;
	
	protected Socket connect() throws UnknownHostException, IOException {
		return connect(listener);
	}
	
	protected Socket connect(TCPPeerSocketListener theListener) throws UnknownHostException, IOException {
		Util.waitUntil(100, ()->theListener.listenSocket != null);
		assertNotNull(theListener.listenSocket);
		return new Socket("localhost", theListener.port);
	}
	
	protected void assertSocketClosed(Socket socket, long startTime, int minDelay) {
		MutableBoolean closed = new MutableBoolean();
		
		Thread thread = new Thread(()-> {
			try {
				IOUtils.readFully(socket.getInputStream(), new byte[1]);
			} catch (IOException e) {
				closed.setTrue();
			}
		});
		
		thread.start();
		try {
			thread.join(TCPHandshakeContext.closeSocketDelayMs + 50);
		} catch (InterruptedException exc) {}
		
		// we should have waited about as long as the close delay, with a couple milliseconds knocked off to account for the time to get here
		assertTrue(System.currentTimeMillis() - startTime >= minDelay);
		
		assertTrue(closed.booleanValue());
	}
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		crypto = new CryptoSupport();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createDefaultArchive();
	}
	
	@SuppressWarnings("deprecation")
	@Before
	public void beforeEach() throws IOException, InvalidBlacklistException {
		master.getBlacklist().clear();
		TCPPeerSocket.disableMakeThreads = true;
		TCPHandshakeContext.closeSocketDelayMs = 100;
		TCPPeerSocket.maxHandshakeTimeMillis = 250;
		listener = new TCPPeerSocketListener(master, 0);
		master.setTCPListener(listener);
		swarm = new DummySwarm(archive.getConfig());
	}
	
	@After
	public void afterEach() throws IOException {
		if(swarm.opened != null) {
			swarm.opened.close();
		}
		
		swarm.close();
		listener.close();
	}
	
	@AfterClass
	public static void afterAll() {
		archive.close();
		master.close();
		TCPPeerSocket.disableMakeThreads = false;
		TCPHandshakeContext.closeSocketDelayMs = TCPHandshakeContext.CLOSE_SOCKET_DELAY_MS_DEFAULT;
		TCPPeerSocket.maxHandshakeTimeMillis = TCPPeerSocket.DEFAULT_MAX_HANDSHAKE_TIME_MILLIS;
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testBeginsListeningWhenConstructorCalled() throws UnknownHostException, IOException {
		connect();
	}
	
	@Test
	public void testListensOnRequstedPortWhenSpecified() throws UnknownHostException, IOException {
		/* Beware! This test wants a specific TCP port, which the OS will only let one process have at a time.
		 * It's pretty easy in eclipse to have a process suspended in the debugger that you then forget about and leave
		 * hanging, occupying this socket and triggering failures in this test.
		 */
		TCPPeerSocketListener specific = new TCPPeerSocketListener(master, TEST_PORT);
		Socket socket = connect(specific);
		assertEquals(TEST_PORT, specific.requestedPort);
		assertEquals(TEST_PORT, specific.port);
		assertEquals(TEST_PORT, specific.listenSocket.getLocalPort());
		assertEquals(TEST_PORT, socket.getPort());
		specific.close();
	}
	
	@Test
	public void testListensOnRandomPortWhenZeroSpecified() throws IOException {
		TCPPeerSocketListener listener2 = new TCPPeerSocketListener(master, 0);
		Util.waitUntil(100, ()->listener.listenSocket != null);
		Util.waitUntil(100, ()->listener2.listenSocket != null);
		assertNotEquals(listener.port, listener2.port);
		listener2.close();
	}
	
	@Test
	public void testReusesPreviousPort() throws IOException, InterruptedException {
		Util.waitUntil(100, ()->listener.listenSocket != null);
		int port = listener.port;
		assertNotEquals(0, port);
		listener.close();
		
		Thread.sleep(10); // give OS some time to free up the socket
		TCPPeerSocketListener listener2 = new TCPPeerSocketListener(master, 0);
		Util.waitUntil(100, ()->listener2.listenSocket != null);
		assertEquals(port, listener2.port);
		listener2.close();
	}
	
	@Test
	public void testReallocationOfPortIfLastPortUnavailableAndZeroRequested() throws IOException {
		Util.waitUntil(100, ()->listener.listenSocket != null);
		int port = listener.port;
		assertNotEquals(0, port);
		
		TCPPeerSocketListener listener2 = new TCPPeerSocketListener(master, 0);
		Util.waitUntil(100, ()->listener2.listenSocket != null);
		assertNotNull(listener2.listenSocket);
		assertNotEquals(listener.port, listener2.port);
		listener2.close();
	}
	
	@Test
	public void testNonReallocationOfPortIfLastPortUnavailableAndNonzeroRequested() throws IOException {
		Util.waitUntil(100, ()->listener.listenSocket != null);
		int port = listener.port;
		assertNotEquals(0, port);

		TCPPeerSocketListener listener2 = new TCPPeerSocketListener(master, port);
		Util.waitUntil(100, ()->listener2.listenSocket != null);
		assertNull(listener2.listenSocket);
		
		listener.close();
		Util.waitUntil(2000, ()->listener2.listenSocket != null);
		assertNotNull(listener2.listenSocket);
		assertEquals(port, listener2.port);
		listener2.close();
	}
	
	@Test
	public void testStopsListeningWhenCloseCalled() throws UnknownHostException, IOException, InvalidBlacklistException {
		connect();
		listener.close();
		Util.sleep(25); // observed socket staying open briefly after connection close on Linux 4.15.0-23-generic
		try {
			connect();
			fail();
		} catch(ConnectException exc) {}
	}
	
	@Test
	public void testGetPortReturnsCurrentPort() {
		Util.waitUntil(100, ()->listener.listenSocket != null);
		assertNotEquals(listener.getPort(), 0);
		assertEquals(listener.listenSocket.getLocalPort(), listener.getPort());
		assertEquals(listener.port, listener.getPort());
	}
	
	@Test
	public void testAdvertiseInstantiatesNewAdListener() {
		listener.advertise(swarm);
		Util.waitUntil(100, ()->swarm.announced != null);
		assertNotNull(swarm.announced);
		assertEquals(listener.getPort(), swarm.announced.port);
	}
	
	@Test
	public void testListenerForSwarmReturnsNullIfSwarmNotAdvertised() {
		assertNull(listener.listenerForSwarm(swarm));
	}
	
	@Test
	public void testListenerForSwarmReturnsListenerIfSwarmAdvertised() throws IOException, InvalidBlacklistException, UnconnectableAdvertisementException {
		listener.advertise(swarm);
		TCPPeerAdvertisementListener adListener = listener.listenerForSwarm(swarm);
		assertNotNull(adListener);
		assertEquals(listener.getPort(), adListener.localAd().port);
		assertEquals(swarm, adListener.swarm);
	}
	
	@Test
	public void testDisconnectsBlacklistedPeers() throws IOException {
		listener.advertise(swarm);
		master.getBlacklist().add("127.0.0.1", 60000);
		Socket socket = connect();
		assertSocketClosed(socket, 0, 0);
	}
	
	@Test
	public void testDisconnectsPeersBeforeAdvertisedSwarms() throws UnknownHostException, IOException {
		Socket socket = connect();
		assertSocketClosed(socket, 0, 0);
		socket.close();
	}
	
	@Test
	public void testHandshakesWithPeers() throws IOException, ProtocolViolationException {
		ZKMaster cMaster = ZKMaster.openBlankTestVolume();
		ZKArchive cArchive = cMaster.createDefaultArchive();
		PeerSwarm cSwarm = cArchive.getConfig().getSwarm();
		listener.advertise(swarm);

		Socket socket = connect(); 
		TCPHandshakeContext clientHandshake = new TCPHandshakeContext(cSwarm,
				socket,
				swarm.getPublicIdentityKey());
		clientHandshake.establishSecret();
		
		cArchive.close();
		cMaster.close();
	}
	
	@Test
	public void testHandshakeProtocolViolationsTriggersDelayedClose() throws IOException, ProtocolViolationException {
		ZKMaster cMaster = ZKMaster.openBlankTestVolume();
		ZKArchive cArchive = cMaster.createDefaultArchive();
		PeerSwarm cSwarm = cArchive.getConfig().getSwarm();
		listener.advertise(swarm);
		long startTime = System.currentTimeMillis();

		Socket socket = connect();
		TCPHandshakeContext clientHandshake = new TCPHandshakeContext(cSwarm,
				socket,
				crypto.makePrivateDHKey().publicKey()); // bad public key will cause protocol violation
		try {
			clientHandshake.establishSecret();
			fail();
		} catch(Exception exc) {}
		
		assertSocketClosed(socket, startTime, TCPHandshakeContext.closeSocketDelayMs);
		
		cArchive.close();
		cMaster.close();
	}
}
