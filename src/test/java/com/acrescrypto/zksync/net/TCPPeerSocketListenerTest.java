package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import org.junit.Ignore;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.noise.HandshakeState;
import com.acrescrypto.zksync.utility.Util;
import com.dosse.upnp.UPnP;

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
	
	class DummyConnection extends PeerConnection {
	}
	
	public final static int TEST_PORT = 44583; // chosen randomly in hopes of not stepping on active local ports on test system
	
	static CryptoSupport crypto;
	
	ZKMaster master;
	ZKArchive archive;
	TCPPeerSocketListener listener;
	DummySwarm swarm;
	PrivateDHKey peerKey;
	byte[] keyKnowledgeProof, keyHash, staticKeyCiphertext, timeProof;
	
	protected TCPPeerSocket peerSocket() throws UnsupportedProtocolException, IOException, ProtocolViolationException, BlacklistedException, UnconnectableAdvertisementException {
		Util.waitUntil(100, ()->listener.listenSocket != null && listener.port != 0);
		listener.advertise(swarm);
		byte[] encArchiveId = archive.getConfig().getEncryptedArchiveId(listener.getIdentityKey().publicKey().getBytes());
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(listener.getIdentityKey().publicKey(), "localhost", listener.port, encArchiveId);
		ad.resolve();
		
		ArchiveAccessor accessor2 = new ArchiveAccessor(master, archive.getConfig().getAccessor());
		try(ZKArchiveConfig config2 = ZKArchiveConfig.openExisting(accessor2, archive.getConfig().getArchiveId())) {
    		DummySwarm swarm2 = new DummySwarm(config2);
    		
    		TCPPeerSocket socket = new TCPPeerSocket(swarm2, ad);
    		socket.connect(new DummyConnection());
    		return socket;
		}
	}
	
	protected Socket connect() throws UnknownHostException, IOException {
		return connect(listener);
	}
	
	protected Socket connect(TCPPeerSocketListener theListener) throws UnknownHostException, IOException {
		Util.waitUntil(100, ()->theListener.listenSocket != null && theListener.port != 0);
		assertNotNull(theListener.listenSocket);
		return new Socket("localhost", theListener.port);
	}
	
	protected void assertSocketClosed(Socket socket, boolean immediate) {
		MutableBoolean closed = new MutableBoolean();
		
		Thread thread = new Thread(()-> {
			try {
				IOUtils.readFully(socket.getInputStream(), new byte[1]);
			} catch (IOException e) {
				closed.setTrue();
			}
		});
		
		long startTime = System.currentTimeMillis();
		thread.start();
		try {
			thread.join(TCPPeerSocket.socketCloseDelay + 50);
		} catch (InterruptedException exc) {}
		
		if(!immediate) {
			/* setting the minimum delay here is tricky.
			 * we want to wait long enough to prove that we closed because of the
			 * socket timeout; but the socket started timing out before this
			 * method even gets called, and we don't know how long that's been.
			 * 
			 * right now, just pick a reasonable fudge interval -- this is the
			 * maximum length of time we'll expect the socket to have been timing
			 * out before getting to the point where we calculate startTime.
			 * this will probably have to get tuned as this test runs on different
			 * systems, and maybe a better solution will present itself.
			 */
			long fudgeMs     = 25;
			long elapsed     = System.currentTimeMillis() - startTime;
			long minDuration = TCPPeerSocket.socketCloseDelay - fudgeMs;
			
			assertTrue(elapsed >= minDuration);
		}
		assertTrue(closed.isTrue());
	}
	
	protected byte[] readData(Socket socket, int length) {
		class Holder { boolean closed; }
		Holder holder = new Holder();
		byte[] data = new byte[length];

		Thread thread = new Thread(()-> {
			try {
				IOUtils.readFully(socket.getInputStream(), data);
			} catch (IOException e) {
				holder.closed = true;
			}
		});
		
		thread.start();
		try {
			thread.join(10);
		} catch(InterruptedException exc) {}
		assertFalse(holder.closed);
		return data;
	}
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		TestUtils.startDebugMode();
		crypto = CryptoSupport.defaultCrypto();
	}
	
	@Before
	public void beforeEach() throws IOException, InvalidBlacklistException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		TCPPeerSocket.disableMakeThreads = true;
		TCPPeerSocket.socketCloseDelay = 50;
		TCPPeerSocket.maxHandshakeTimeMillis = 250;
		master.getGlobalConfig().set("net.swarm.enabled", true);
		listener = master.getTCPListener();
		swarm = new DummySwarm(archive.getConfig());
		peerKey = crypto.makePrivateDHKey();
	}
	
	@After
	public void afterEach() throws IOException {
		Util.waitUntil(10, ()->listener.established);
		if(swarm.opened != null) {
			swarm.opened.close();
		}
		
		swarm.close();
		listener.close();
		archive.close();
		master.close();
		UPnP.clearDebugMappings();
	}
	
	@AfterClass
	public static void afterAll() {
		TCPPeerSocket.disableMakeThreads = false;
		TCPPeerSocket.socketCloseDelay = TCPPeerSocket.DEFAULT_SOCKET_CLOSE_DELAY;
		TCPPeerSocket.maxHandshakeTimeMillis = TCPPeerSocket.DEFAULT_MAX_HANDSHAKE_TIME_MILLIS;
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testBeginsListeningWhenConstructorCalled() throws UnknownHostException, IOException {
		connect();
	}
	
	@Test @Ignore // TODO: Disabled due to chronic ITFs
	public void testListensOnRequstedPortWhenSpecified() throws UnknownHostException, IOException {
		/* Beware! This test wants a specific TCP port, which the OS will only let one process have at a time.
		 * It's pretty easy in eclipse to have a process suspended in the debugger that you then forget about and leave
		 * hanging, occupying this socket and triggering failures in this test.
		 */
		ZKMaster master2 = ZKMaster.openBlankTestVolume();
		master2.getGlobalConfig().set("net.swarm.port", TEST_PORT);
		master2.getGlobalConfig().set("net.swarm.enabled", true);
		TCPPeerSocketListener specific = master2.getTCPListener();
		assertTrue(Util.waitUntil(100, ()->specific.listenSocket != null && specific.listenSocket.getLocalPort() == TEST_PORT && specific.port == TEST_PORT));

		Socket socket = connect(specific);
		assertEquals(TEST_PORT, socket.getPort());
		
		specific.close();
	}
	
	@Test
	public void testListensOnRandomPortWhenZeroSpecified() throws IOException {
		TCPPeerSocketListener listener2 = new TCPPeerSocketListener(master);
		Util.waitUntil(100, ()->listener.listenSocket != null);
		Util.waitUntil(100, ()->listener2.listenSocket != null);
		assertNotEquals(listener.port, listener2.port);
		listener2.close();
	}
	
	@Test
	public void testReusesPreviousPort() throws IOException, InterruptedException {
		Util.waitUntil(100, ()->listener.listenSocket != null && listener.port != 0);
		int port = listener.port;
		listener.close();
		
		Thread.sleep(10); // give OS some time to free up the socket
		TCPPeerSocketListener listener2 = new TCPPeerSocketListener(master);
		Util.waitUntil(100, ()->listener2.listenSocket != null);
		Util.waitUntil(100, ()->listener2.port == port);
		assertEquals(port, listener2.port);
		listener2.close();
	}
	
	@Test
	public void testReallocationOfPortIfLastPortUnavailableAndZeroRequested() throws IOException {
		Util.waitUntil(100, ()->listener.listenSocket != null && listener.port != 0);
		int port = listener.port;
		assertNotEquals(0, port);
		
		TCPPeerSocketListener listener2 = new TCPPeerSocketListener(master);
		Util.waitUntil(100, ()->listener2.listenSocket != null && listener2.port != 0);
		assertNotNull(listener2.listenSocket);
		assertNotEquals(listener.port, listener2.port);
		listener2.close();
	}
	
	@Test @Ignore
	public void testNonReallocationOfPortIfLastPortUnavailableAndNonzeroRequested() throws IOException {
		master.getGlobalConfig().set("net.swarm.port", TEST_PORT);
		Util.waitUntil(100, ()->listener.listenSocket != null && listener.port == TEST_PORT);

		TCPPeerSocketListener listener2 = new TCPPeerSocketListener(master);
		Util.waitUntil(1000, ()->listener2.listenSocket != null && listener2.port != 0);
		assertNull(listener2.listenSocket); // TODO: itf here, 7/14/20, 4663ff8, linux, increased timeout from 100ms to 1000ms
		
		listener.close();
		Util.waitUntil(2000, ()->listener2.listenSocket != null && listener2.port != 0);
		assertNotNull(listener2.listenSocket);
		assertEquals(TEST_PORT, listener2.port);
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
		Util.waitUntil(100, ()->listener.listenSocket != null && listener.port != 0);
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
		assertTrue(Util.waitUntil(100, ()->listener.getPort() != 0));
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
		assertSocketClosed(socket, true);
	}
	
	@Test
	public void testDisconnectsPeersBeforeAdvertisedSwarms() throws UnknownHostException, IOException {
		Socket socket = connect();
		assertSocketClosed(socket, false);
		socket.close();
	}
	
	// TODO: unrecognized archive ID

	@Test
	public void testMarksPeersBlindIfWrongProofSent() throws UnknownHostException, IOException, UnconnectableAdvertisementException, UnsupportedProtocolException, ProtocolViolationException, BlacklistedException {
		TCPPeerSocket peerSocket = peerSocket();
		HandshakeState handshake = peerSocket.setupHandshakeState();
		peerSocket.getSwarm().getConfig().getAccessor().becomeSeedOnly();
		handshake.handshake(peerSocket.in, peerSocket.out);
		assertTrue(Util.waitUntil(100, ()->swarm.opened != null));
		assertEquals(PeerConnection.PEER_TYPE_BLIND, swarm.opened.getPeerType());
	}
	
	@Test
	public void testMarksPeersBlindIfOwnArchiveIsSeedOnly() throws UnknownHostException, IOException, UnconnectableAdvertisementException, UnsupportedProtocolException, ProtocolViolationException, BlacklistedException {
		TCPPeerSocket peerSocket = peerSocket();
		swarm.getConfig().getAccessor().becomeSeedOnly();
		HandshakeState handshake = peerSocket.setupHandshakeState();
		handshake.handshake(peerSocket.in, peerSocket.out);
		assertTrue(Util.waitUntil(100, ()->swarm.opened != null));
		assertEquals(PeerConnection.PEER_TYPE_BLIND, swarm.opened.getPeerType());
	}
	
	@Test
	public void testMarksPeersFullIfCorrectProofAndArchiveHasReadAccess() throws IOException, UnconnectableAdvertisementException, UnsupportedProtocolException, ProtocolViolationException, BlacklistedException {
		// TODO Urgent: (itf) 2018-12-19 Linux UniversalTests 6ae6644, ConnectException: ConnectionRefused
		TCPPeerSocket peerSocket = peerSocket();
		HandshakeState handshake = peerSocket.setupHandshakeState();
		handshake.handshake(peerSocket.in, peerSocket.out);
		assertTrue(Util.waitUntil(100, ()->swarm.opened != null));
		assertEquals(PeerConnection.PEER_TYPE_FULL, swarm.opened.getPeerType());
	}
	
	@Test
	public void testSendsValidProofWhenClientProofValidAndArchiveHasReadAccess() throws UnconnectableAdvertisementException, IOException, UnsupportedProtocolException, ProtocolViolationException, BlacklistedException {
		// TODO Urgent: (itf) 2018-12-17 Linux UniversalTests 656c833, ConnectException: ConnectionRefused
		TCPPeerSocket peerSocket = peerSocket();
		HandshakeState handshake = peerSocket.setupHandshakeState();
		handshake.handshake(peerSocket.in, peerSocket.out);
		assertEquals(PeerConnection.PEER_TYPE_FULL, peerSocket.getPeerType());
	}
	
	
	@Test
	public void testDoesNotInvokeUPnPIfDisabled() {
		assertFalse(UPnP.isMappedTCP(listener.getPort()));
	}
	
	@Test
	public void testOpensUPnPWhenEnabledAfterInit() {
		master.getGlobalConfig().set("net.swarm.upnp", true);
		Util.waitUntil(1000, ()->UPnP.isMappedTCP(listener.getPort()));
	}
	
	@Test
	public void testOpensUPnPWhenEnabledBeforeInit() throws IOException {
		listener.close();
		master.getGlobalConfig().set("net.swarm.upnp", true);
		listener = new TCPPeerSocketListener(master);
		Util.waitUntil(1000, ()->UPnP.isMappedTCP(listener.getPort()));
	}
	
	@Test
	public void testClosesUPnPAtCloseTime() throws IOException {
		master.getGlobalConfig().set("net.swarm.upnp", true);
		Util.waitUntil(1000, ()->UPnP.isMappedTCP(listener.getPort()));
		listener.close();
		Util.waitUntil(1000, ()->!UPnP.isMappedTCP(listener.getPort()));
	}
	
	@Test
	public void testClosesUPnPWhenDisabled() throws IOException {
		master.getGlobalConfig().set("net.swarm.upnp", true);
		Util.waitUntil(1000, ()->UPnP.isMappedTCP(listener.getPort()));
		master.getGlobalConfig().set("net.swarm.upnp", false);
		Util.waitUntil(1000, ()->!UPnP.isMappedTCP(listener.getPort()));
	}
	
	@Test @Ignore // TODO: This race condition persists and causes an ITF. Fix is non-trivial, and I am already planning to redo socket management approach anyway.
	public void testIsImmuneToPortRebindRaceCondition() throws IOException {
		int port = TEST_PORT;
		
		for(int i = 0; i < 3; i++) {
			ZKMaster master2 = ZKMaster.openBlankTestVolume();
			new Thread(()->master2.getGlobalConfig().set("net.swarm.enabled", true)).start();
			new Thread(()->master2.getGlobalConfig().set("net.swarm.port", port)).start();
			// TODO: itf here, 7/14/20, 4663ff8, linux -- increased timeouts from 250ms to 1000ms on next 3 lines
			assertTrue (Util.waitUntil(1000, ()->master2.getTCPListener().getPort() == port));
			assertTrue (Util.waitUntil(1000, ()->master2.getTCPListener().listenSocket.getLocalPort() == port));
			assertFalse(Util.waitUntil(1000, ()->master2.getTCPListener().listenSocket.getLocalPort() != port));
			master2.getTCPListener().close();
			master2.close();
		}
	}
}
