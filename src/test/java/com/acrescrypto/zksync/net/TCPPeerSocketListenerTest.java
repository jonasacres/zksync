package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.net.ConnectException;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerSocketListenerTest {
	class DummySwarm extends PeerSwarm {
		TCPPeerAdvertisement announced;
		PeerConnection opened;
		
		public DummySwarm(ZKArchiveConfig config) throws IOException { super(config); }
		@Override public void advertiseSelf(PeerAdvertisement ad) { announced = (TCPPeerAdvertisement) ad; }
		@Override public void openedConnection(PeerConnection connection) { opened = connection; }
	}
	
	public final static int TEST_PORT = 44583; // chosen randomly in hopes of not stepping on active local ports on test system
	
	static CryptoSupport crypto;
	static ZKMaster master;
	static ZKArchive archive;
	
	TCPPeerSocketListener listener;
	DummySwarm swarm;
	PrivateDHKey peerKey;
	
	protected Socket connect() throws UnknownHostException, IOException {
		return connect(listener);
	}
	
	protected Socket connect(TCPPeerSocketListener theListener) throws UnknownHostException, IOException {
		Util.waitUntil(100, ()->theListener.listenSocket != null);
		assertNotNull(theListener.listenSocket);
		return new Socket("localhost", theListener.port);
	}
	
	protected void assertSocketClosed(Socket socket) {
		class Holder { boolean closed; }
		Holder holder = new Holder();
		
		Thread thread = new Thread(()-> {
			try {
				IOUtils.readFully(socket.getInputStream(), new byte[1]);
			} catch (IOException e) {
				holder.closed = true;
			}
		});
		
		thread.start();
		try {
			thread.join(100);
		} catch (InterruptedException exc) {}
		assertTrue(holder.closed);
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
	
	protected void sendHandshake(PublicDHKey adKey, Socket socket, int timeSliceOffset, ZKArchiveConfig proofConfig) throws IOException {
		int timeSlice = swarm.config.getAccessor().timeSliceIndex() + timeSliceOffset;
		
		ByteBuffer keyHashInput = ByteBuffer.allocate(2*crypto.asymPublicSigningKeySize());
		keyHashInput.put(peerKey.publicKey().getBytes());
		keyHashInput.put(adKey.getBytes());
		Key keyHashKey = swarm.config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_SEED);
		
		byte[] keyHash = keyHashKey.authenticate(keyHashInput.array());
		byte[] proof;
		if(proofConfig != null) proof = proofConfig.getAccessor().temporalProof(timeSlice, 0, peerKey.sharedSecret(adKey));
		else proof = crypto.rng(crypto.symKeyLength());
		
		OutputStream out = socket.getOutputStream();
		out.write(peerKey.publicKey().getBytes());
		out.write(keyHash);
		out.write(ByteBuffer.allocate(4).putInt(timeSlice).array());
		out.write(proof);
	}
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		crypto = new CryptoSupport();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
	}
	
	@Before
	public void beforeEach() throws IOException, InvalidBlacklistException {
		master.getBlacklist().clear();
		listener = new TCPPeerSocketListener(master, 0);
		swarm = new DummySwarm(archive.getConfig());
		peerKey = crypto.makePrivateDHKey();
	}
	
	@After
	public void afterEach() throws IOException {
		listener.close();
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
	}
	
	@Test
	public void testListensOnRandomPortWhenZeroSpecified() throws IOException {
		TCPPeerSocketListener listener2 = new TCPPeerSocketListener(master, 0);
		Util.waitUntil(100, ()->listener.listenSocket != null);
		Util.waitUntil(100, ()->listener2.listenSocket != null);
		assertNotEquals(listener.port, listener2.port);
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
	}
	
	@Test
	public void testStopsListeningWhenCloseCalled() throws UnknownHostException, IOException {
		connect();
		listener.close();
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
	public void testListenerForSwarmReturnsListenerIfSwarmAdvertised() {
		listener.advertise(swarm);
		TCPPeerAdvertisementListener adListener = listener.listenerForSwarm(swarm);
		assertNotNull(adListener);
		assertEquals(listener.getPort(), adListener.port);
		assertEquals(swarm, adListener.swarm);
	}
	
	@Test
	public void testDisconnectsBlacklistedPeers() throws IOException {
		listener.advertise(swarm);
		master.getBlacklist().add("127.0.0.1", 60000);
		Socket socket = connect();
		assertSocketClosed(socket);
	}
	
	@Test
	public void testDisconnectsPeersBeforeAdvertisedSwarms() throws UnknownHostException, IOException {
		Socket socket = connect();
		assertSocketClosed(socket);
	}
	
	@Test
	public void testDisconnectsPeersSendingInvalidKeyHash() throws UnknownHostException, IOException {
		listener.advertise(swarm);
		Socket socket = connect();
		sendHandshake(crypto.makePrivateDHKey().publicKey(), socket, 0, swarm.config);
		assertSocketClosed(socket);
	}
	
	@Test
	public void testDisconnectsPeersSendingBackdatedTimeDiffs() throws IOException, UnconnectableAdvertisementException {
		master.setCurrentTime(1000l*1000l*1000l*24l*10000l);
		listener.advertise(swarm);
		Socket socket = connect();
		sendHandshake(listener.listenerForSwarm(swarm).localAd().pubKey, socket, -1, swarm.config);
		assertSocketClosed(socket);
	}
	
	@Test
	public void testDisconnectsPeersSendingPostdatedTimeDiffs() throws IOException, UnconnectableAdvertisementException {
		master.setCurrentTime(1000l*1000l*1000l*24l*10000l);
		listener.advertise(swarm);
		Socket socket = connect();
		sendHandshake(listener.listenerForSwarm(swarm).localAd().pubKey, socket, 1, swarm.config);
		assertSocketClosed(socket);
	}
	
	@Test
	public void testToleratesPeersSendingBackdatedTimeDiffsWithinGrace() throws IOException, UnconnectableAdvertisementException {
		ArchiveAccessor accessor = archive.getConfig().getAccessor();
		master.setCurrentTime((accessor.timeSlice(10000) + 10)*1000l*1000l);
		
		listener.advertise(swarm);
		Socket socket = connect();
		sendHandshake(listener.listenerForSwarm(swarm).localAd().pubKey, socket, -1, swarm.config);
		readData(socket, 1);
	}
	
	@Test
	public void testToleratesPeersSendingPostdatedTimeDiffsWithinGrace() throws IOException, UnconnectableAdvertisementException {
		ArchiveAccessor accessor = archive.getConfig().getAccessor();
		master.setCurrentTime((accessor.timeSlice(10001) - 10)*1000l*1000l);
		
		listener.advertise(swarm);
		Socket socket = connect();
		sendHandshake(listener.listenerForSwarm(swarm).localAd().pubKey, socket, 1, swarm.config);
		readData(socket, 1);
	}

	@Test
	public void testMarksPeersBlindIfWrongProofSent() throws UnknownHostException, IOException, UnconnectableAdvertisementException {
		listener.advertise(swarm);
		Socket socket = connect();
		sendHandshake(listener.listenerForSwarm(swarm).localAd().pubKey, socket, 0, null);
		Util.waitUntil(100, ()->swarm.opened != null);
		assertEquals(PeerConnection.PEER_TYPE_BLIND, swarm.opened.peerType);
	}
	
	@Test
	public void testMarksPeersBlindIfOwnArchiveIsSeedOnly() throws UnknownHostException, IOException, UnconnectableAdvertisementException {
		ArchiveAccessor roAccessor = archive.getConfig().getAccessor().makeSeedOnly();
		ZKArchiveConfig roConfig = new ZKArchiveConfig(roAccessor, archive.getConfig().getArchiveId());
		DummySwarm roSwarm = new DummySwarm(roConfig);
		
		listener.advertise(roSwarm);
		Socket socket = connect();
		sendHandshake(listener.listenerForSwarm(roSwarm).localAd().pubKey, socket, 0, swarm.config);
		Util.waitUntil(100, ()->swarm.opened != null);
		assertEquals(PeerConnection.PEER_TYPE_BLIND, roSwarm.opened.peerType);
	}
	
	@Test
	public void testMarksPeersFullIfCorrectProofAndArchiveHasReadAccess() throws IOException, UnconnectableAdvertisementException {
		listener.advertise(swarm);
		Socket socket = connect();
		sendHandshake(listener.listenerForSwarm(swarm).localAd().pubKey, socket, 0, swarm.config);
		Util.waitUntil(100, ()->swarm.opened != null);
		assertEquals(PeerConnection.PEER_TYPE_FULL, swarm.opened.peerType);
	}
	
	@Test
	public void testSendsEphemeralKeyAndAuthHash() throws UnknownHostException, IOException, UnconnectableAdvertisementException {
		listener.advertise(swarm);
		Socket socket = connect();
		sendHandshake(listener.listenerForSwarm(swarm).localAd().pubKey, socket, 0, swarm.config);
		Util.waitUntil(100, ()->swarm.opened != null);
		
		PublicDHKey ephemeral = new PublicDHKey(readData(socket, crypto.asymPrivateDHKeySize()));
		byte[] authHash = readData(socket, crypto.hashLength());
		byte[] tempSecret = peerKey.sharedSecret(listener.listenerForSwarm(swarm).localAd().pubKey);
		byte[] secret = peerKey.sharedSecret(ephemeral);
		byte[] expectedAuthHash = crypto.authenticate(secret, tempSecret);
		
		assertTrue(Arrays.equals(expectedAuthHash, authHash));
	}
	
	@Test
	public void testSendsBogusProofWhenClientProofInvalid() throws UnconnectableAdvertisementException, IOException {
		int timeIndex = swarm.config.getAccessor().timeSliceIndex();
		listener.advertise(swarm);
		Socket socket = connect();
		sendHandshake(listener.listenerForSwarm(swarm).localAd().pubKey, socket, 0, null);
		Util.waitUntil(100, ()->swarm.opened != null);
		
		PublicDHKey ephemeral = new PublicDHKey(readData(socket, crypto.asymPrivateDHKeySize()));
		readData(socket, crypto.hashLength());
		byte[] secret = peerKey.sharedSecret(ephemeral);
		
		byte[] responseProof = readData(socket, crypto.symKeyLength());
		byte[] expectedResponseProof = swarm.config.getAccessor().temporalProof(timeIndex, 1, secret);
		assertFalse(Arrays.equals(expectedResponseProof, responseProof));
	}
	
	@Test
	public void testSendsBogusProofWhenArchiveIsSeedOnly() throws UnconnectableAdvertisementException, IOException {
		int timeIndex = swarm.config.getAccessor().timeSliceIndex();
		ArchiveAccessor roAccessor = archive.getConfig().getAccessor().makeSeedOnly();
		ZKArchiveConfig roConfig = new ZKArchiveConfig(roAccessor, archive.getConfig().getArchiveId());
		DummySwarm roSwarm = new DummySwarm(roConfig);
		
		listener.advertise(roSwarm);
		Socket socket = connect();
		sendHandshake(listener.listenerForSwarm(roSwarm).localAd().pubKey, socket, 0, swarm.config);
		Util.waitUntil(100, ()->roSwarm.opened != null);
		
		PublicDHKey ephemeral = new PublicDHKey(readData(socket, crypto.asymPrivateDHKeySize()));
		readData(socket, crypto.hashLength());
		byte[] secret = peerKey.sharedSecret(ephemeral);
		
		byte[] responseProof = readData(socket, crypto.symKeyLength());
		byte[] expectedResponseProof = swarm.config.getAccessor().temporalProof(timeIndex, 1, secret);
		assertFalse(Arrays.equals(expectedResponseProof, responseProof));
	}
	
	@Test
	public void testSendsValidProofWhenClientProofValidAndArchiveHasReadAccess() throws UnconnectableAdvertisementException, IOException {
		int timeIndex = swarm.config.getAccessor().timeSliceIndex();
		listener.advertise(swarm);
		Socket socket = connect();
		sendHandshake(listener.listenerForSwarm(swarm).localAd().pubKey, socket, 0, swarm.config);
		Util.waitUntil(100, ()->swarm.opened != null);
		
		PublicDHKey ephemeral = new PublicDHKey(readData(socket, crypto.asymPrivateDHKeySize()));
		readData(socket, crypto.hashLength());
		byte[] secret = peerKey.sharedSecret(ephemeral);
		
		byte[] responseProof = readData(socket, crypto.symKeyLength());
		byte[] expectedResponseProof = swarm.config.getAccessor().temporalProof(timeIndex, 1, secret);
		assertTrue(Arrays.equals(expectedResponseProof, responseProof));
	}
}
