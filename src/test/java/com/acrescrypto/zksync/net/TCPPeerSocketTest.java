package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerSocketTest {
	interface ServerSocketCallback {
		void callback(Socket serverSocket, TCPPeerSocket clientSocket, byte[] secret) throws Exception;
	}
	
	class DummySwarm extends PeerSwarm {
		PeerConnection opened;
		
		public DummySwarm(ZKArchiveConfig config) throws IOException {
			super(config);
		}
		
		@Override public void openedConnection(PeerConnection connection) {
			opened = connection;
		}
	}
	
	class DummyServer {
		ServerSocket server;
		LinkedList<Socket> clients = new LinkedList<Socket>();
		
		public DummyServer() throws IOException {
			server = new ServerSocket(0);
			new Thread(() -> runThread()).start();
		}
		
		void runThread() {
			try {
				while(!server.isClosed()) {
					Socket client = server.accept();
					synchronized(this) {
						clients.add(client);
						this.notifyAll();
					}
				}
			} catch(IOException exc) {
			}
		}
		
		synchronized Socket getClient(int index) {
			if(clients.size() <= index) {
				try { this.wait(500); } catch(InterruptedException exc) {}
				if(clients.size() <= index) fail();
			}
			
			return clients.get(index);
		}
		
		synchronized Socket getClientWithPort(int port) {
			for(int i = 0; i < 2; i++) {
				for(Socket client : clients) {
					if(client.getPort() == port) return client;
				}
				
				if(i == 0) try { this.wait(500); } catch(InterruptedException exc) {}
			}
			
			fail();
			return null; // unreachable; makes compiler happy
		}
		
		int getPort() {
			return server.getLocalPort();
		}
		
		void close() throws IOException {
			server.close();
		}
	}
	
	class DummyConnection {
		TCPPeerSocket client;
		Socket serverSock;
		boolean handshakeFinished;
		Exception handshakeException;
		PrivateDHKey serverEphKey;
		ZKArchive peerArchive;
		Key writeChainKey, readChainKey;
		byte[] secret;
		
		DummyConnection(TCPPeerSocket client) {
			this(archive, client);
		}
		
		DummyConnection(ZKArchive peerArchive, TCPPeerSocket client) {
			this.client = client;
			this.peerArchive = peerArchive;
			this.serverEphKey = crypto.makePrivateDHKey();
		}
		
		DummyConnection(Socket serverSock, TCPPeerSocket client, byte[] secret) {
			this.serverSock = serverSock;
			this.client = client;
			this.secret = secret;
			initKeys(true);
		}
		
		byte[] serverReadNext() throws IOException {
			int msgLen = ByteBuffer.wrap(serverReadRaw(4)).getInt();
			byte[] msgIv = new byte[crypto.symIvLength()];
			byte[] ciphertext = serverReadRaw(msgLen);
			return nextReadKey().decrypt(msgIv, ciphertext);
		}
		
		void serverWrite(byte[] data) throws IOException {
			byte[] msgIv = new byte[crypto.symIvLength()];
			byte[] ciphertext = nextWriteKey().encrypt(msgIv, data, 0);
			serverWriteRaw(ByteBuffer.allocate(4).putInt(ciphertext.length).array());
			serverWriteRaw(ciphertext);
		}
		
		byte[] serverReadRaw(int len) throws IOException {
			return readBytes(serverSock, len);
		}
		
		void serverWriteRaw(byte[] data) throws IOException {
			writeBytes(serverSock, data);
		}
		
		Key nextReadKey() {
			Key newChainKey = readChainKey.derive(0, new byte[0]);
			Key msgKey = readChainKey.derive(1, new byte[0]);
			readChainKey = newChainKey;
			return msgKey;
		}
		
		Key nextWriteKey() {
			Key newChainKey = writeChainKey.derive(0, new byte[0]);
			Key msgKey = writeChainKey.derive(1, new byte[0]);
			writeChainKey = newChainKey;
			return msgKey;
		}
		
		DummyConnection handshake() throws IOException {
			return handshake(true, true);
		}
		
		DummyConnection handshake(boolean sendValidAuth, boolean sendValidProof) throws IOException {
			new Thread(()->{
				try {
					client.handshake();
				} catch (ProtocolViolationException | IOException exc) {
					handshakeException = exc;
				} finally {
					handshakeFinished = true;
					synchronized(this) {
						this.notifyAll();
					}
				}
			}).start();
			
			while(client.socket == null) { try { Thread.sleep(1); } catch(InterruptedException exc) {} }
			this.serverSock = server.getClientWithPort(client.socket.getLocalPort());
			PublicDHKey pubKey = crypto.makePublicDHKey(serverReadRaw(crypto.asymPublicDHKeySize()));
			serverReadRaw(crypto.hashLength()); // key auth
			int timeIndex = ByteBuffer.wrap(serverReadRaw(4)).getInt();
			serverReadRaw(crypto.symKeyLength()); // proof
			
			this.secret = serverEphKey.sharedSecret(pubKey);
			byte[] tempSecret = serverKey.sharedSecret(pubKey);
			byte[] auth = crypto.authenticate(secret, tempSecret);
			byte[] proof = peerArchive.getConfig().getAccessor().temporalProof(timeIndex, 1, secret);
			
			initKeys(false);
			
			serverWriteRaw(serverEphKey.publicKey().getBytes());
			serverWriteRaw(sendValidAuth ? auth : crypto.rng(auth.length));
			serverWriteRaw(sendValidProof ? proof : crypto.rng(proof.length));
			synchronized(this) {
				try { this.wait(); } catch (InterruptedException e) {}
			}
			
			return this;
		}
		
		void initKeys(boolean localRoleIsClient) {
			readChainKey = new Key(crypto, crypto.expand(secret, crypto.symKeyLength(), new byte[] { (byte) (localRoleIsClient ? 1 : 0) }, "zksync".getBytes()));
			writeChainKey = new Key(crypto, crypto.expand(secret, crypto.symKeyLength(), new byte[] { (byte) (localRoleIsClient ? 0 : 1) }, "zksync".getBytes()));
		}
	}
	
	static CryptoSupport crypto;
	static ZKMaster master;
	static ZKArchive archive;

	PrivateDHKey serverKey;
	DummySwarm swarm;
	TCPPeerSocket socket;
	TCPPeerAdvertisement ad;
	DummyServer server;
	
	void blindHandshake(TCPPeerSocket socket) {
		new Thread(()-> {
			try {
				socket.handshake();
			} catch (ProtocolViolationException | IOException e) {
			}
		}).start();
	}
	
	byte[] readBytes(Socket socket, int len) throws IOException {
		byte[] buf = new byte[len];
		IOUtils.read(socket.getInputStream(), buf);
		return buf;
	}
	
	byte[] readBytes(int len) throws IOException {
		return readBytes(server.getClient(0), len);
	}
	
	void writeBytes(byte[] bytes) throws IOException {
		writeBytes(server.getClient(0), bytes);
	}
	
	void writeBytes(Socket socket, byte[] bytes) throws IOException {
		socket.getOutputStream().write(bytes);
	}
	
	void setupServerSocket(ServerSocketCallback callback) throws Exception {
		ServerSocket server = null;
		try {
			byte[] secret = master.getCrypto().rng(master.getCrypto().asymDHSecretSize());
			server = new ServerSocket(0);
			Socket clientSocketRaw = new Socket("localhost", server.getLocalPort());
			TCPPeerSocket clientSocket = new TCPPeerSocket(swarm, clientSocketRaw, secret, PeerConnection.PEER_TYPE_BLIND);
			Socket serverSocket = server.accept();
			assertEquals(clientSocketRaw, clientSocket.socket);
			assertTrue(Arrays.equals(secret, clientSocket.getSharedSecret()));
			callback.callback(serverSocket, clientSocket, secret);
		} catch(Exception exc) {
			throw(exc);
		} finally {
			if(server != null) server.close();
		}
	}
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		crypto = master.getCrypto();
	}
	
	@Before
	public void beforeEach() throws IOException, ProtocolViolationException, BlacklistedException, UnconnectableAdvertisementException {
		swarm = new DummySwarm(archive.getConfig());
		server = new DummyServer();
		serverKey = master.getCrypto().makePrivateDHKey();
		ad = new TCPPeerAdvertisement(serverKey.publicKey(), "localhost", server.getPort());
		socket = new TCPPeerSocket(swarm, ad);
	}
	
	@After
	public void afterEach() throws IOException {
		TCPPeerSocket.maxHandshakeTimeMillis = TCPPeerSocket.DEFAULT_MAX_HANDSHAKE_TIME_MILLIS;
		master.getBlacklist().clear();
		server.close();
	}

	@Test
	public void testInitializeWithClientSocket() throws Exception {
		setupServerSocket((serverSocket, clientSocket, secret)->{
			assertEquals(clientSocket, swarm.opened.socket);
			DummyConnection conn = new DummyConnection(serverSocket, clientSocket, secret);
			byte[] buf = new byte[5], data = "hello".getBytes();
			
			conn.serverWrite(data);
			clientSocket.read(buf, 0, buf.length);
			assertTrue(Arrays.equals(data, buf));
			
			byte[] response = "hola".getBytes();
			clientSocket.write(response, 0, response.length);
			byte[] received = conn.serverReadNext();
			assertTrue(Arrays.equals(response, received));
		});
	}
	
	@Test
	public void testUsesEphemeralKeys() throws IOException, ProtocolViolationException, BlacklistedException, UnconnectableAdvertisementException {
		TCPPeerSocket socket2 = new TCPPeerSocket(swarm, ad);

		byte[] pubKeyBufA = new byte[crypto.asymPublicDHKeySize()];
		byte[] pubKeyBufB = new byte[crypto.asymPublicDHKeySize()];
		
		blindHandshake(socket);
		blindHandshake(socket2);
		
		IOUtils.read(server.getClient(0).getInputStream(), pubKeyBufA);
		IOUtils.read(server.getClient(1).getInputStream(), pubKeyBufB);
		
		assertFalse(Arrays.equals(pubKeyBufA, pubKeyBufB));
	}
	
	@Test
	public void testHandshakeSendsInfo() throws IOException {
		blindHandshake(socket);
		
		byte[] pubKey = readBytes(crypto.asymPublicDHKeySize());
		byte[] keyHash = readBytes(crypto.hashLength());
		int timeIndex = ByteBuffer.wrap(readBytes(server.getClient(0), 4)).getInt();
		
		Key keyHashKey = swarm.config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_SEED);
		ByteBuffer keyHashInput = ByteBuffer.allocate(2*crypto.asymPublicDHKeySize());
		keyHashInput.put(pubKey);
		keyHashInput.put(ad.pubKey.getBytes());
		byte[] expectedKeyHash = keyHashKey.authenticate(keyHashInput.array());
		
		assertTrue(Arrays.equals(socket.dhPrivateKey.publicKey().getBytes(), pubKey));
		assertTrue(Arrays.equals(expectedKeyHash, keyHash));
		assertEquals(swarm.config.getAccessor().timeSliceIndex(), timeIndex);
	}
	
	@Test
	public void testHandshakeSendsRealProofIfArchiveHasReadAccess() throws IOException {
		blindHandshake(socket);
		PublicDHKey clientKey = crypto.makePublicDHKey(readBytes(crypto.asymPublicDHKeySize()));
		readBytes(crypto.hashLength());
		int timeIndex = ByteBuffer.wrap(readBytes(4)).getInt();
		byte[] proof = readBytes(crypto.symKeyLength());
		
		byte[] secret = serverKey.sharedSecret(clientKey);
		byte[] expectedProof = swarm.config.getAccessor().temporalProof(timeIndex, 0, secret);
		
		assertTrue(Arrays.equals(expectedProof, proof));
	}
	
	@Test
	public void testHandshakeSendsGarbageProofIfArchiveIsSeedOnly() throws IOException, BlacklistedException, ProtocolViolationException {
		// this test only works if the proof is the last thing the client sends in the handshake.
		// we're testing that the client sends SOMETHING, even if it does not possess the correct proof.
		// the check against the expected hash validates that the test was configured correctly.
		
		ArchiveAccessor roAccessor = archive.getConfig().getAccessor().makeSeedOnly();
		ZKArchiveConfig roConfig = new ZKArchiveConfig(roAccessor, archive.getConfig().getArchiveId());
		DummySwarm roSwarm = new DummySwarm(roConfig);
		TCPPeerSocket roSocket = new TCPPeerSocket(roSwarm, ad);
		blindHandshake(roSocket);
		
		PublicDHKey clientKey = crypto.makePublicDHKey(readBytes(crypto.asymPublicDHKeySize()));
		byte[] keyHash = readBytes(crypto.hashLength());
		int timeIndex = ByteBuffer.wrap(readBytes(4)).getInt();
		byte[] proof = readBytes(crypto.symKeyLength());
		
		Key keyHashKey = swarm.config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_SEED);
		ByteBuffer keyHashInput = ByteBuffer.allocate(2*crypto.asymPublicDHKeySize());
		keyHashInput.put(clientKey.getBytes());
		keyHashInput.put(ad.pubKey.getBytes());

		byte[] expectedKeyHash = keyHashKey.authenticate(keyHashInput.array());
		byte[] secret = serverKey.sharedSecret(clientKey);
		byte[] expectedProof = swarm.config.getAccessor().temporalProof(timeIndex, 0, secret);

		assertTrue(Arrays.equals(roSocket.dhPrivateKey.publicKey().getBytes(), clientKey.getBytes()));
		assertTrue(Arrays.equals(expectedKeyHash, keyHash));
		assertFalse(Arrays.equals(expectedProof, proof));		
	}
	
	@Test
	public void testHandshakeThrowsExceptionIfAuthHashIsInvalid() throws IOException {
		assertNotNull(new DummyConnection(socket).handshake(false, true).handshakeException);
	}
	
	@Test
	public void testHandshakeBlacklistsPeerIfAuthHashIsInvalid() throws IOException {
		assertFalse(master.getBlacklist().contains(ad.host));
		new DummyConnection(socket).handshake(false, true);
		assertTrue(master.getBlacklist().contains(socket.socket.getInetAddress().getHostAddress()));
	}
	
	@Test
	public void testHandshakeDoesntBlacklistsPeerIfAuthHashAndProofAreValid() throws IOException {
		assertFalse(master.getBlacklist().contains(ad.host));
		new DummyConnection(socket).handshake(true, true);
		assertFalse(master.getBlacklist().contains(socket.socket.getInetAddress().getHostAddress()));
	}

	@Test
	public void testHandshakeDoesntBlacklistsPeerIfAuthHashIsValidAndProofIsInvalid() throws IOException {
		assertFalse(master.getBlacklist().contains(ad.host));
		new DummyConnection(socket).handshake(true, false);
		assertFalse(master.getBlacklist().contains(socket.socket.getInetAddress().getHostAddress()));
	}
	
	@Test
	public void testHandshakeSetsPeerTypeToSeedOnlyIfArchiveHasSeedOnlyAccess() throws IOException, BlacklistedException {
		ArchiveAccessor roAccessor = archive.getConfig().getAccessor().makeSeedOnly();
		ZKArchiveConfig roConfig = new ZKArchiveConfig(roAccessor, archive.getConfig().getArchiveId());
		DummySwarm roSwarm = new DummySwarm(roConfig);
		TCPPeerSocket roSocket = new TCPPeerSocket(roSwarm, ad);
		assertNotEquals(PeerConnection.PEER_TYPE_BLIND, roSocket.getPeerType());
		new DummyConnection(roSocket).handshake(true, true);
		assertEquals(PeerConnection.PEER_TYPE_BLIND, roSocket.getPeerType());
	}
	
	@Test
	public void testHandshakeSetsPeerTypeToSeedOnlyIfServerSendsGarbageProof() throws IOException {
		assertNotEquals(PeerConnection.PEER_TYPE_BLIND, socket.getPeerType());
		new DummyConnection(socket).handshake(true, false);
		assertEquals(PeerConnection.PEER_TYPE_BLIND, socket.getPeerType());
	}
	
	@Test
	public void testHandshakeSetsPeerTypeToFullIfServerSendsValidProofAndArchiveHasReadAccess() throws IOException {
		assertNotEquals(PeerConnection.PEER_TYPE_FULL, socket.getPeerType());
		new DummyConnection(socket).handshake(true, true);
		assertEquals(PeerConnection.PEER_TYPE_FULL, socket.getPeerType());
	}
	
	@Test
	public void testHandshakeClosesSocketIfPeerDoesNotCompleteInTime() {
		TCPPeerSocket.maxHandshakeTimeMillis = 10;
		blindHandshake(socket);
		assertFalse(Util.waitUntil(TCPPeerSocket.maxHandshakeTimeMillis-1, ()->socket.isClosed()));
		assertTrue(Util.waitUntil(5, ()->socket.isClosed()));
	}
	
	@Test
	public void testWriteTransmitsWellFormattedEncryptedMessages() throws IOException, ProtocolViolationException {
		byte[] data = "hello world".getBytes();
		DummyConnection dummy = new DummyConnection(socket).handshake();
		socket.write(data, 0, data.length);
		byte[] read = dummy.serverReadNext();
		
		assertTrue(Arrays.equals(data, read));
	}
	
	@Test
	public void testWriteHonorsBufferOffset() throws IOException, ProtocolViolationException {
		byte[] data = "hello world".getBytes();
		int offset = 6;
		
		DummyConnection dummy = new DummyConnection(socket).handshake();
		socket.write(data, offset, data.length-offset);
		byte[] read = dummy.serverReadNext();
		
		assertTrue(Arrays.equals("world".getBytes(), read));
	}
	
	@Test
	public void testWriteHonorsBufferLength() throws IOException, ProtocolViolationException {
		byte[] data = "hello world".getBytes();
		int length = 5;
		
		DummyConnection dummy = new DummyConnection(socket).handshake();
		socket.write(data, 0, length);
		byte[] read = dummy.serverReadNext();
		
		assertTrue(Arrays.equals("hello".getBytes(), read));
	}
	
	@Test
	public void testWriteUpdatesKeyWithSuccessiveMessages() throws IOException {
		byte[] data = "hello world".getBytes();
		DummyConnection dummy = new DummyConnection(socket).handshake();
		
		int len = -1;
		byte[] lastCiphertext = null;
		
		for(int i = 0; i < 10; i++) {
			socket.write(data, 0, data.length);
			int msgLen = ByteBuffer.wrap(dummy.serverReadRaw(4)).getInt();
			if(len == -1) len = msgLen;
			assertEquals(msgLen, len); // sanity check to ensure we're not just misaligned
			
			byte[] ciphertext = dummy.serverReadRaw(msgLen);
			if(lastCiphertext != null) assertFalse(Arrays.equals(lastCiphertext, ciphertext));
			lastCiphertext = ciphertext;
		}
	}
	
	@Test
	public void testSuccessiveWritesCanBeDecrypted() throws IOException {
		byte[] data = "hello world".getBytes();
		DummyConnection dummy = new DummyConnection(socket).handshake();
		
		for(int i = 0; i < 10; i++) {
			socket.write(data, 0, data.length);
			assertTrue(Arrays.equals(data, dummy.serverReadNext()));
		}
	}
	
	@Test
	public void testReadDecryptsEncryptedMessages() throws IOException, ProtocolViolationException {
		byte[] data = "you must gather your party before venturing forth".getBytes();
		byte[] buf = new byte[data.length];
		DummyConnection dummy = new DummyConnection(socket).handshake();
		
		dummy.serverWrite(data);
		socket.read(buf, 0, buf.length);
		assertTrue(Arrays.equals(data, buf));
	}
	
	@Test
	public void testReadHonorsBufferOffset() throws IOException, ProtocolViolationException {
		byte[] data = "one two".getBytes();
		byte[] buf = "one two".getBytes();
		
		DummyConnection dummy = new DummyConnection(socket).handshake();
		dummy.serverWrite(data);
		socket.read(buf, 4, 3);
		assertTrue(Arrays.equals("one one".getBytes(), buf));
	}
	
	@Test
	public void testReadHonorsBufferLength() throws IOException, ProtocolViolationException {
		byte[] data = "abcdef".getBytes();
		byte[] buf = "ghijkl".getBytes();
		
		DummyConnection dummy = new DummyConnection(socket).handshake();
		dummy.serverWrite(data);
		socket.read(buf, 0, 3);
		assertTrue(Arrays.equals("abcjkl".getBytes(), buf));
	}
	
	@Test(expected = ProtocolViolationException.class)
	public void testReadThrowsExceptionIfMessageLengthIsNegative() throws IOException, ProtocolViolationException {
		byte[] buf = new byte[1];
		DummyConnection dummy = new DummyConnection(socket).handshake();
		dummy.serverWriteRaw(ByteBuffer.allocate(4).putInt(-1).array());
		socket.read(buf, 0, 1);
	}
	
	@Test(expected = ProtocolViolationException.class)
	public void testReadThrowsExceptionIfMessageLengthIsZero() throws IOException, ProtocolViolationException {
		byte[] buf = new byte[1];
		DummyConnection dummy = new DummyConnection(socket).handshake();
		dummy.serverWriteRaw(ByteBuffer.allocate(4).putInt(0).array());
		socket.read(buf, 0, 1);
	}
	
	@Test(expected = ProtocolViolationException.class)
	public void testReadThrowsExceptionIfMessageLengthExceedsLimit() throws IOException, ProtocolViolationException {
		int limit = TCPPeerSocket.MAX_MSG_LEN + 2*crypto.symBlockSize();
		byte[] buf = new byte[1];
		DummyConnection dummy = new DummyConnection(socket).handshake();
		dummy.serverWriteRaw(ByteBuffer.allocate(4).putInt(limit+1).array());
		socket.read(buf, 0, 1);
	}
	
	@Test
	public void testReadDoesNotThrowExceptionIfMessageLengthIsWithinLimit() throws IOException, ProtocolViolationException {
		int limit = TCPPeerSocket.MAX_MSG_LEN;
		byte[] buf = new byte[limit];
		DummyConnection dummy = new DummyConnection(socket).handshake();
		dummy.serverWrite(new byte[limit]);
		socket.read(buf, 0, limit);
	}
	
	@Test(expected = ProtocolViolationException.class)
	public void testReadThrowsExceptionIfEofReceivedInMessage() throws IOException, ProtocolViolationException {
		int limit = TCPPeerSocket.MAX_MSG_LEN;
		byte[] buf = new byte[limit];
		DummyConnection dummy = new DummyConnection(socket).handshake();
		dummy.serverWriteRaw(ByteBuffer.allocate(4).putInt(8).array());
		dummy.serverWriteRaw(new byte[7]);
		dummy.serverSock.close();
		socket.read(buf, 0, limit);
	}
	
	@Test
	public void testReadAllowsPartialReadOfMessages() throws IOException, ProtocolViolationException {
		byte[] data = "one two".getBytes();
		byte[] buf0 = new byte[3], buf1 = new byte[1], buf2 = new byte[data.length-buf0.length-buf1.length], buf3 = new byte[data.length];
		
		DummyConnection dummy = new DummyConnection(socket).handshake();
		dummy.serverWrite(data);
		
		socket.read(buf0, 0, buf0.length);
		socket.read(buf1, 0, buf1.length);
		socket.read(buf2, 0, buf2.length);
		
		assertTrue(Arrays.equals("one".getBytes(), buf0));
		assertTrue(Arrays.equals(" ".getBytes(), buf1));
		assertTrue(Arrays.equals("two".getBytes(), buf2));
		
		// test that we can do more reads afterwards
		dummy.serverWrite(data);
		socket.read(buf3, 0, buf3.length);
		assertTrue(Arrays.equals(data, buf3));
	}
	
	@Test
	public void testIsClientReturnsFalseIfConstructedWithSharedSecret() throws Exception {
		setupServerSocket((serverSocket, clientSocket, secret)->{
			assertFalse(clientSocket.isLocalRoleClient());
		});
	}
	
	@Test
	public void testIsClientReturnsTrueIfConstructedWithAdvertisement() {
		assertTrue(socket.isLocalRoleClient());
	}
	
	@Test
	public void testCloseClosesTheSocket() throws IOException {
		new DummyConnection(socket).handshake();
		assertFalse(socket.socket.isClosed());
		socket.close();
		assertTrue(socket.socket.isClosed());
	}
	
	@Test
	public void testIsClosedReturnsTrueIfSocketClosed() throws IOException {
		new DummyConnection(socket).handshake();
		assertFalse(socket.isClosed());
		socket.close();
		assertTrue(socket.isClosed());
	}

	@Test
	public void testIsClosedReturnsFalseIfSocketNotClosed() throws IOException {
		new DummyConnection(socket).handshake();
		assertFalse(socket.isClosed());
	}

	@Test
	public void testGetSharedSecretReturnsSharedSecret() throws IOException {
		DummyConnection dummy = new DummyConnection(socket).handshake();
		assertTrue(Arrays.equals(dummy.secret, socket.sharedSecret));
	}
	
	@Test
	public void testGetAdReturnsAd() {
		assertEquals(ad, socket.getAd());
	}
	
	@Test
	public void testMatchesAddressReturnsTrueIfAddressIsIPOfSocket() throws IOException {
		DummyConnection dummy = new DummyConnection(socket).handshake();
		assertTrue(socket.matchesAddress(dummy.serverSock.getInetAddress().getHostAddress()));
	}
	
	@Test
	public void testMatchesAddressReturnsTrueIfAddressResolvesToIPOfSocket() throws IOException {
		new DummyConnection(socket).handshake();
		assertTrue(socket.matchesAddress("localhost"));
	}
	
	@Test
	public void testMatchesAddressReturnsFalseIfAddressDoesNotMatchSocket() throws IOException {
		new DummyConnection(socket).handshake();
		assertFalse(socket.matchesAddress("10.1.2.3"));
	}
	
	// TODO P2P: (implement) Test ignores closed message IDs
	// TODO P2P: (implement) Test ignores skipped message IDs
}
