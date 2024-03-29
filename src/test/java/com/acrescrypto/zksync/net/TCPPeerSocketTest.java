package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.io.IOException;
import java.io.StringReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.InfiniteCircularInputStream;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.noise.CipherState;
import com.acrescrypto.zksync.net.noise.SipObfuscator;
import com.acrescrypto.zksync.net.noise.VariableLengthHandshakeState;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerSocketTest {
	interface ServerSocketCallback {
		void callback(Socket serverSocket, TCPPeerSocket clientSocket, byte[] secret, CipherState[] states, SipObfuscator sip) throws Exception;
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
			Util.setThreadName("DummyServer listen thread " + server.getLocalPort());
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
		CipherState readState, writeState;
		SipObfuscator sip;
		
		DummyConnection(TCPPeerSocket client) {
			this(archive, client);
		}
		
		DummyConnection(ZKArchive peerArchive, TCPPeerSocket client) {
			this.client = client;
			this.peerArchive = peerArchive;
			this.serverEphKey = crypto.makePrivateDHKey();
		}
		
		DummyConnection(Socket serverSock, TCPPeerSocket client, CipherState[] states, SipObfuscator sip) {
			this.serverSock = serverSock;
			this.client = client;
			this.readState = states[0];
			this.writeState = states[1];
			this.sip = sip;
		}
		
		byte[] serverReadNext() throws IOException {
			int msgLenObf = ByteBuffer.wrap(serverReadRaw(2)).getShort();
			int msgLen = sip.read().obfuscate2(msgLenObf);
			byte[] ciphertext = serverReadRaw(msgLen);
			return readState.decryptWithAssociatedData(new byte[0], ciphertext);
		}
		
		void serverWrite(byte[] data) throws IOException {
			byte[] ciphertext = writeState.encryptWithAssociatedData(new byte[0], data);
			int msgLenObf = sip.write().obfuscate2(ciphertext.length);
			serverWriteRaw(ByteBuffer.allocate(2).putShort((short) msgLenObf).array());
			serverWriteRaw(ciphertext);
		}
		
		byte[] serverReadRaw(int len) throws IOException {
			return readBytes(serverSock, len);
		}
		
		void serverWriteRaw(byte[] data) throws IOException {
			writeBytes(serverSock, data);
		}
		
		DummyConnection handshake() throws IOException {
			return handshake(true, true);
		}
		
		DummyConnection handshake(boolean sendValidAuth, boolean sendValidProof) throws IOException {

			new Thread(()->{
				Util.setThreadName("TCPPeerSocketTest DummyConnection handshake thread");
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
			
			assertTrue(Util.waitUntil(3000, ()->client.socket != null));
			this.serverSock = server.getClientWithPort(client.socket.getLocalPort());
			
	        VariableLengthHandshakeState handshake = new VariableLengthHandshakeState(
					crypto,
					TCPPeerSocket.HANDSHAKE_PROTOCOL,
					TCPPeerSocket.HANDSHAKE_PATTERN,
					false,
					new byte[0],
					serverKey,
					null,
					null,
					null,
					null
					);
			
			handshake.setDerivationCallback((key)->{
				sip = new SipObfuscator(key.derive(SipObfuscator.SIP_OBFUSCATOR_ASK_NAME).getRaw(), false);
			});

			handshake.setObfuscation(
					(key)->{
						Key sym = new Key(crypto, crypto.makeSymmetricKey(handshake.getRemoteEphemeralKey().getBytes()));
						return sym.encryptUnauthenticated(new byte[crypto.symIvLength()], key.getBytes());
					},
					
					(inn)->{
						Key sym = new Key(crypto, crypto.makeSymmetricKey(serverKey.publicKey().getBytes()));
						byte[] ciphertext = IOUtils.readFully(inn, crypto.asymPublicDHKeySize());
						byte[] keyRaw = sym.decryptUnauthenticated(new byte[crypto.symIvLength()], ciphertext);
						return new byte[][] { keyRaw, ciphertext };
					}
				);
			
			handshake.setSimplePayload(
				(round)->{
					if(round != 4) return null;
					byte[] proof;

					if(sendValidProof) {
						proof = archive.getConfig().getAccessor().temporalProof(0, 1, handshake.getHash());
					} else {
						proof = crypto.rng(crypto.symKeyLength());
					}

					JsonObject json = Json.createObjectBuilder()
							.add("proof", Util.bytesToHex(proof))
							.build();
					
					return json.toString().getBytes();

				},
				
				(round, payload)->{
					if(round != 3) return;
					
					JsonReader reader = Json.createReader(new StringReader(new String(payload)));
					reader.readObject(); // just make sure we got json					
					handshake.setPsk(archive.getConfig().getArchiveId());
				}
			);
			
			CipherState[] states = handshake.handshake(serverSock.getInputStream(), serverSock.getOutputStream());
			assertTrue(Util.waitUntil(3000, ()->client.writeState != null));
			readState = states[0];
			writeState = states[1];
			
			if(!TCPPeerSocket.disableMakeThreads) {
				serverReadNext();
				if(!PeerConnection.DISABLE_TAG_LIST) serverReadNext();
			}
			
			return this;
		}

		public boolean hasAvailable() {
			try {
				return serverSock.getInputStream().available() > 0;
			} catch(IOException exc) {
				exc.printStackTrace();
				return false;
			}
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
			Util.setThreadName("TCPPeerSocketTest blindHandshake thread");
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
		Socket clientSocketRaw = null, serverSocket = null;
		TCPPeerSocket clientSocket = null;
		PublicDHKey identityKey = crypto.makePrivateDHKey().publicKey();
		
		try {
			byte[] secret = master.getCrypto().rng(master.getCrypto().asymDHSecretSize());
			server = new ServerSocket(0);
			CipherState[] states = { new CipherState(), new CipherState() };
			SipObfuscator sip = new SipObfuscator(new byte[0], true);
			clientSocketRaw = new Socket("localhost", server.getLocalPort());
			clientSocket = new TCPPeerSocket(swarm, identityKey, clientSocketRaw, states, sip, secret, PeerConnection.PEER_TYPE_BLIND, 0);
			serverSocket = server.accept();
			assertEquals(clientSocketRaw, clientSocket.socket);
			assertTrue(Arrays.equals(secret, clientSocket.getSharedSecret()));
			callback.callback(serverSocket,
					clientSocket,
					secret,
					new CipherState[] { new CipherState(), new CipherState() },
					new SipObfuscator(new byte[0], false));
		} catch(Exception exc) {
			throw(exc);
		} finally {
			if(server != null) server.close();
			if(clientSocket != null) clientSocket.close();
			if(clientSocketRaw != null) clientSocketRaw.close();
			if(serverSocket != null) serverSocket.close();
		}
	}
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		TestUtils.startDebugMode();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		crypto = master.getCrypto();
	}
	
	@Before
	public void beforeEach() throws IOException, ProtocolViolationException, BlacklistedException, UnconnectableAdvertisementException {
		TCPPeerSocket.maxHandshakeTimeMillis = 400;
		TCPPeerSocket.disableMakeThreads = true;
		swarm = new DummySwarm(archive.getConfig());
		server = new DummyServer();
		serverKey = master.getTCPListener().getIdentityKey();
		byte[] encryptedArchiveId = archive.getConfig().getEncryptedArchiveId(serverKey.publicKey().getBytes());
		ad = new TCPPeerAdvertisement(serverKey.publicKey(), "localhost", server.getPort(), encryptedArchiveId).resolve();
		master.getBlacklist().clear();
		socket = new TCPPeerSocket(swarm, ad);
	}
	
	@After
	public void afterEach() throws IOException {
		master.getBlacklist().clear();
		server.close();
		swarm.close();
		socket.close();
	}
	
	@AfterClass
	public static void afterAll() {
		archive.close();
		master.close();
		TCPPeerSocket.disableMakeThreads = false;
		TCPPeerSocket.maxHandshakeTimeMillis = TCPPeerSocket.DEFAULT_MAX_HANDSHAKE_TIME_MILLIS;
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}

	@Test
	public void testInitializeWithClientSocket() throws Exception {
		TCPPeerSocket.disableMakeThreads = true;
		setupServerSocket((serverSocket, clientSocket, secret, states, sip)->{
			assertEquals(clientSocket, swarm.opened.socket);
			DummyConnection conn = new DummyConnection(serverSocket, clientSocket, states, sip);
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
			int msgLenObf = ByteBuffer.wrap(dummy.serverReadRaw(2)).getShort();
			int msgLen = dummy.sip.read().obfuscate2(msgLenObf);
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
	public void testReadThrowsExceptionIfMessageLengthIsZero() throws IOException, ProtocolViolationException {
		byte[] buf = new byte[1];
		DummyConnection dummy = new DummyConnection(socket).handshake();
		int obfLen = dummy.sip.write().obfuscate2(0);
		dummy.serverWriteRaw(Util.serializeShort((short) obfLen));
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
	
	@Test(expected = EOFException.class)
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
		setupServerSocket((serverSocket, clientSocket, secret, states, sip)->{
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
	
	// The following might get split into a PeerSocketTestBase later on... but they'd need to be rewritten!
	
	@Test
	public void testViolationBlacklistsPeer() throws IOException {
		new DummyConnection(socket).handshake();
		assertFalse(master.getBlacklist().contains(socket.getAddress()));
		socket.violation();
		assertTrue(master.getBlacklist().contains(socket.getAddress()));
	}
	
	@Test
	public void testViolationClosesSocket() throws IOException {
		new DummyConnection(socket).handshake();
		assertFalse(socket.isClosed());
		socket.violation();
		assertTrue(socket.isClosed());
	}
	
	@Test
	public void testSendsMessageSegmentsWhenReady() throws IOException {
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();
		byte[] payload = crypto.rng(128);
		ByteBuffer msgBuf = ByteBuffer.allocate(PeerMessage.HEADER_LENGTH + payload.length);
		msgBuf.position(PeerMessage.HEADER_LENGTH);
		msgBuf.put(payload);
		MessageSegment segment = new MessageSegment(1234, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, msgBuf);
		
		socket.dataReady(segment);
		ByteBuffer received = ByteBuffer.wrap(conn.serverReadNext());
		assertEquals(segment.msg.msgId, received.getInt());
		assertEquals(payload.length, received.getInt());
		assertEquals(segment.msg.cmd, received.get());
		assertEquals(segment.flags, received.get());
		assertEquals(0, received.getShort());
		
		byte[] readPayload = new byte[payload.length];
		received.get(readPayload);
		assertTrue(Arrays.equals(payload, readPayload));
		assertFalse(received.hasRemaining());
	}
	
	@Test
	public void testProcessesNewMessages() throws IOException {
		int msgId = 1234;
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();
		MessageSegment segment = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		conn.serverWrite(segment.content.array());
		assertTrue(Util.waitUntil(100, ()->socket.messageWithId(msgId) != null));
		assertEquals(msgId, socket.messageWithId(msgId).msgId);
	}
	
	@Test
	public void testProcessesContinuationsOfExistingMessages() throws IOException {
		int msgId = 1234;
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();
		MessageSegment segment = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH+1));
		conn.serverWrite(segment.content.array());
		
		assertTrue(Util.waitUntil(100, ()->socket.messageWithId(msgId) != null));
		PeerMessageIncoming msg = socket.messageWithId(msgId);
		assertTrue(Util.waitUntil(100, ()->msg.bytesReceived == 1));

		conn.serverWrite(segment.content.array());
		assertTrue(Util.waitUntil(100, ()->msg.bytesReceived == 2));
		msg.rxBuf.setEOF();
	}
	
	@Test
	public void testTriggersViolationIfLengthIsNegative() throws IOException, ProtocolViolationException, BlacklistedException, UnconnectableAdvertisementException {
		ByteBuffer buf = ByteBuffer.allocate(PeerMessage.HEADER_LENGTH);
		buf.putInt(1234);
		buf.putInt(-1);
		buf.put(PeerConnection.CMD_REQUEST_PAGE_TAGS);
		buf.put((byte) 0);
		buf.putShort((short) 0);
		
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();
		conn.serverWrite(buf.array());
	}
	
	@Test
	public void testTriggersViolationIfLengthExceedsLimit() throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(PeerMessage.HEADER_LENGTH);
		buf.putInt(1234);
		buf.putInt(socket.maxPayloadSize()+1);
		buf.put(PeerConnection.CMD_REQUEST_PAGE_TAGS);
		buf.put((byte) 0);
		buf.putShort((short) 0);
		
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();
		conn.serverWrite(buf.array());
		assertTrue(Util.waitUntil(100, ()->socket.isClosed()));
	}
	
	@Test
	public void testDoesntTriggerViolationIfLengthWithinLimit() throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(PeerMessage.HEADER_LENGTH);
		buf.putInt(1234);
		buf.putInt(socket.maxPayloadSize());
		buf.put(PeerConnection.CMD_REQUEST_PAGE_TAGS);
		buf.put((byte) 0);
		buf.putShort((short) 0);
		
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();
		conn.serverWrite(buf.array());
		assertFalse(Util.waitUntil(100, ()->socket.isClosed()));
	}
	
	@Test
	public void testIgnoresClosedMessageIDs() throws IOException {
		int msgId = 1234;
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();
		MessageSegment segment = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		conn.serverWrite(segment.content.array());
		
		assertTrue(Util.waitUntil(100, ()->socket.messageWithId(msgId) != null));
		PeerMessageIncoming msg = socket.messageWithId(msgId);
		
		segment = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) PeerMessage.FLAG_FINAL, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		conn.serverWrite(segment.content.array());
		assertTrue(Util.waitUntil(100, ()->socket.messageWithId(msgId) == null));
		assertEquals(0, msg.bytesReceived);
		
		segment = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) PeerMessage.FLAG_FINAL, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH+8));
		conn.serverWrite(segment.content.array());
		assertFalse(Util.waitUntil(100, ()->msg.bytesReceived > 0));
		assertNull(socket.messageWithId(msgId));
	}
	
	@Test
	public void testIgnoresSkippedMessageIDs() throws IOException, ProtocolViolationException, BlacklistedException, UnconnectableAdvertisementException {
		int msgId = 1234;
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();
		
		MessageSegment segment0 = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		MessageSegment segment1 = new MessageSegment(msgId-1, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		
		conn.serverWrite(segment0.content.array());
		conn.serverWrite(segment1.content.array());
		
		assertTrue(Util.waitUntil(200, ()->socket.messageWithId(msgId) != null));
		assertFalse(Util.waitUntil(200, ()->socket.messageWithId(msgId-1) != null));
	}
	
	@Test
	public void testDiscardsMessagesOnceSegmentMarkedFinal() throws IOException {
		int msgId = 1234;
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();
		MessageSegment segment = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		
		conn.serverWrite(segment.content.array());
		assertTrue(Util.waitUntil(100, ()->socket.messageWithId(msgId) != null));
		
		segment = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) PeerMessage.FLAG_FINAL, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		conn.serverWrite(segment.content.array());
		
		assertTrue(Util.waitUntil(100, ()->socket.messageWithId(msgId) == null));
	}
	
	@Test
	public void testDiscardsMessagesOnceFinishedMessageCalled() throws IOException {
		int msgId = 1234;
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();
		MessageSegment segment = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		conn.serverWrite(segment.content.array());
		
		assertTrue(Util.waitUntil(100, ()->socket.messageWithId(msgId) != null));
		PeerMessageIncoming msg = socket.messageWithId(msgId);
		socket.finishedMessage(msg);
		assertNull(socket.messageWithId(msgId));
		
		segment = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH+8));
		conn.serverWrite(segment.content.array());
		assertFalse(Util.waitUntil(100, ()->msg.bytesReceived > 0));
	}
	
	@Test
	public void testDiscardsOldestMessageWhenMaximumMessageCountIsReached() throws IOException {
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();
		
		MessageSegment[] segments = new MessageSegment[PeerMessage.DEFAULT_MAX_OPEN_MESSAGES];
		for(int i = 0; i < segments.length; i++) {
			segments[i] = new MessageSegment(i, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
			conn.serverWrite(segments[i].content.array());
			Util.sleep(1); // make sure we get differing timestamps on everything
		}
		
		for(int i = 0; i < segments.length; i++) {
			final int iFixed = i;
			assertTrue(Util.waitUntil(100, ()->socket.messageWithId(iFixed) != null));
		}
		
		MessageSegment oneMore = new MessageSegment(segments.length, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		conn.serverWrite(oneMore.content.array());
		
		assertTrue(Util.waitUntil(100, ()->socket.messageWithId(0) == null));
		for(int i = 1; i < segments.length; i++) {
			assertTrue(socket.messageWithId(i) != null);
		}
	}
	
	@Test
	public void testClosesSocketWhenMissingMessagesBecauseMaxReceivedIdMaxedOut() throws IOException {
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();

		MessageSegment segment0 = new MessageSegment(Integer.MAX_VALUE, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		MessageSegment segment1 = new MessageSegment(Integer.MIN_VALUE+1, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		
		conn.serverWrite(segment0.content.array());
		assertTrue(Util.waitUntil(100, ()->socket.messageWithId(Integer.MAX_VALUE) != null));
		assertFalse(socket.isClosed());

		conn.serverWrite(segment1.content.array());
		assertTrue(Util.waitUntil(100, ()->socket.isClosed()));
		assertFalse(socket.swarm.getConfig().getAccessor().getMaster().getBlacklist().contains(socket.getAddress()));
	}
	
	@Test
	public void testSendsCancelMessagesWhenReceivingSegmentsForClosedMessageIDs() throws IOException {
		int msgId = 1234;
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();

		MessageSegment segment0 = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		MessageSegment segment1 = new MessageSegment(msgId-1, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		
		conn.serverWrite(segment0.content.array());
		assertTrue(Util.waitUntil(100, ()->socket.messageWithId(msgId) != null));

		conn.serverWrite(segment1.content.array());
		ByteBuffer buf = ByteBuffer.wrap(conn.serverReadNext());
		assertEquals(segment1.msg.msgId, buf.getInt());
		assertEquals(0, buf.getInt()); // length
		assertEquals(segment1.msg.cmd, buf.get());
		assertEquals(PeerMessage.FLAG_CANCEL | PeerMessage.FLAG_FINAL, buf.get());
		assertEquals(0, buf.getShort());
		assertFalse(buf.hasRemaining());
	}
	
	@Test
	public void testDoesntSendCancelMessagesForSegmentsMarkedFinal() throws IOException {
		int msgId = 1234;
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();

		MessageSegment segment0 = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		MessageSegment segment1 = new MessageSegment(msgId-1, PeerConnection.CMD_REQUEST_PAGE_TAGS, PeerMessage.FLAG_FINAL, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		
		conn.serverWrite(segment0.content.array());
		assertTrue(Util.waitUntil(100, ()->socket.messageWithId(msgId) != null));

		conn.serverWrite(segment1.content.array());
		assertFalse(Util.waitUntil(100, ()->conn.hasAvailable()));
	}
	
	@Test
	public void testDoesntSendCancelMessagesForNewMessageIDs() throws IOException {
		int msgId = 1234;
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();

		MessageSegment segment0 = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		MessageSegment segment1 = new MessageSegment(msgId+1, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		
		conn.serverWrite(segment0.content.array());
		assertTrue(Util.waitUntil(100, ()->socket.messageWithId(msgId) != null));

		conn.serverWrite(segment1.content.array());
		assertFalse(Util.waitUntil(100, ()->conn.hasAvailable()));
	}
	
	@Test
	public void testDoesntSendCancelMessagesForExistingMessageIDs() throws IOException {
		int msgId = 1234;
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();

		MessageSegment segment0 = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		
		conn.serverWrite(segment0.content.array());
		assertTrue(Util.waitUntil(100, ()->socket.messageWithId(msgId) != null));

		conn.serverWrite(segment0.content.array());
		assertFalse(Util.waitUntil(100, ()->conn.hasAvailable()));
	}
	
	@Test
	public void testStopsSendingMessagesWhenCancelReceived() throws IOException {
		TCPPeerSocket.disableMakeThreads = false;
		DummyConnection conn = new DummyConnection(socket).handshake();
		InfiniteCircularInputStream stream = new InfiniteCircularInputStream(new byte[] { 4, 8, 15, 16, 23, 42 });
		PeerMessageOutgoing msg = socket.makeOutgoingMessage(PeerConnection.CMD_REQUEST_PAGE_TAGS, stream);
		assertTrue(Util.waitUntil(100, ()->conn.hasAvailable()));
		
		MessageSegment cancel = new MessageSegment(msg.msgId, msg.cmd, (byte) (PeerMessage.FLAG_CANCEL | PeerMessage.FLAG_FINAL), ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		conn.serverWrite(cancel.content.array());
		assertTrue(Util.waitUntil(100, ()->{
			if(!conn.hasAvailable()) return true;
			try { conn.serverReadNext(); } catch(IOException exc) {}
			return false;
		}));
	}
	
	@Test
	public void testAssignsMessageIdWhenIdIsIntegerMinValue() throws IOException, ProtocolViolationException {
		int msgId = Integer.MIN_VALUE;
		TCPPeerSocket.disableMakeThreads = false;
		new DummyConnection(socket).handshake();

		MessageSegment segment = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		assertEquals(msgId, segment.msg.msgId);
		socket.sendMessage(segment);
		assertNotEquals(msgId, segment.msg.msgId);
	}
	
	@Test
	public void testPreservesMessageIdWhenIdIsNotIntegerMinValue() throws IOException, ProtocolViolationException {
		int msgId = Integer.MIN_VALUE+1;
		TCPPeerSocket.disableMakeThreads = false;
		new DummyConnection(socket).handshake();

		MessageSegment segment = new MessageSegment(msgId, PeerConnection.CMD_REQUEST_PAGE_TAGS, (byte) 0, ByteBuffer.allocate(PeerMessage.HEADER_LENGTH));
		assertEquals(msgId, segment.msg.msgId);
		socket.sendMessage(segment);
		assertEquals(msgId, segment.msg.msgId);
	}
}
