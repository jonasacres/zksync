package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PRNG;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.utility.Util;

public class PeerMessageOutgoingTest {
	class DummyArchiveConfig extends ZKArchiveConfig {
		public DummyArchiveConfig() {
			this.archiveId = CryptoSupport.defaultCrypto().hash(Util.serializeInt(0));
		}
	}
	class DummySwarm extends PeerSwarm {
		public DummySwarm() {
			this.config = new DummyArchiveConfig();
		}
	}
	
	class DummySocket extends PeerSocket {
		MessageSegment received;
		int messageId = 1234;
		int timeoutMs = 250;
		byte[] written;
		
		int total;
		
		public DummySocket() {
			super(null);
			this.swarm = new DummySwarm();
		}
		
		@Override public PeerAdvertisement getAd() { return null; }
		@Override public void write(byte[] data, int offset, int length) {}
		@Override public int read(byte[] data, int offset, int length) { return 0; }
		@Override public boolean isLocalRoleClient() { return false; }
		@Override public void _close() {}
		@Override public boolean isClosed() { return false; }
		@Override public byte[] getSharedSecret() { return null; }
		@Override public String getAddress() { return "dummy"; }
		@Override public void handshake(PeerConnection conn) throws ProtocolViolationException, IOException {}
		@Override public int getPeerType() throws UnsupportedOperationException { return -1; }
		@Override public synchronized void dataReady(MessageSegment segment) {
			received = segment;
			this.notifyAll();
		}
		
		@Override public int issueMessageId() { return messageId; }
		public synchronized void waitForDataReady() throws TimeoutException {
			if(received == null) {
				try { this.wait(timeoutMs); } catch(InterruptedException exc) {}
				if(received == null) throw new TimeoutException();
			}
		}
		
		public byte[] readBufferedMessage(PeerMessageOutgoing msg) {
			synchronized(msg) {
				byte[] content = new byte[received.content.limit()];
				System.arraycopy(received.content.array(), 0, content, 0, content.length);
				total += received.content.limit() - PeerMessage.HEADER_LENGTH;
				received.delivered();
				received = null;
				return content;
			}
		}
	}
	
	class DummyPeerConnection extends PeerConnection {
		boolean paused;
		public DummyPeerConnection() { socket = new DummySocket(); }
		@Override public boolean isPausable(byte cmd) { return cmd == CMD; }
		@Override public synchronized void waitForUnpause() {
			while(paused) {
				try { this.wait(); } catch (InterruptedException e) { e.printStackTrace(); }
			}
		}
		
		public void pause() { paused = true; }
		public synchronized void unpause() { 
			paused = false;
			this.notifyAll();
		}
	}
	
	final static byte CMD = 1;
	DummyPeerConnection connection;
	DummySocket socket;
	PipedOutputStream writeEnd;
	PipedInputStream readEnd;
	PeerMessageOutgoing msg;
	PRNG readPRNG, writePRNG;
	
	public void assertReceivedMessage(byte[] expectedPayload, boolean expectEOF) throws TimeoutException {
		ByteBuffer expected = ByteBuffer.allocate(PeerMessage.HEADER_LENGTH + expectedPayload.length);
		expected.putInt(msg.msgId);
		expected.putInt(expectedPayload.length);
		expected.put(msg.cmd);
		expected.put(expectEOF ? PeerMessage.FLAG_FINAL : 0);
		expected.putShort((short) 0);
		expected.put(expectedPayload);
		
		socket.waitForDataReady();
		assertEquals(msg.msgId, socket.received.msg.msgId);
		byte[] received = socket.readBufferedMessage(msg);
		assertTrue(Arrays.equals(expected.array(), received));
	}
	
	@Before
	public void beforeEach() throws IOException {
		connection = new DummyPeerConnection();
		socket = (DummySocket) connection.socket;
		writeEnd = new PipedOutputStream();
		readEnd = new PipedInputStream(writeEnd);
		msg = new PeerMessageOutgoing(connection, CMD, readEnd);
		
		CryptoSupport crypto = CryptoSupport.defaultCrypto();
		readPRNG = crypto.prng(new byte[] {1, 2, 4, 8});
		writePRNG = crypto.prng(new byte[] {1, 2, 4, 8});
	}
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@After
	public void afterEach() throws IOException {
		connection.unpause();
		connection.close();
		msg.abort();
		readEnd.close();
		writeEnd.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Test
	public void testConstructor() {
		assertEquals(connection, msg.connection);
		assertEquals(CMD, msg.cmd);
		assertEquals(Integer.MIN_VALUE, msg.msgId);
		assertEquals(readEnd, msg.txPayload);
	}
	
	@Test
	public void testNotifiesSocketWhenDataReady() throws IOException, TimeoutException {
		assertNull(socket.received);
		writeEnd.write(new byte[1]);
		writeEnd.flush();
		socket.waitForDataReady();
		assertEquals(msg.msgId, socket.received.msg.msgId);
	}
	
	@Test
	public void testDoesNotSendWhenConnectionIsPaused() throws IOException {
		connection.pause();
		writeEnd.write(new byte[4]);
		writeEnd.flush();
		try {
			socket.waitForDataReady();
		} catch(TimeoutException exc) {}
		assertNull(socket.received);
	}
	
	@Test
	public void testResumesSendingWhenConnectionIsUnpaused() throws IOException, TimeoutException {
		connection.pause();
		writeEnd.write(new byte[4]);
		writeEnd.flush();
		connection.unpause();
		socket.waitForDataReady();
		assertEquals(msg.msgId, socket.received.msg.msgId);
	}
	
	@Test
	public void testSendsNonpausableCommandsWhenPaused() throws IOException, TimeoutException {
		PipedOutputStream writeEnd2 = new PipedOutputStream();
		PeerMessageOutgoing nonpausable = new PeerMessageOutgoing(connection, (byte) (CMD+1), new PipedInputStream(writeEnd2));
		connection.pause();
		writeEnd2.write(new byte[4]);
		writeEnd2.flush();
		socket.waitForDataReady();
		assertEquals(nonpausable.msgId, socket.received.msg.msgId);
		writeEnd2.close();
	}
	
	@Test
	public void testBuildsValidHeader() throws IOException, TimeoutException {
		byte[] payload = "Hello, world!".getBytes();
		writeEnd.write(payload);
		writeEnd.flush();
		assertReceivedMessage(payload, false);
	}
	
	@Test
	public void testMergesSuccessiveWrites() throws IOException, TimeoutException {
		// I'm uneasy about this test. Very timing dependent.
		byte[] payload = "Hello, world!".getBytes();
		writeEnd.write(payload, 0, 5);
		writeEnd.flush();
		writeEnd.write(payload, 5, payload.length - 5);
		writeEnd.flush();
		assertReceivedMessage(payload, false);
	}
	
	@Test
	public void testSendBigMessageThenLittle() throws IOException, TimeoutException {
		byte[] bigMessage = writePRNG.getBytes(2*msg.minPayloadBufferSize());
		byte[] littleMessage = writePRNG.getBytes(msg.minPayloadBufferSize()-1);

		writeEnd.write(bigMessage);
		writeEnd.flush();
		assertReceivedMessage(bigMessage, false);
		
		writeEnd.write(littleMessage);
		writeEnd.flush();
		assertReceivedMessage(littleMessage, false);
	}
	
	@Test
	public void testSendLittleMessageThenBig() throws IOException, TimeoutException {
		byte[] bigMessage = writePRNG.getBytes(2*msg.minPayloadBufferSize());
		byte[] littleMessage = writePRNG.getBytes(msg.minPayloadBufferSize()-1);
		
		writeEnd.write(littleMessage);
		writeEnd.flush();
		socket.waitForDataReady();
		assertReceivedMessage(littleMessage, false);

		writeEnd.write(bigMessage);
		writeEnd.flush();
		assertReceivedMessage(bigMessage, false);
	}
	
	@Test
	public void testSendOversizedMessages() throws IOException, TimeoutException {
		byte[] oversized = writePRNG.getBytes(100*msg.maxPayloadBufferSize());
		ByteBuffer readBuf = ByteBuffer.allocate(oversized.length);
		
		new Thread(()-> {
			try {
				writeEnd.write(oversized);
				writeEnd.flush();
				// these pipes seem to close before all data is read if the thread dies; keep thread going until buffer fills
				while(readBuf.hasRemaining()) {
					try { Thread.sleep(100); } catch (InterruptedException e) {}
				}
			} catch(IOException exc) {
				exc.printStackTrace();
			}
		}).start();
		
		while(readBuf.hasRemaining()) {
			socket.waitForDataReady();
			ByteBuffer chunk = ByteBuffer.wrap(socket.readBufferedMessage(msg));
			chunk.position(PeerMessage.HEADER_LENGTH);
			readBuf.put(chunk);
		}
		
		assertEquals(oversized.length, readBuf.position());
		assertTrue(Arrays.equals(oversized, readBuf.array()));
	}
	
	@Test
	public void testSetsTxClosed() throws IOException {
		assertFalse(msg.txClosed());
		writeEnd.close();
		assertTrue(Util.waitUntil(100, ()->msg.txClosed()));
	}
}
