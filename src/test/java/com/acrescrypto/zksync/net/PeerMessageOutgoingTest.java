package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PRNG;

public class PeerMessageOutgoingTest {
	class DummySocket extends PeerSocket {
		PeerMessageOutgoing received;
		int messageId = 1234;
		byte[] written;
		
		@Override public PeerAdvertisement getAd() { return null; }
		@Override public void write(byte[] data, int offset, int length) {}
		@Override public int read(byte[] data, int offset, int length) { return 0; }
		@Override public boolean isClient() { return false; }
		@Override public void close() {}
		@Override public boolean isClosed() { return false; }
		@Override public byte[] getSharedSecret() { return null; }
		@Override public String getAddress() { return "dummy"; }
		@Override public synchronized void dataReady(PeerMessageOutgoing msg) { received = msg; this.notifyAll(); }
		@Override public int issueMessageId() { return messageId; }
		public synchronized void waitForDataReady() { while(received == null) try { this.wait(); } catch(InterruptedException exc) {} } 
		public byte[] readBufferedMessage(PeerMessageOutgoing msg) {
			synchronized(msg) {
				ByteBuffer buf = ByteBuffer.allocate(msg.txBuf.position());
				buf.put(msg.txBuf.array(), 0, msg.txBuf.position());
				msg.clearTxBuf();
				received = null;
				return buf.array();
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
	
	public void assertReceivedMessage(byte[] expectedPayload, boolean expectEOF) {
		ByteBuffer expected = ByteBuffer.allocate(PeerMessage.HEADER_LENGTH + expectedPayload.length);
		expected.putInt(msg.msgId);
		expected.putInt(expectedPayload.length);
		expected.put(msg.cmd);
		expected.put(expectEOF ? PeerMessage.FLAG_FINAL : 0);
		expected.putShort((short) 0);
		expected.put(expectedPayload);
		
		socket.waitForDataReady();
		assertEquals(msg, socket.received);
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
		
		CryptoSupport crypto = new CryptoSupport();
		readPRNG = crypto.prng(new byte[] {1, 2, 4, 8});
		writePRNG = crypto.prng(new byte[] {1, 2, 4, 8});
	}
	
	@Test
	public void testConstructor() {
		assertEquals(connection, msg.connection);
		assertEquals(CMD, msg.cmd);
		assertEquals(socket.messageId, msg.msgId);
		assertEquals(readEnd, msg.txPayload);
	}
	
	@Test
	public void testNotifiesSocketWhenDataReady() throws IOException {
		assertNull(socket.received);
		writeEnd.write(new byte[1]);
		try { Thread.sleep(1); } catch(InterruptedException exc) { exc.printStackTrace(); }
		assertEquals(msg, socket.received);
	}
	
	@Test
	public void testDoesNotSendWhenConnectionIsPaused() throws IOException {
		connection.pause();
		writeEnd.write(new byte[4]);
		try { Thread.sleep(1); } catch(InterruptedException exc) { exc.printStackTrace(); }
		assertNull(socket.received);
	}
	
	@Test
	public void testResumesSendingWhenConnectionIsUnpaused() throws IOException {
		connection.pause();
		writeEnd.write(new byte[4]);
		connection.unpause();
		try { Thread.sleep(1); } catch(InterruptedException exc) { exc.printStackTrace(); }
		assertEquals(msg, socket.received);
	}
	
	@Test
	public void testSendsNonpausableCommandsWhenPaused() throws IOException {
		PipedOutputStream writeEnd2 = new PipedOutputStream();
		PeerMessageOutgoing nonpausable = new PeerMessageOutgoing(connection, (byte) (CMD+1), new PipedInputStream(writeEnd2));
		
		connection.pause();
		writeEnd2.write(new byte[4]);
		try { Thread.sleep(1); } catch(InterruptedException exc) { exc.printStackTrace(); }
		assertEquals(nonpausable, socket.received);
	}
	
	@Test
	public void testBuildsValidHeader() throws IOException {
		byte[] payload = "Hello, world!".getBytes();
		writeEnd.write(payload);
		assertReceivedMessage(payload, false);
	}
	
	@Test
	public void testUpdatesHeaderInSuccessiveWrites() throws IOException {
		byte[] payload = "Hello, world!".getBytes();
		writeEnd.write(payload, 0, 5);
		writeEnd.write(payload, 5, payload.length - 5);
		assertReceivedMessage(payload, false);
	}
	
	@Test
	public void testSendBigMessageThenLittle() throws IOException {
		byte[] bigMessage = writePRNG.getBytes(2*msg.minPayloadBufferSize());
		byte[] littleMessage = writePRNG.getBytes(msg.minPayloadBufferSize()-1);

		System.out.println(System.currentTimeMillis() + " - writing");
		writeEnd.write(bigMessage);
		System.out.println(System.currentTimeMillis() + " - waiting");
		try { Thread.sleep(1); } catch(InterruptedException exc) { exc.printStackTrace(); }
		System.out.println(System.currentTimeMillis() + " - receiving");
		assertReceivedMessage(bigMessage, false);
		
		System.out.println(System.currentTimeMillis() + " - writing little");
		writeEnd.write(littleMessage);
		System.out.println(System.currentTimeMillis() + " - waiting");
		try { Thread.sleep(1); } catch(InterruptedException exc) { exc.printStackTrace(); }
		System.out.println(System.currentTimeMillis() + " - receiving");
		assertReceivedMessage(littleMessage, false);
		System.out.println(System.currentTimeMillis() + " - received");
	}
	
	@Test
	public void testSendLittleMessageThenBig() throws IOException {
		byte[] bigMessage = writePRNG.getBytes(2*msg.minPayloadBufferSize());
		byte[] littleMessage = writePRNG.getBytes(msg.minPayloadBufferSize()-1);
		
		writeEnd.write(littleMessage);
		try { Thread.sleep(1); } catch(InterruptedException exc) { exc.printStackTrace(); }
		assertReceivedMessage(littleMessage, false);

		writeEnd.write(bigMessage);
		try { Thread.sleep(1); } catch(InterruptedException exc) { exc.printStackTrace(); }
		assertReceivedMessage(bigMessage, false);
}
	
	@Test
	public void testSendOversizedMessages() throws IOException {
		byte[] oversized = writePRNG.getBytes(2*msg.maxPayloadBufferSize());
		ByteBuffer readBuf = ByteBuffer.allocate(oversized.length);
		
		new Thread(()-> {
			try {
				writeEnd.write(oversized);
				writeEnd.close();
			} catch(IOException exc) {
				exc.printStackTrace();
			}
		}).start();
		
		while(!msg.txEOF) {
			socket.waitForDataReady();
			ByteBuffer chunk = ByteBuffer.wrap(socket.readBufferedMessage(msg));
			assertTrue(chunk.capacity() >= PeerMessage.HEADER_LENGTH);
			assertTrue(readBuf.remaining() >= chunk.capacity() - PeerMessage.HEADER_LENGTH);
			readBuf.put(chunk.array(), PeerMessage.HEADER_LENGTH, chunk.capacity() - PeerMessage.HEADER_LENGTH);
		}
		
		assertTrue(Arrays.equals(oversized, readBuf.array()));
	}
	
	// sends data to socket with header
	// injects header to each successive write operation
	// supports variably-sized write operations
	
	// txClosed false if EOF not reached
	// sets txClosed on EOF
	// thread safety
}
