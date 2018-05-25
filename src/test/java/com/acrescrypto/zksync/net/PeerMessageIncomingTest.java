package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PRNG;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;

public class PeerMessageIncomingTest {
	class DummySocket extends PeerSocket {
		boolean violated;
		@Override public PeerAdvertisement getAd() { return null; }
		@Override public void write(byte[] data, int offset, int length) {}
		@Override public int read(byte[] data, int offset, int length) { return 0; }
		@Override public boolean isLocalRoleClient() { return false; }
		@Override public void close() {}
		@Override public boolean isClosed() { return false; }
		@Override public byte[] getSharedSecret() { return null; }
		@Override public void violation() { violated = true; }
		@Override public String getAddress() { return "dummy"; }
		@Override public void handshake() {}
		@Override public int getPeerType() { return -1; }
	}
	
	class DummyPeerConnection extends PeerConnection {
		boolean messageReceived;
		public DummyPeerConnection() { socket = new DummySocket(); }
		@Override public void handle(PeerMessageIncoming incoming) throws ProtocolViolationException { messageReceived = true; }
	}
	
	final static byte CMD = 1;
	final static byte FLAGS = 2;
	final static int MSG_ID = 1234;
	
	PeerMessageIncoming msg;
	
	@Before
	public void beforeEach() {
		msg = new PeerMessageIncoming(new DummyPeerConnection(), CMD, FLAGS, MSG_ID);
	}
	
	@Test
	public void testConstructor() {
		assertEquals(CMD, msg.cmd);
		assertEquals(FLAGS, msg.flags);
		assertEquals(MSG_ID, msg.msgId);
	}
	
	@Test
	public void testInvokesConnectionHandlerOnNewThread() {
		class Holder { Thread thread; };
		Holder holder = new Holder();

		PeerConnection conn = new DummyPeerConnection() { 
			public void handle(PeerMessageIncoming msg) {
				synchronized(holder) {
					holder.thread = Thread.currentThread();
					holder.notifyAll();
				}
			};
		};
		
		synchronized(holder) {
			new PeerMessageIncoming(conn, (byte) 0, (byte) 0, 0);
			try {
				holder.wait();
			} catch (InterruptedException e) { fail(); }
		}
		
		assertNotEquals(Thread.currentThread(), holder.thread);
	}
	
	@Test
	public void testLogsSocketViolationOnProtocolViolationExceptionFromHandler() {
		PeerConnection conn = new DummyPeerConnection() { 
			public void handle(PeerMessageIncoming msg) throws ProtocolViolationException { throw new ProtocolViolationException(); }
		};
		
		(new PeerMessageIncoming(conn, (byte) 0, (byte) 0, 0)).waitForFinish();
		assertTrue(((DummySocket) conn.socket).violated);
	}
	
	@Test
	public void testRxbufReadReturnsImmediatelyWhenDataReady() throws EOFException {
		msg.receivedData((byte) 0, "some data for you".getBytes());
		msg.rxBuf.get(); // either returns immediately or hangs
	}
	
	@Test
	public void testRxbufReadBlocksWhenNoDataReady() throws EOFException {
		class Holder { boolean cleared; };
		Holder holder = new Holder();
		
		Thread thread = new Thread(() -> {
			try {
				msg.rxBuf.get();
			} catch (EOFException e) {
				e.printStackTrace();
			}
			
			holder.cleared = true;
		});
		thread.start();
		
		try { Thread.sleep(1); } catch (InterruptedException e) { e.printStackTrace(); }
		assertFalse(holder.cleared);
		msg.receivedData((byte) 0, new byte[1]);
		try { thread.join(1000); } catch (InterruptedException e) { e.printStackTrace(); }
		assertTrue(holder.cleared);
	}
	
	@Test
	public void testRxBufReadWithoutArgsReturnsAllPendingData() throws EOFException {
		msg.receivedData((byte) 0, "mary had a ".getBytes());
		msg.receivedData((byte) 0, "little lamb".getBytes());
		
		byte[] data = msg.rxBuf.read();
		assertEquals("mary had a little lamb", new String(data));
	}
	
	@Test
	public void testRxbufReadWithLengthReturnsStatedSize() throws EOFException {
		msg.receivedData((byte) 0, "0123456789".getBytes());
		byte[] data = msg.rxBuf.read(5);
		assertEquals("01234", new String(data));
	}
	
	@Test
	public void testRxbufReadWithLengthBlocksUntilRequestedLengthAvailable() throws EOFException {
		class Holder { boolean passed; };
		Holder holder = new Holder();
		
		Thread thread = new Thread(() -> {
			byte[] data;
			try {
				data = msg.rxBuf.read(20);
				holder.passed = "01234567890123456789".equals(new String(data));
			} catch (EOFException e) {
			}
		});
		
		msg.receivedData((byte) 0, "0123456789".getBytes());
		thread.start();
		try { Thread.sleep(1); } catch (InterruptedException e) { e.printStackTrace(); };
		assertFalse(holder.passed);
		msg.receivedData(PeerMessage.FLAG_FINAL, "0123456789".getBytes());
		try { thread.join(1000); } catch (InterruptedException e) { e.printStackTrace(); }
		assertTrue(holder.passed);
	}
	
	@Test
	public void testRxbufReadWithLengthThrowsEOFIfFileEndsBeforeLengthReached() {
		msg.receivedData(PeerMessage.FLAG_FINAL, "0123456789".getBytes());
		try {
			msg.rxBuf.read(20);
			fail();
		} catch(EOFException exc) {}
	}
	
	@Test
	public void testRxbufReadWithArrayFillsGivenArray() throws EOFException {
		byte[] data = new byte[10];
		msg.receivedData((byte) 0, "0123456789".getBytes());
		assertEquals(data, msg.rxBuf.read(data));
		assertEquals("0123456789", new String(data));
	}
	
	@Test
	public void testRxbufGetReturnsNextByte() throws EOFException {
		msg.receivedData((byte) 0, new byte[] {0,1,2,3,4,5,6,7,8,9});
		for(byte i = 0; i < 10; i++) {
			assertEquals(i, msg.rxBuf.get());
		}
	}
	
	@Test
	public void testRxbufGetShortReturnsNextShort() throws EOFException {
		short[] shorts = { 0, -1, 9, Short.MIN_VALUE, Short.MAX_VALUE };
		
		for(short s : shorts) {
			msg.receivedData((byte) 0, ByteBuffer.allocate(2).putShort(s).array());
		}
		
		for(short s : shorts) {
			assertEquals(s, msg.rxBuf.getShort());
		}
	}
	
	@Test
	public void testRxbufGetIntReturnsNextInt() throws EOFException {
		int[] ints = { 0, -1, 203480, Integer.MIN_VALUE, Integer.MAX_VALUE };
		
		for(int i : ints) {
			msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(i).array());
		}
		
		for(int i : ints) {
			assertEquals(i, msg.rxBuf.getInt());
		}
	}
	
	@Test
	public void testRxbufGetLongReturnsNextLong() throws EOFException {
		long[] longs = { 0, -1, 4325840912380229409l, Long.MIN_VALUE, Long.MAX_VALUE };
		
		for(long x : longs) {
			msg.receivedData((byte) 0, ByteBuffer.allocate(8).putLong(x).array());
		}
		
		for(long x : longs) {
			assertEquals(x, msg.rxBuf.getLong());
		}
	}
	
	@Test
	public void testReceivedDataAccumulatesUpToMaxBufferSizeWithoutBlocking() {
		msg.receivedData((byte) 0, new byte[PeerMessageIncoming.MAX_BUFFER_SIZE]);
	}
	
	@Test
	public void testReceivedDataBlocksUntilReadWhenMaxBufferSizeExceeded() throws EOFException {
		class Holder { boolean passed; }
		Holder holder = new Holder();
		
		msg.receivedData((byte) 0, new byte[PeerMessageIncoming.MAX_BUFFER_SIZE]);
		Thread thread = new Thread(()->{
			msg.receivedData((byte) 0, new byte[1]);
			holder.passed = true;
		});
		thread.start();
		
		try { Thread.sleep(1); } catch (InterruptedException e) { e.printStackTrace(); };
		assertFalse(holder.passed);
		msg.rxBuf.get();
		try { Thread.sleep(1); } catch (InterruptedException e) { e.printStackTrace(); };
		assertTrue(holder.passed);
	}
	
	@Test
	public void testReadBufferMaintainsConsistencyThroughResize() throws EOFException {
		byte[] seed = { 1, 2, 3, 4 };
		CryptoSupport crypto = new CryptoSupport();
		PRNG prngWrite = crypto.prng(seed), prngRead = crypto.prng(seed);
		
		// fill up the buffer, then read just a little bit
		msg.rxBuf.write(prngWrite.getBytes(PeerMessageIncoming.MAX_BUFFER_SIZE));
		assertTrue(Arrays.equals(prngRead.getBytes(1024), msg.rxBuf.read(1024)));
		
		// fill it back up in two separate operations; then empty everything and make sure it matches
		msg.rxBuf.write(prngWrite.getBytes(512));
		msg.rxBuf.write(prngWrite.getBytes(512));
		assertTrue(Arrays.equals(prngRead.getBytes(PeerMessageIncoming.MAX_BUFFER_SIZE), msg.rxBuf.read(PeerMessageIncoming.MAX_BUFFER_SIZE)));
		
		// now that we drained the whole thing, fill it up again (assumes MAX_BUFFER_SIZE % 1024 == 0)
		for(int i = 0; i < PeerMessageIncoming.MAX_BUFFER_SIZE; i += 1024) {
			msg.rxBuf.write(prngWrite.getBytes(1024));
		}
		assertTrue(Arrays.equals(prngRead.getBytes(PeerMessageIncoming.MAX_BUFFER_SIZE), msg.rxBuf.read(PeerMessageIncoming.MAX_BUFFER_SIZE)));
	}
	
	@Test
	public void testReceivedDataSetsEOFWhenFLAGFINALSet() {
		msg.receivedData((byte) 0, new byte[1]);
		assertFalse(msg.rxBuf.isEOF());
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[1]);
		assertTrue(msg.rxBuf.isEOF());
	}
	
	@Test
	public void testReceivedDataTriggersWaitForEOFWhenFLAGFINALSet() {
		class Holder { boolean finished; }
		Holder holder = new Holder();
		
		Thread thread = new Thread(()-> {
			msg.rxBuf.waitForEOF();
			holder.finished = true;
		});
		
		thread.start();
		msg.receivedData((byte) 0, new byte[1]);
		try { Thread.sleep(1); } catch (InterruptedException e) { e.printStackTrace(); };
		assertFalse(holder.finished);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[1]);
		try { thread.join(1000); } catch (InterruptedException e) { e.printStackTrace(); }
		assertTrue(holder.finished);
	}
	
	@Test
	public void testReceivedDataTrigersEOFExceptionsWhenFLAGFINALSet() {
		class Holder { boolean finished; }
		Holder holder = new Holder();
		
		Thread thread = new Thread(()-> {
			try {
				msg.rxBuf.read(2);
			} catch (EOFException e) {
				holder.finished = true;
			}
		});
		thread.start();
		
		msg.receivedData((byte) 0, new byte[1]);
		try { Thread.sleep(1); } catch (InterruptedException e) { e.printStackTrace(); };
		assertFalse(holder.finished);
		msg.receivedData(PeerMessage.FLAG_FINAL, new byte[0]);
		try { thread.join(1000); } catch (InterruptedException e) { e.printStackTrace(); };
		assertTrue(holder.finished);
	}
	
	@Test
	public void testToleratesWriteOfZeroLength() {
		class Holder { boolean finished; }
		Holder holder = new Holder();

		Thread thread = new Thread(()-> {
			try {
				holder.finished = msg.rxBuf.getInt() == 0x12345678;
			} catch (EOFException e) {
			}
		});
		
		thread.start();
		msg.receivedData((byte) 0, new byte[0]);
		try { Thread.sleep(1); } catch (InterruptedException e) { e.printStackTrace(); };
		assertFalse(holder.finished);
		msg.receivedData((byte) 0, ByteBuffer.allocate(4).putInt(0x12345678).array());
		try { thread.join(1000); } catch (InterruptedException e) { e.printStackTrace(); };
		assertTrue(holder.finished);
	}
}

