package com.acrescrypto.zksync.utility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;

public class AppendableInputStreamTest {
	AppendableInputStream stream;
	CryptoSupport crypto;
	
	void assertReadBytes(byte[] expected) {
		byte[] read = new byte[expected.length];
		stream.read(read);
		assertTrue(Arrays.equals(expected, read));
	}
	
	@Before
	public void beforeEach() {
		crypto = new CryptoSupport();
		stream = new AppendableInputStream();
	}
	
	@Test
	public void testWriteByte() {
		byte[] bytes = { 1, 2, 3, 4 };
		for(byte b : bytes) {
			stream.write(b);
		}
		
		assertReadBytes(bytes);
	}
	
	@Test
	public void testWriteArray() {
		byte[] bytes = crypto.rng(128);
		stream.write(bytes);
		assertReadBytes(bytes);
	}
	
	@Test
	public void testWriteArrayWithArguments() {
		byte[] bytes = crypto.rng(128);
		stream.write(bytes, 16, 64);
		
		byte[] expected = new byte[64];
		System.arraycopy(bytes, 16, expected, 0, expected.length);
		assertReadBytes(expected);
	}
	
	@Test
	public void testReadByte() {
		byte[] bytes = crypto.rng(64);
		stream.write(bytes);
		for(byte b : bytes) {
			assertEquals(b, stream.read());
		}
	}
	
	@Test
	public void testReadArray() {
		byte[] bytes = crypto.rng(64);
		stream.write(bytes);
		
		byte[] expected = new byte[bytes.length];
		stream.read(expected);
		assertTrue(Arrays.equals(expected, bytes));
	}
	
	@Test
	public void testReadWithArguments() {
		byte[] bytes = crypto.rng(64);
		stream.write(bytes);
		
		byte[] expected = new byte[128], read = new byte[128];
		System.arraycopy(bytes, 0, expected, 32, bytes.length-16);
		stream.read(read, 32, bytes.length-16);
		assertTrue(Arrays.equals(expected, read));
	}
	
	@Test
	public void testReadBlocksUntilData() throws InterruptedException {
		byte[] expected = { 1, 2, 3, 4 };
		byte[] read = new byte[expected.length];
		class Holder { boolean waited; }
		Holder holder = new Holder();
		
		stream.write(expected, 0, expected.length-1);
		
		Thread thread = new Thread(()-> {
			stream.read(read, 0, expected.length);
			holder.waited = true;
		});
		
		thread.start();
		Thread.sleep(10);
		assertFalse(holder.waited);
		stream.write(expected, expected.length-1, 1);
		thread.join(100);
		assertTrue(holder.waited);
		assertTrue(Arrays.equals(expected, read));
	}
	
	@Test
	public void testReadBlocksUntilEOF() throws InterruptedException {
		byte[] write = { 1, 2, 3, 4 };
		byte[] read = new byte[write.length], expected = write.clone();
		expected[expected.length-1] = 0;
		
		class Holder { boolean waited; }
		Holder holder = new Holder();
		
		stream.write(write, 0, write.length-1);
		
		Thread thread = new Thread(()-> {
			stream.read(read, 0, write.length);
			holder.waited = true;
		});
		
		thread.start();
		Thread.sleep(10);
		assertFalse(holder.waited);
		stream.eof();
		thread.join(100);
		assertTrue(holder.waited);
		assertTrue(Arrays.equals(expected, read));
	}
	
	@Test
	public void testReadReturnsActualBytesRead() {
		stream.write(new byte[8]);
		stream.eof();
		assertEquals(8, stream.read(new byte[16]));
	}
	
	@Test
	public void testReadReturnsNegativeOneIfEOF() {
		stream.write(new byte[8]);
		stream.eof();
		stream.read(new byte[16]);
		
		assertEquals(-1, stream.read());
		assertEquals(-1, stream.read(new byte[1]));
		assertEquals(-1, stream.read(new byte[1], 0, 1));
	}
	
	@Test
	public void testConcurrency() throws InterruptedException {
		byte[] data = crypto.rng(1024*1024);
		byte[] read = new byte[data.length];
		
		Thread writer = new Thread(()->{
			int r = 0;
			while(r < data.length) {
				int len = Math.min(data.length-r, (int) (Math.random()*1024)+1);
				stream.write(data, r, len);
				r += len;
				if(Math.random() < 0.5) try { Thread.sleep(1); } catch(InterruptedException exc) {}
			}
			stream.eof();
		});
		
		Thread reader = new Thread(()->{
			int r = 0;
			while(r < data.length) {
				int readLen = Math.min(read.length-r, (int) (Math.random()*1024)+1);
				int rv = stream.read(read, r, readLen);
				if(rv <= 0) {
					System.out.println("Read " + r);
					break;
				}
				r += rv;
				if(Math.random() < 0.5) try { Thread.sleep(1); } catch(InterruptedException exc) {}
			}
		});
		
		reader.start();
		writer.start();
		
		writer.join();
		reader.join();
		
		assertTrue(Arrays.equals(data, read));
	}
}
