package com.acrescrypto.zksync.utility;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;

public class StorableBuffer {
	protected ByteBuffer memBuf;
	protected String path;
	protected FS fs;
	protected File file;
	protected int length, swapThreshold;
	
	public class ByteBufferBackedInputStream extends InputStream {

	    ByteBuffer buf;

	    public ByteBufferBackedInputStream(ByteBuffer buf) {
	        this.buf = buf;
	    }

	    public int read() throws IOException {
	        if (!buf.hasRemaining()) {
	            return -1;
	        }
	        return buf.get() & 0xFF;
	    }

	    public int read(byte[] bytes, int off, int len)
	            throws IOException {
	        if (!buf.hasRemaining()) {
	            return -1;
	        }

	        len = Math.min(len, buf.remaining());
	        buf.get(bytes, off, len);
	        return len;
	    }
	}
	
	protected static FS scratchFS;
	
	public static void setScratchFS(FS newScratchFS) {
		scratchFS = newScratchFS; 
	}
	
	public static FS getScratchFS() {
		return scratchFS;
	}
	
	public static StorableBuffer scratchBuffer(int swapThreshold) {
		return new StorableBuffer(scratchFS, swapThreshold);
	}
	
	public StorableBuffer(FS fs, int swapThreshold) {
		this.fs = fs;
		this.memBuf = ByteBuffer.allocate(Math.min(swapThreshold, 1024));
		this.swapThreshold = swapThreshold;
		this.path = "scratch-" + Util.bytesToHex((new CryptoSupport()).rng(32));
	}
	
	public synchronized void put(byte[] data) throws IOException {
		if(length + data.length < swapThreshold) {
			putInMemory(data);
		} else {
			putToFile(data);
		}
		
		length += data.length;
	}
	
	public InputStream getInputStream() throws IOException {
		if(memBuf != null) {
			return new ByteBufferBackedInputStream(memBuf);
		} else {
			return fs.open(path, File.O_RDONLY).getInputStream();
		}
	}
	
	public int getLength() {
		return length;
	}
	
	protected void putInMemory(byte[] data) {
		if(memBuf.remaining() < data.length) {
			int newSize = 1 << (int) Math.ceil(Math.log(memBuf.position() + data.length)/Math.log(2));
			ByteBuffer newBuf = ByteBuffer.allocate(newSize);
			newBuf.put(memBuf.array(), 0, memBuf.position());
			memBuf = newBuf;
		}
		
		memBuf.put(data);
	}
	
	protected void putToFile(byte[] data) throws IOException {
		if(file == null) convertToFile();
		file.write(data);
	}
	
	protected void convertToFile() throws IOException {
		file = fs.open(path, File.O_WRONLY|File.O_CREAT|File.O_TRUNC);
		byte[] array = new byte[memBuf.position()];
		memBuf.rewind();
		memBuf.get(array);
		file.write(memBuf.array());
		memBuf = null;
	}
	
	public void delete() throws IOException {
		memBuf = null;
		if(file == null) return;
		if(fs.exists(path)) fs.unlink(path);
		file = null;
	}
}
