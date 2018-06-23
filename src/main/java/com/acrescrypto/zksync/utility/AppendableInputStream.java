package com.acrescrypto.zksync.utility;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

public class AppendableInputStream extends InputStream {
	protected Queue<ByteBuffer> chunks = new LinkedList<ByteBuffer>();
	protected boolean eof;
	long readLen, writeLen;

	public void write(int b) {
		write(ByteBuffer.wrap(new byte[] { (byte) b }));
	}
	
	public void write(byte[] data) {
		write(ByteBuffer.wrap(data));
	}
	
	public void write(byte[] data, int offset, int length) {
		write(ByteBuffer.wrap(data, offset, length));
	}
	
	public synchronized void write(ByteBuffer buf) {
		writeLen += buf.remaining();
		chunks.add(buf);
		this.notifyAll();
	}
	
	public synchronized void eof() {
		eof = true;
		this.notifyAll();
	}
	
	@Override
	public synchronized int available() {
		if(finished()) return -1;
		return (int) (writeLen - readLen);
	}
	
	@Override
	public int read() {
		byte[] b = new byte[] { -1 };
		read(b, 0, 1);
		return b[0];
	}
	
	@Override
	public int read(byte[] b) {
		return read(b, 0, b.length);
	}

	@Override
	public synchronized int read(byte[] b, int offset, int length) {
		if(finished()) {
			return -1;
		}
		
		int numBytesRead = 0;
		
		while(numBytesRead < length) {
			while(!chunks.isEmpty() && !chunks.peek().hasRemaining()) {
				chunks.remove();
			}
			waitForChunk();
			if(finished()) break;
			
			ByteBuffer buf = chunks.peek();
			int bufReadLen = Math.min(length-numBytesRead, buf.remaining());
			buf.get(b, numBytesRead+offset, bufReadLen);
			numBytesRead += bufReadLen;
			readLen += bufReadLen;
		}
		
		return numBytesRead;
	}
	
	protected synchronized void waitForChunk() {
		while(chunks.isEmpty() && !eof) {
			try {
				this.wait();
			} catch (InterruptedException e) {}
		}
	}
	
	protected boolean finished() {
		return readLen == writeLen && eof;
	}
}
