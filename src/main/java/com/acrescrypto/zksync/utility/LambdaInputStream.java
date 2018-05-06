package com.acrescrypto.zksync.utility;

import java.io.IOException;
import java.io.InputStream;

public class LambdaInputStream extends InputStream {
	public interface LambdaInputStreamSource {
		public InputStream nextStream();
	}
	
	protected LambdaInputStreamSource source;
	protected InputStream current;
	protected boolean closed;
	
	public LambdaInputStream(LambdaInputStreamSource source) {
		this.source = source;
	}

	@Override
	public int read() throws IOException {
		int r = -1;
		while(r == -1 && !closed) {
			if(current != null) r = current.read();
			if(r == -1) nextStream();
			else return r;
		}
		
		return -1;
	}
	
	@Override
	public int read(byte[] buf, int off, int len) throws IOException {
		int readBytes = 0;
		int r = -1;
		
		while(!closed && readBytes < len) {
			if(current != null) r = current.read(buf, off+readBytes, len-readBytes);
			if(r == -1) nextStream();
		}
		
		if(closed && readBytes == 0) return -1;
		return readBytes;
	}
	
	@Override
	public int available() throws IOException {
		if(current == null) nextStream();
		if(closed) return -1;
		return current.available();
	}
	
	@Override
	public void close() {
		closed = true;
		current = null;
	}
	
	protected void nextStream() {
		if(closed) return;
		current = source.nextStream();
		if(current == null) closed = true;
	}
}
