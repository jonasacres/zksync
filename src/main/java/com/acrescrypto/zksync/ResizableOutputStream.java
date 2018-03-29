package com.acrescrypto.zksync;

import java.io.IOException;
import java.io.OutputStream;

public class ResizableOutputStream extends OutputStream {
	public interface ResizableOutputStreamLambda {
		public void write(byte[] buf, int offset);
	}
	
	protected byte[] buf;
	protected int offset;
	protected ResizableOutputStreamLambda lambda;
	
	public ResizableOutputStream() {
		this(null);
	}
	
	public ResizableOutputStream(ResizableOutputStreamLambda lambda) {
		this.lambda = lambda;
		buf = new byte[256];
	}
	
	@Override
	public void write(byte[] b) {
		write(b, 0, b.length);
	}
	
	@Override
	public void write(byte[] b, int off, int len) {
		checkSize(len);
		for(int i = 0; i < len; i++) buf[offset++] = b[i];
		if(lambda != null) lambda.write(buf, offset);
	}

	@Override
	public void write(int b) throws IOException {
		checkSize(1);
		buf[offset++] = (byte) b;
		if(lambda != null) lambda.write(buf, offset);
	}
	
	public byte[] toArray() {
		byte[] array = new byte[offset];
		for(int i = 0; i < offset; i++) array[i] = buf[i];
		return array;
	}
	
	protected void checkSize(int bytesToAdd) {
		if(offset + bytesToAdd < buf.length) return;
		int size = buf.length;
		while(size <= offset + bytesToAdd) size *= 2;
		
		byte[] newBuf = new byte[size];
		for(int i = 0; i < offset; i++) newBuf[i] = buf[i];
		buf = newBuf;
	}
	
	public int getOffset() {
		return offset;
	}
}
