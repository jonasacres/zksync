package com.acrescrypto.zksync.fs;

import java.io.IOException;
import java.io.InputStream;

public class FileInputStream extends InputStream {
	protected File file;
	private byte[] buf = new byte[1];
	protected long markOffset;
	
	public FileInputStream(File file) {
		this.file = file;
		mark(0);
	}

	@Override
	public int read() throws IOException {
		if(file.read(buf, 0, 1) < 0) return -1;
		return buf[0];
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		return file.read(b, off, len);
	}
	
	@Override
	public long skip(long n) throws IOException {
		long start = file.seek(0, File.SEEK_CUR); 
		return file.seek(n, File.SEEK_CUR) - start;
	}
	
	@Override
	public int available() throws IOException {
		return file.available();
	}
	
	@Override
	public void mark(int readlimit) {
		try {
			markOffset = file.seek(0, File.SEEK_SET);
		} catch (IOException e) {
		}
	}
	
	@Override
	public void reset() throws IOException {
		file.seek(markOffset, File.SEEK_SET);
	}
	
	@Override
	public boolean markSupported() {
		return true;
	}
}
