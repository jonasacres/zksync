package com.acrescrypto.zksync.fs;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class File implements Closeable {
	public final static int O_RDONLY = 1 << 0;
	public final static int O_WRONLY = 1 << 1;
	public final static int O_RDWR = O_RDONLY | O_WRONLY; // no this is not how POSIX works, but it is nicer
	public final static int O_CREAT = 1 << 2;
	public final static int O_NOFOLLOW = 1 << 3;
	public final static int O_APPEND = 1 << 4;
	public final static int O_TRUNC = 1 << 5;
	
	public final static int SEEK_SET = 0;
	public final static int SEEK_CUR = 1;
	public final static int SEEK_END = 2;
	
	public abstract String getPath();
	public abstract Stat getStat() throws IOException;
	
	public long getSize() throws IOException {
		return getStat().getSize();
	}

	protected FS fs;
	protected File(FS fs) {
		this.fs = fs;
		fs.reportOpenFile(this);
	}
	
	protected Logger logger = LoggerFactory.getLogger(File.class);
	
	public abstract void truncate(long size) throws IOException;
	public abstract int read(byte[] buf, int offset, int maxLength) throws IOException;

	public final byte[] read(int maxLength) throws IOException {
		if(maxLength <= 0) maxLength = (int) getSize();
		maxLength = (int) Math.max(0, Math.min(maxLength, getSize()));
		byte[] buf = new byte[(int) maxLength];
		int readBytes = read(buf, 0, (int) maxLength);
		if(readBytes < buf.length && readBytes >= 0) {
			byte[] newBuf = new byte[readBytes];
			for(int i = 0; i < newBuf.length; i++) newBuf[i] = buf[i];
			buf = newBuf;
		}
		
		logger.trace("FS -: read {} maxLen={} actualLen={} newOffset={} readBytes={}",
				this.getPath(),
				maxLength,
				buf.length,
				this.seek(0, SEEK_CUR),
				readBytes);
		return buf;
	}
		
	public byte[] read() throws IOException {
		long sizeNeeded = getSize() - pos();
		if(sizeNeeded > Integer.MAX_VALUE) throw new IndexOutOfBoundsException();
		return read((int) sizeNeeded);
	}
	
	public long pos() throws IOException {
		return seek(0, SEEK_CUR);
	}
	
	public void write(byte[] data) throws IOException {
		write(data, 0, data.length);
	}
	
	public abstract void write(byte[] data, int offset, int length) throws IOException;
	public abstract long seek(long pos, int mode) throws IOException;
	public abstract void flush() throws IOException;
	public abstract void close() throws IOException;
	
	public abstract void copy(File file) throws IOException;
	public abstract void rewind() throws IOException;
	public abstract boolean hasData() throws IOException;
	
	public abstract int available() throws IOException;
	public FS getFs() { return fs; }
}
