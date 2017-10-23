package com.acrescrypto.zksync.fs;

import java.io.Closeable;
import java.io.IOException;

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
	
	public abstract void truncate(long size) throws IOException;
	public abstract int read(byte[] buf, int offset, int maxLength) throws IOException;

	public byte[] read(int maxLength) throws IOException {
		if(maxLength <= 0) maxLength = (int) getStat().getSize();
		maxLength = (int) Math.min(maxLength, getStat().getSize());
		byte[] buf = new byte[(int) maxLength];
		read(buf, 0, (int) maxLength);
		return buf;
	}
	
	public int read(byte[] buf) throws IOException {
		return read(buf, 0, buf.length);
	}
	
	public byte[] read() throws IOException {
		long sizeNeeded = getStat().getSize() - pos();
		if(sizeNeeded > Integer.MAX_VALUE) throw new IndexOutOfBoundsException();
		return read((int) sizeNeeded);
	}
	
	public long pos() throws IOException {
		return seek(0, SEEK_CUR);
	}
	
	public abstract void write(byte[] data) throws IOException;
	public abstract long seek(long pos, int mode) throws IOException;
	public abstract void flush() throws IOException;
	public abstract void close() throws IOException;
	
	public abstract void copy(File file) throws IOException;
	public abstract void rewind();
	public abstract boolean hasData() throws IOException;
}
