package com.acrescrypto.zksync.fs.ramfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.exceptions.EACCESException;
import com.acrescrypto.zksync.exceptions.EISDIRException;
import com.acrescrypto.zksync.exceptions.EMLINKException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.ramfs.RAMFS.Inode;

public class RAMFile extends File {
	String path;
	Inode inode;
	RAMFS fs;
	ByteBuffer data;
	int mode;

	protected RAMFile(RAMFS fs, String path, int mode) throws EMLINKException, IOException {
		super(fs);
		this.fs = fs;
		this.path = path;
		this.mode = mode;

		try {
			if((mode & O_NOFOLLOW) != 0 && fs.lstat(path).isSymlink()) throw new EMLINKException(path);
			if((mode & O_NOFOLLOW) == 0) {
				this.inode = fs.lookup(path);
			} else {
				this.inode = fs.llookup(path);
			}
		} catch(ENOENTException exc) {
			if((mode & O_CREAT) == 0) throw exc;
			else this.inode = fs.makeInode(path, (inode)->{});
		}
		
		if(inode.stat.isDirectory()) throw new EISDIRException(path);
		
		if((mode & O_TRUNC) != 0) {
			inode.data = new byte[0];
		}
		
		data = ByteBuffer.wrap(inode.data);
		if((mode & O_APPEND) != 0) {
			data.position(data.limit());
		}
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public Stat getStat() throws IOException {
		return inode.stat;
	}

	@Override
	public void truncate(long size) throws IOException {
		assertWritable();
		fs.truncate(path, size);
		resync();
	}

	@Override
	public int read(byte[] buf, int offset, int maxLength) throws IOException {
		assertReadable();
		int readLen = Math.min(maxLength, data.remaining());
		data.get(buf, offset, readLen);
		return readLen;
	}

	@Override
	public void write(byte[] buf, int offset, int length) throws IOException {
		assertWritable();
		if(data.remaining() < length) {
			ByteBuffer newData = ByteBuffer.allocate(data.position() + length);
			data.limit(data.position());
			data.rewind();
			newData.put(data);
			data = newData;
		}
		
		flush();
		
		data.put(buf, offset, length);
	}

	@Override
	public long seek(long pos, int mode) throws IOException {
		switch(mode) {
		case File.SEEK_CUR:
			data.position((int) (data.position() + pos));
			break;
		case File.SEEK_SET:
			data.position((int) pos);
			break;
		case File.SEEK_END:
			int newPos = (int) (data.limit() + pos);
			truncate(newPos);
			data.position(newPos);
			break;
		}

		return data.position();
	}

	@Override
	public void flush() throws IOException {
		inode.data = data.array();
		inode.stat.setSize(data.limit());
	}

	@Override
	public void close() throws IOException {
		flush();
	}

	@Override
	public void copy(File file) throws IOException {
		file.rewind();
		inode.data = file.read();
		inode.stat.setSize(inode.data.length);
		data = ByteBuffer.wrap(inode.data);
	}

	@Override
	public void rewind() throws IOException {
		data.rewind();
	}

	@Override
	public boolean hasData() throws IOException {
		return data.hasRemaining();
	}

	@Override
	public int available() throws IOException {
		return data.remaining();
	}
	
	protected void resync() {
		data = ByteBuffer.wrap(inode.data);
	}
	
	protected void assertWritable() throws EACCESException {
		if((mode & File.O_WRONLY) == 0) throw new EACCESException(path);
	}
	
	protected void assertReadable() throws EACCESException {
		if((mode & File.O_RDONLY) == 0) throw new EACCESException(path);
	}
}
