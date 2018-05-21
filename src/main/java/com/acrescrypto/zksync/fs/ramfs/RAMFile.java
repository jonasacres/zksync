package com.acrescrypto.zksync.fs.ramfs;

import java.io.IOException;
import java.util.ArrayList;

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
	DataBacking data;
	int mode;
	
	class DataBacking {
		int pageSize = 65536;
		ArrayList<byte[]> pages;
		long size, pos;
		
		public DataBacking(byte[] initial) {
			pages = new ArrayList<byte[]>();
			write(initial, 0, initial.length);
			pos = 0;
		}
		
		int read(byte[] buf, int offset, int length) {
			int numRead = 0;
			while(numRead < length) {
				int pageOffset = (int) (pos % pageSize);
				int pageRemaining = pageSize - pageOffset;
				int readAmount = Math.min(pageRemaining, length-numRead);
				
				System.arraycopy(currentPage(), pageOffset, buf, offset + numRead, readAmount);
				
				pos += readAmount;
				numRead += readAmount;
			}
			
			return numRead;
		}
		
		void write(byte[] buf, int offset, int length) {
			int numWritten = 0;
			
			while(numWritten < length) {
				int pageOffset = (int) (pos % pageSize);
				int pageRemaining = pageSize - pageOffset;
				int writeAmount = Math.min(pageRemaining, length-numWritten);
				
				System.arraycopy(buf, offset + numWritten, currentPage(), pageOffset, writeAmount);
				
				pos += writeAmount;
				numWritten += writeAmount;
			}
			
			if(size < pos) size = pos;
		}
		
		byte[] currentPage() {
			try {
				return pages.get((int) pos/pageSize);
			} catch(IndexOutOfBoundsException exc) {
				pages.add(new byte[pageSize]);
				return pages.get((int) pos/pageSize);
			}
		}
		
		long remaining() {
			return size - pos;
		}
		
		byte[] toArray() {
			byte[] array = new byte[(int) size];
			long oldPos = pos;
			pos = 0;
			read(array, 0, (int) size);
			pos = oldPos;
			return array;
		}
	}

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
		
		resync();
		if((mode & O_APPEND) != 0) {
			data.pos = data.size;
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
	public byte[] read() throws IOException {
		assertReadable();
		data.pos = data.size;
		return data.toArray();
	}

	@Override
	public int read(byte[] buf, int offset, int maxLength) throws IOException {
		assertReadable();
		int readLen = Math.min(maxLength, (int) data.remaining());
		data.read(buf, offset, readLen);
		return readLen;
	}

	@Override
	public void write(byte[] buf, int offset, int length) throws IOException {
		assertWritable();
		data.write(buf, offset, length);
	}

	@Override
	public long seek(long pos, int mode) throws IOException {
		switch(mode) {
		case File.SEEK_CUR:
			data.pos = data.pos + pos;
			break;
		case File.SEEK_SET:
			data.pos = pos;
			break;
		case File.SEEK_END:
			long newPos = data.size + pos;
			truncate(newPos);
			data.pos = newPos;
			break;
		}

		return data.pos;
	}

	@Override
	public void flush() throws IOException {
		inode.data = data.toArray();
		inode.stat.setSize(data.size);
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
		resync();
	}

	@Override
	public void rewind() throws IOException {
		data.pos = 0;
	}

	@Override
	public boolean hasData() throws IOException {
		return data.remaining() > 0;
	}

	@Override
	public int available() throws IOException {
		return (int) data.remaining();
	}
	
	protected void resync() {
		data = new DataBacking(inode.data);
	}
	
	protected void assertWritable() throws EACCESException {
		if((mode & File.O_WRONLY) == 0) throw new EACCESException(path);
	}
	
	protected void assertReadable() throws EACCESException {
		if((mode & File.O_RDONLY) == 0) throw new EACCESException(path);
	}
}
