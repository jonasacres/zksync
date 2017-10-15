package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class ZKFile extends File {
	protected ZKFS fs;
	protected Inode inode;
	protected long offset;
	protected String path;
	protected PageMerkel merkel;
	protected int mode;
	protected Page bufferedPage;
	protected boolean dirty;
	
	protected ZKFile() {}
		
	public ZKFile(ZKFS fs, String path, int mode) throws IOException {
		this.fs = fs;
		this.path = path;
		this.mode = mode;
		
		try {
			this.inode = fs.inodeForPath(path, (mode & O_NOFOLLOW) == 0);
		} catch(ENOENTException e) {
			this.inode = fs.create(path);
		}

		this.merkel = new PageMerkel(fs, this.inode);
}
	
	public ZKFS getFS() {
		return fs;
	}
	
	public Inode getInode() {
		return inode;
	}
	
	public void setPageTag(int pageNum, byte[] hash) {
		assertWritable();
		merkel.setPageTag(pageNum, hash);
	}
	
	public byte[] getPageTag(int pageNum) {
		return merkel.getPageTag(pageNum);
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public Stat getStat() {
		return inode.getStat();
	}

	@Override
	public void truncate(long size) {
		assertWritable();
		inode.getStat().setSize(size);
		if(offset >= size) offset = size-1;
	}

	@Override
	public int read(byte[] buf, int bufOffset, int maxLength) throws IOException {
		int numToRead = (int) Math.min(maxLength, getStat().getSize()-offset), readLen = numToRead;
		while(numToRead > 0) {
			int neededPageNum = (int) (offset/fs.getPrivConfig().getPageSize());
			bufferPage(neededPageNum);
			bufferedPage.seek((int) (offset % fs.getPrivConfig().getPageSize()));
			int numRead = bufferedPage.read(buf, bufOffset + readLen - numToRead, numToRead);
			numToRead -= numRead;
			offset += numRead;
		}
		
		return readLen;
	}
	
	protected void bufferPage(int pageNum) throws IOException {
		if(bufferedPage != null) {
			if(bufferedPage.pageNum == pageNum) return;
			bufferedPage.flush();
		}
		bufferedPage = new Page(this, pageNum);
	}
	
	@Override
	public void write(byte[] data) throws IOException {
		write(data, 0, data.length);
	}
	
	public void write(byte[] data, int bufOffset, int length) throws IOException {
		dirty = true;
		int leftToWrite = length;
		
		while(leftToWrite > 0) {
			int neededPageNum = (int) (this.offset/fs.getPrivConfig().getPageSize());
			bufferPage(neededPageNum);
			int numWritten = bufferedPage.write(data, bufOffset + length - leftToWrite, leftToWrite);
			
			leftToWrite -= numWritten;
			bufOffset += numWritten;
			this.offset += numWritten;
			if(this.offset > getStat().getSize()) getStat().setSize(this.offset);
		}
	}
	
	protected void calculateRefType() throws IOException {
		if(inode.getStat().getSize() <= fs.getPrivConfig().getImmediateThreshold()) {
			inode.setRefType(Inode.REF_TYPE_IMMEDIATE);
		} else if(inode.getStat().getSize() <= fs.getPrivConfig().getPageSize()) {
			inode.setRefType(Inode.REF_TYPE_INDIRECT);
		} else {
			inode.setRefType(Inode.REF_TYPE_2INDIRECT);
		}
		
		inode.setRefTag(merkel.getMerkelTag());
	}
	
	@Override
	public boolean hasData() {
		assertReadable();
		return offset < inode.getStat().getSize();
	}
	
	@Override
	public void rewind() {
		offset = 0;
	}

	@Override
	public long seek(long pos, int mode) {
		long newOffset = -1;
		
		switch(mode) {
		case SEEK_SET:
			newOffset = pos;
			break;
		case SEEK_CUR:
			newOffset = offset + pos;
			break;
		case SEEK_END:
			newOffset = inode.getStat().getSize();
			break;
		}
		
		if(newOffset < 0) throw new IllegalArgumentException();
		return offset = newOffset;
	}
	
	@Override
	public void flush() throws IOException {
		if(!dirty) return;
		inode.getStat().setMtime(System.currentTimeMillis() * 1000l * 1000l);
		bufferedPage.flush();
		calculateRefType();
	}

	@Override
	public void close() throws IOException {
		flush();
	}

	@Override
	public void copy(File file) throws IOException {
		assertWritable();
		int pageSize = fs.getPrivConfig().getPageSize();
		while(file.hasData()) write(file.read(pageSize));
		
		inode.getStat().setAtime(file.getStat().getAtime());
		inode.getStat().setCtime(file.getStat().getCtime());
		inode.getStat().setMtime(file.getStat().getMtime());
		inode.getStat().setGroup(file.getStat().getGroup());
		inode.getStat().setGid(file.getStat().getGid());
		inode.getStat().setUser(file.getStat().getUser());
		inode.getStat().setUid(file.getStat().getUid());
		inode.getStat().setMode(file.getStat().getMode());
	}
	
	private void assertReadable() {
		if((mode & File.O_RDONLY) == 0) throw new RuntimeException("File is not opened for reading");
	}
	
	private void assertWritable() {
		if((mode & File.O_WRONLY) == 0) throw new RuntimeException("File is not opened for writing");
	}
}
