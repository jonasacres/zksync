package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

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
	
	HashMap<Integer, Page> pageCache;
		
	public ZKFile(ZKFS fs, String path, int mode) throws IOException {
		fs.assertPathIsDirectory(fs.dirname(path));
		this.fs = fs;
		this.path = path;
		this.mode = mode;
		
		try {
			this.inode = fs.inodeForPath(path);
		} catch(ENOENTException e) {
			this.inode = new Inode(fs);
		}
	}
	
	public ZKFS getFS() {
		return fs;
	}
	
	public Inode getInode() {
		return inode;
	}
	
	public Page getPage(int pageNum) {
		if(!pageCache.containsKey(pageNum)) {
			pageCache.put(pageNum, new Page(this, pageNum));
		}
		
		return pageCache.get(pageNum);
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
	public int read(byte[] buf, int offset, int maxLength) throws IOException {
		assertReadable();
		ByteBuffer readBuf = ByteBuffer.wrap(buf);
		readBuf.position(offset);
		
		int len = (int) Math.min(buf.length - offset, inode.getStat().getSize() - this.offset), readBytes = len;
		len = (int) Math.min(len, maxLength);
		
		while(len > 0) {
			int offsetInPage = (int) (this.offset % fs.getPrivConfig().getPageSize());
			int bytesToRead = Math.min(len, (int) (fs.getPrivConfig().getPageSize() - offsetInPage));
			int pageNum = (int) (this.offset/fs.getPrivConfig().getPageSize());
			Page page = getPage(pageNum);
			
			readBuf.put(page.read(), offsetInPage, bytesToRead);
			len -= bytesToRead;
			this.offset += bytesToRead;
		}
		
		return readBytes;
	}
	
	@Override
	public void write(byte[] data) throws IOException {
		assertWritable();
		int dOffset = 0;
		
		ByteBuffer writeBuf = ByteBuffer.allocate((int) fs.getPrivConfig().getPageSize());
		while(dOffset < data.length) {
			int offsetInPage = (int) (offset % fs.getPrivConfig().getPageSize());
			int bytesToWrite = Math.min(data.length - dOffset, (int) (fs.getPrivConfig().getPageSize() - offsetInPage));
			int pageNum = (int) (offset/fs.getPrivConfig().getPageSize());
			Page page = getPage(pageNum);
			
			writeBuf.reset();
			writeBuf.put(data, dOffset, bytesToWrite);
			
			if(offsetInPage > 0) {
				page.append(writeBuf.array(), offsetInPage);
				page.finalize();
			} else {
				page.setPlaintext(writeBuf.array());
			}
			
			pageCache.remove(pageNum); // cache is in sync with storage, so evict cache from memory
			
			offset += bytesToWrite;
			dOffset += bytesToWrite;
		}
		
		if(offset > inode.getStat().getSize()) inode.getStat().setSize(offset);
	}
	
	private void calculateRefType() throws IOException {
		assertWritable();
		long fileSize = inode.getStat().getSize();
		if(fileSize <= fs.getPrivConfig().getImmediateThreshold()) {
			inode.setRefType(Inode.REF_TYPE_IMMEDIATE);
			inode.setRefTag(read());
		} else if(fileSize <= fs.getPrivConfig().getPageSize()) {
			inode.setRefType(Inode.REF_TYPE_INDIRECT);
			inode.setRefTag(merkel.getPageTag(0));
		} else {
			if(inode.getRefTag().equals(merkel.getMerkelTag())) return;
			merkel.commit();
			inode.setRefType(Inode.REF_TYPE_2INDIRECT);
			inode.setRefTag(merkel.getMerkelTag());
		}
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
	public void seek(long pos, int mode) {
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
		offset = newOffset;
	}

	@Override
	public void close() throws IOException {
		calculateRefType();
		pageCache.clear();
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
