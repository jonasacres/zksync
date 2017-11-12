package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import com.acrescrypto.zksync.exceptions.EACCESException;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.EMLINKException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.exceptions.NonexistentPageException;
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
	
	public final static int O_LINK_LITERAL = 1 << 16; // treat symlinks as literal files
	
	public ZKFile(ZKFS fs, RefTag tag, int inodeId, int mode) throws IOException {
		this.fs = fs;
		this.path = tag.toString();
		this.mode = mode;
		this.merkel = new PageMerkel(tag);
		this.inode = new Inode(fs);
		
		inferSize(tag);
		
		if((mode & (O_NOFOLLOW | O_LINK_LITERAL)) != 0) {
			throw new EINVALException("O_LINK_LITERAL, O_NOFOLLOW not valid in direct tag access");
		}
		
		if((mode & O_TRUNC) != 0) truncate(0);
		if((mode & O_APPEND) != 0) offset = this.inode.getStat().getSize();
	}
		
	public ZKFile(ZKFS fs, String path, int mode) throws IOException {
		this.fs = fs;
		this.path = path;
		this.mode = mode;
		
		if((mode & (O_NOFOLLOW | O_LINK_LITERAL)) == O_LINK_LITERAL) {
			throw new EINVALException("O_LINK_LITERAL not valid without O_NOFOLLOW");
		}
		
		try {
			this.inode = fs.inodeForPath(path, (mode & O_NOFOLLOW) == 0);
			if(this.inode.getStat().isSymlink() && (mode & O_LINK_LITERAL) == 0) {
				throw new EMLINKException(path);
			}
		} catch(ENOENTException e) {
			if((mode & O_CREAT) == 0) throw e;
			this.inode = fs.create(path);
		}
		
		this.merkel = new PageMerkel(this.inode.getRefTag());
		if((mode & O_TRUNC) != 0) truncate(0);
		if((mode & O_APPEND) != 0) offset = this.inode.getStat().getSize();
	}
	
	public ZKFS getFS() {
		return fs;
	}
	
	public Inode getInode() {
		return inode;
	}
	
	public void setPageTag(int pageNum, byte[] hash) throws IOException {
		assertWritable();
		merkel.setPageTag(pageNum, hash);
	}
	
	public byte[] getPageTag(int pageNum) throws NonexistentPageException {
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
	public void truncate(long size) throws IOException {
		assertWritable();
		if(size == inode.getStat().getSize()) return;
		
		if(size > inode.getStat().getSize()) {
			long oldOffset = offset;
			seek(0, SEEK_END);
			while(size > inode.getStat().getSize()) {
				byte[] zeros = new byte[(int) Math.min(fs.archive.privConfig.getPageSize(), size - inode.getStat().getSize())];
				write(zeros);
			}
			seek(oldOffset, SEEK_SET);
		} else {
			int newPageCount = (int) Math.ceil((double) size/fs.archive.privConfig.getPageSize());
			merkel.resize(newPageCount);
			for(int i = newPageCount; i < merkel.numPages; i++) {
				merkel.setPageTag(i, new byte[fs.archive.crypto.hashLength()]);
			}

			inode.getStat().setSize(size);
			if(offset >= size) offset = size;
			
			int lastPage = (int) (size/fs.archive.privConfig.getPageSize());
			bufferPage(lastPage);
			bufferedPage.truncate((int) (size % fs.archive.privConfig.getPageSize()));
		}
		
		dirty = true;
	}

	@Override
	public int read(byte[] buf, int bufOffset, int maxLength) throws IOException {
		assertReadable();
		int numToRead = (int) Math.min(maxLength, getStat().getSize()-offset), readLen = numToRead;
		while(numToRead > 0) {
			int neededPageNum = (int) (offset/fs.archive.privConfig.getPageSize());
			bufferPage(neededPageNum);
			bufferedPage.seek((int) (offset % fs.archive.privConfig.getPageSize()));
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
		if(pageNum < Math.ceil(((double) inode.getStat().getSize())/fs.archive.privConfig.getPageSize())) {
			bufferedPage.load();
		} else {
			bufferedPage.blank();
		}
	}
	
	@Override
	public void write(byte[] data) throws IOException {
		write(data, 0, data.length);
	}
	
	public void write(byte[] data, int bufOffset, int length) throws IOException {
		assertWritable();
		dirty = true;
		int leftToWrite = length;
		int pageSize = fs.archive.privConfig.getPageSize();
		
		while(leftToWrite > 0) {
			int neededPageNum = (int) (this.offset/pageSize);
			bufferPage(neededPageNum);
			int offsetInPage = (int) (offset % fs.archive.privConfig.getPageSize()), pageCapacity = (int) (pageSize-offsetInPage);
			bufferedPage.seek(offsetInPage);
			int numWritten = bufferedPage.write(data, bufOffset + length - leftToWrite, Math.min(leftToWrite, pageCapacity));
			
			leftToWrite -= numWritten;
			this.offset += numWritten;
			if(this.offset > getStat().getSize()) getStat().setSize(this.offset);
		}
	}
	
	@Override
	public boolean hasData() throws IOException {
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
		inode.setChangedFrom(fs.baseRevision);
		bufferedPage.flush();
		merkel.commit();
		dirty = false;
	}

	@Override
	public void close() throws IOException {
		flush();
	}

	@Override
	public void copy(File file) throws IOException {
		assertWritable();
		int pageSize = fs.archive.privConfig.getPageSize();
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
	
	protected void inferSize(RefTag tag) throws IOException {
		bufferPage((int) (tag.numPages-1));
		inode.getStat().setSize((tag.numPages-1)*fs.archive.privConfig.getPageSize() + bufferedPage.size);
	}
	
	protected void assertReadable() throws IOException {
		if((mode & File.O_RDONLY) == 0) throw new EACCESException("File is not opened for reading");
	}
	
	protected void assertWritable() throws IOException {
		if((mode & File.O_WRONLY) == 0) throw new EACCESException("File is not opened for writing");
	}
	
	protected void assertIntegrity(boolean condition, String explanation) {
		if(!condition) throw new InvalidArchiveException(String.format("%s (inode %d): %s", path, inode.getStat().getInodeId(), explanation));
	}
}
