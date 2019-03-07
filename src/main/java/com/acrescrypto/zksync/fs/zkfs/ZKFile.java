package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.EACCESException;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.EMLINKException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.utility.Util;

/** Represents a file handle within a zkfs. */
public class ZKFile extends File {
	protected ZKFS zkfs; /** filesystem to which this file belongs */
	protected Inode inode; /** inode describing metadata of this file */
	protected long offset; /** current location of file pointer (offset in bytes into file) */
	protected String path; /** path from which the file was opened */
	protected PageTree tree; /** Tree of page hashes, used to locate/validate page contents. */
	protected int mode; /** file access mode bitmask */
	protected Page bufferedPage; /** buffered page contents (page for current file pointer offset) */
	protected boolean dirty; /** true if file has been modified since last flush */
	protected boolean trusted; /** verify page signatures when reading <=> !trusted */
	protected boolean closed; /** is this file closed? */
	
	public final static int O_LINK_LITERAL = 1 << 16; /** treat symlinks as literal files, needed for lowlevel symlink operations */
	
	protected Logger logger = LoggerFactory.getLogger(ZKFile.class);
	
	protected ZKFile(ZKFS fs) {
		super(fs);
		logger.trace("ZKFS {} {}: open {} - (0x{}), {} open",
				Util.formatArchiveId(fs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(fs.baseRevision),
				path,
				Integer.toHexString(mode),
				fs.getOpenFiles().size());
		this.zkfs = fs;
		this.trusted = true; // set to false for inode table
	}
	
	/** Open a file handle at a path */
	public ZKFile(ZKFS fs, String path, int mode, boolean trusted) throws IOException {
		super(fs);
		try {
			this.zkfs = fs;
			this.path = path;
			this.mode = mode;
			this.trusted = trusted;
			
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
			
			this.tree = new PageTree(this.inode);
			if((mode & O_WRONLY) != 0 && zkfs.archive.config.isReadOnly()) throw new EACCESException("cannot open files with write access when archive is opened read-only");
			if((mode & O_TRUNC) != 0) truncate(0);
			if((mode & O_APPEND) != 0) offset = this.inode.getStat().getSize();
		} catch(Throwable exc) {
			this.close();
			throw exc;
		}
	}
	
	/** Filesystem for this file */
	public ZKFS getFS() {
		return zkfs;
	}
	
	/** Inode for this file */
	public Inode getInode() {
		return inode;
	}
	
	/** Updates the page tree reference to a specific page number. Updates inode reftag automatically. */
	public void setPageTag(int pageNum, byte[] hash) throws IOException {
		assertWritable();
		tree.setPageTag(pageNum, hash);
	}
	
	/** Obtain page tree reference for a specific page number. 
	 * @throws IOException */
	public byte[] getPageTag(int pageNum) throws IOException {
		return tree.getPageTag(pageNum);
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
				byte[] zeros = new byte[(int) Math.min(zkfs.archive.config.pageSize, size - inode.getStat().getSize())];
				write(zeros);
			}
			seek(oldOffset, SEEK_SET);
		} else {
			int newPageCount = (int) Math.ceil((double) size/zkfs.archive.config.pageSize);
			tree.resize(newPageCount);
			for(int i = newPageCount; i < tree.maxNumPages; i++) {
				tree.setPageTag(i, new byte[zkfs.archive.crypto.hashLength()]);
			}

			inode.getStat().setSize(size);
			if(offset >= size) offset = size;
			
			int lastPage = (int) (size/zkfs.archive.config.pageSize);
			bufferPage(lastPage);
			bufferedPage.truncate((int) (size % zkfs.archive.config.pageSize));
		}
		
		dirty = true;
	}

	@Override
	public int read(byte[] buf, int bufOffset, int maxLength) throws IOException {
		assertReadable();
		if(bufOffset < 0 || maxLength > buf.length - bufOffset) throw new IndexOutOfBoundsException();
		
		int numToRead = (int) Math.min(maxLength, getStat().getSize()-offset), readLen = Math.max(0, numToRead);
		if(numToRead == 0) return -1;

		while(numToRead > 0) {
			int neededPageNum = (int) (offset/zkfs.archive.config.pageSize);
			if(neededPageNum < 0) throw new IndexOutOfBoundsException();
			bufferPage(neededPageNum);
			bufferedPage.seek((int) (offset % zkfs.archive.config.pageSize));
			int numRead = bufferedPage.read(buf, bufOffset + readLen - numToRead, numToRead);
			if(numRead == 0) return readLen;
			numToRead -= numRead;
			offset += numRead;
		}
		
		return readLen;
	}
	
	/** Load a given page into the buffer, writing out the currently buffered page if one exists and is dirty */
	protected void bufferPage(int pageNum) throws IOException {
		if(bufferedPage != null) {
			if(bufferedPage.pageNum == pageNum) return;
			bufferedPage.flush();
		}
		
		bufferedPage = new Page(this, pageNum);
		
		if(tree.hasTag(pageNum) && pageNum < tree.numPages) {
			bufferedPage.load();
		} else {
			bufferedPage.blank();
		}
	}
	
	@Override
	public void write(byte[] data) throws IOException {
		write(data, 0, data.length);
	}
	
	/** Writes data into the file at the current pointer location.
	 * 
	 * @param data byte array containing data to be written
	 * @param bufOffset offset into buffer of first byte to be written
	 * @param length number of bytes to write
	 * @throws IOException
	 */
	@Override
	public void write(byte[] data, int bufOffset, int length) throws IOException {
		assertWritable();
		dirty = true;
		int leftToWrite = length;
		int pageSize = zkfs.archive.config.pageSize;
		
		while(leftToWrite > 0) {
			int neededPageNum = (int) (this.offset/pageSize);
			bufferPage(neededPageNum);
			int offsetInPage = (int) (offset % zkfs.archive.config.pageSize), pageCapacity = (int) (pageSize-offsetInPage);
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
		long now = Util.currentTimeNanos();
		inode.getStat().setMtime(now);
		inode.setChangedFrom(zkfs.baseRevision);
		inode.setModifiedTime(now);
		if(bufferedPage != null) bufferedPage.flush();
		inode.setRefTag(tree.commit());
		zkfs.inodeTable.setInode(inode);
		zkfs.markDirty();
		dirty = false;
	}

	@Override
	public void close() throws IOException {
		if(closed) return;
		closed = true;
		
		fs.reportClosedFile(this);
		logger.trace("ZKFS {} {}: close {}, {} open",
				Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(zkfs.baseRevision),
				path,
				fs.getOpenFiles().size());
		flush();
	}

	@Override
	public void copy(File file) throws IOException {
		assertWritable();
		file.rewind();
		int pageSize = zkfs.archive.config.pageSize;
		while(file.hasData()) write(file.read(pageSize));
		flush();
		
		inode.getStat().setAtime(file.getStat().getAtime());
		inode.getStat().setCtime(file.getStat().getCtime());
		inode.getStat().setMtime(file.getStat().getMtime());
		inode.getStat().setGroup(file.getStat().getGroup());
		inode.getStat().setGid(file.getStat().getGid());
		inode.getStat().setUser(file.getStat().getUser());
		inode.getStat().setUid(file.getStat().getUid());
		inode.getStat().setMode(file.getStat().getMode());
	}
		
	public int available() throws IOException {
		// TODO DHT: (test) coverage on this...
		if(bufferedPage == null) return 0;
		return bufferedPage.remaining();
	}
	
	/** Throw an exception if the file is not open for reading.
	 * 
	 * @throws IOException
	 */
	protected void assertReadable() throws IOException {
		if((mode & File.O_RDONLY) == 0) throw new EACCESException("File is not opened for reading");
	}
	
	/** Throw an exception if the file is not open for writing.
	 * 
	 * @throws IOException
	 */
	protected void assertWritable() throws IOException {
		if((mode & File.O_WRONLY) == 0) throw new EACCESException("File is not opened for writing");
	}
	
	/** Throw an exception with an error message if the boolean condition is false. */
	protected void assertIntegrity(boolean condition, String explanation) {
		if(!condition) throw new InvalidArchiveException(String.format("%s (inode %d): %s", path, inode.getStat().getInodeId(), explanation));
	}
}
