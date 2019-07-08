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
	protected ZKFS zkfs;
	/** filesystem to which this file belongs */
	protected Inode inode;
	/** inode describing metadata of this file */
	protected long offset;
	/** current location of file pointer (offset in bytes into file) */
	protected Long overrideMtime;
	/**
	 * Time to set as mtime on next flush (set to null to use current time). Reset
	 * to null after flush().
	 */
	protected String path;
	/** path from which the file was opened */
	protected PageTree tree;
	/** Tree of page hashes, used to locate/validate page contents. */
	protected int mode;
	/** file access mode bitmask */
	protected Page bufferedPage;
	/** buffered page contents (page for current file pointer offset) */
	protected boolean dirty;
	/** true if file has been modified since last flush */
	protected boolean trusted;
	/** verify page signatures when reading <=> !trusted */
	protected boolean closed, closing;
	/** is this file closed? */
	protected long pendingSize;
	/** size to be set on next commit */
	protected int retainCount = 0;

	public final static int O_LINK_LITERAL = 1 << 16;
	/** treat symlinks as literal files, needed for lowlevel symlink operations */

	protected Logger logger = LoggerFactory.getLogger(ZKFile.class);

	protected ZKFile(ZKFS fs) {
		super(fs);
		retain();
		this.zkfs = fs;
		this.trusted = true; // set to false for inode table
		logger.trace("ZKFS {} {}: open {} - (0x{}), {} open",
				Util.formatArchiveId(fs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(fs.baseRevision), path, Integer.toHexString(mode), fs.getOpenFiles().size());
	}

	/** Open a file handle at a path */
	public ZKFile(ZKFS fs, String path, int mode, boolean trusted) throws IOException {
		super(fs);
		retain();
		logger.trace("ZKFS {} {}: open {} - (0x{}), {} open",
				Util.formatArchiveId(fs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(fs.baseRevision), path, Integer.toHexString(mode), fs.getOpenFiles().size());
		this.zkfs = fs;
		try {
			this.path = path;
			Inode inode;

			try {
				inode = fs.inodeForPath(path, (mode & O_NOFOLLOW) == 0);
			} catch (ENOENTException exc) {
				if ((mode & O_CREAT) == 0)
					throw exc;
				inode = fs.create(path);
			}

			initWithInode(fs, inode, mode, trusted);
		} catch (Throwable exc) {
			this.close();
			throw exc;
		}
	}

	public ZKFile(ZKFS fs, Inode inode, int mode, boolean trusted) throws IOException {
		super(fs);
		retain();
		this.zkfs = fs;
		logger.trace("ZKFS {} {}: open inode {} - (0x{}), {} open",
				Util.formatArchiveId(fs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(fs.baseRevision), inode.getStat().getInodeId(), Integer.toHexString(mode),
				fs.getOpenFiles().size());
		try {
			this.path = "(inode " + inode.getStat().getInodeId() + ")";
			initWithInode(fs, inode, mode, trusted);
		} catch (Throwable exc) {
			this.close();
			throw exc;
		}
	}

	protected void initWithInode(ZKFS fs, Inode inode, int mode, boolean trusted) throws IOException {
		try {
			this.zkfs = fs;
			this.mode = mode;
			this.trusted = trusted;
			this.inode = inode;
			this.pendingSize = inode.getStat().getSize();

			if ((mode & (O_NOFOLLOW | O_LINK_LITERAL)) == O_LINK_LITERAL) {
				throw new EINVALException("O_LINK_LITERAL not valid without O_NOFOLLOW");
			}

			if (this.inode.getStat().isSymlink() && (mode & O_LINK_LITERAL) == 0) {
				throw new EMLINKException(path);
			}

			logger.trace("ZKFS {} {}: Loading path {} from inode {} {}",
					Util.formatArchiveId(fs.getArchive().getConfig().getArchiveId()),
					Util.formatRevisionTag(fs.getBaseRevision()), path, inode.getStat().getInodeId(),
					Util.formatRefTag(inode.getRefTag()));

			this.tree = new PageTree(this.inode);
			if ((mode & O_WRONLY) != 0) {
				zkfs.assertWritable(path);
				if (zkfs.archive.config.isReadOnly())
					throw new EACCESException("cannot open files with write access when archive is opened read-only");
			}
			if ((mode & O_TRUNC) != 0)
				truncate(0);
			if ((mode & O_APPEND) != 0)
				offset = pendingSize;

			zkfs.addOpenFile(this);
		} catch (Throwable exc) {
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

	/**
	 * Updates the page tree reference to a specific page number. Updates inode
	 * reftag automatically.
	 */
	protected void setPageTag(int pageNum, byte[] hash) throws IOException {
		assertWritable();
		tree.setPageTag(pageNum, hash);
	}

	/**
	 * Obtain page tree reference for a specific page number.
	 * 
	 * @throws IOException
	 */
	protected byte[] getPageTag(int pageNum) throws IOException {
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
	public long getSize() {
		return pendingSize;
	}

	@Override
	public synchronized void truncate(long size) throws IOException {
		assertWritable();
		if (size == pendingSize)
			return;

		if (size > pendingSize) {
			long oldOffset = offset;
			seek(0, SEEK_END);
			while (size > pendingSize) {
				byte[] zeros = new byte[(int) Math.min(zkfs.archive.config.pageSize, size - pendingSize)];
				write(zeros);
			}
			seek(oldOffset, SEEK_SET);
		} else {
			int newPageCount = (int) Math.ceil((double) size / zkfs.archive.config.pageSize);
			long oldPageCount = tree.numPages;
			tree.resize(newPageCount);
			for (long i = newPageCount; i < Math.min(oldPageCount, tree.maxNumPages); i++) {
				tree.setPageTag(i, new byte[zkfs.archive.crypto.hashLength()]);
			}

			tree.setNumPages(newPageCount);
			pendingSize = size;
			if (offset >= size) {
				offset = size;
			}

			if (size % zkfs.archive.config.pageSize > 0 || size == 0) {
				int lastPage = (int) (size / zkfs.archive.config.pageSize);
				bufferPage(lastPage);
				bufferedPage.truncate((int) (size % zkfs.archive.config.pageSize));
			}
		}

		dirty = true;
	}

	@Override
	public synchronized int read(byte[] buf, int bufOffset, int maxLength) throws IOException {
		assertReadable();
		if (bufOffset < 0 || maxLength > buf.length - bufOffset)
			throw new IndexOutOfBoundsException();

		int numToRead = (int) Math.min(maxLength, pendingSize - offset), readLen = Math.max(0, numToRead);
		logger.trace("ZKFS {}: (ZKFile READ 1) {}, numToRead={}, readLen={}, offset={}, |buf|={}",
				Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()), path, numToRead, readLen, offset,
				buf.length);
		if (numToRead == 0)
			return -1;

		while (numToRead > 0) {
			int neededPageNum = (int) (offset / zkfs.archive.config.pageSize);
			if (neededPageNum < 0)
				throw new IndexOutOfBoundsException();
			bufferPage(neededPageNum);
			bufferedPage.seek((int) (offset % zkfs.archive.config.pageSize));
			int numRead = bufferedPage.read(buf, bufOffset + readLen - numToRead, numToRead);
			logger.trace("ZKFS {} {}: (ZKFile READ 2) {} page {}, numRead={}, bufOffset={}",
					Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
					Util.formatRevisionTag(zkfs.baseRevision), path, neededPageNum, numRead,
					bufOffset + readLen - numToRead);
			if (numRead == 0)
				return readLen - numToRead;
			numToRead -= numRead;
			offset += numRead;
		}

		return readLen;
	}

	/**
	 * Load a given page into the buffer, writing out the currently buffered page if
	 * one exists and is dirty
	 */
	protected synchronized void bufferPage(int pageNum) throws IOException {
		if (bufferedPage != null) {
			if (bufferedPage.pageNum == pageNum)
				return;
			bufferedPage.flush();
		}

		bufferedPage = new Page(this, pageNum);
		if (tree.hasTag(pageNum) && pageNum < tree.numPages) {
			logger.trace("ZKFS {} {}: {} buffering pre-existing page {}",
					Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
					Util.formatRevisionTag(zkfs.baseRevision), this.getPath(), pageNum);
			bufferedPage.load();
		} else {
			logger.trace("ZKFS {} {}: {} buffering blank page {}",
					Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
					Util.formatRevisionTag(zkfs.baseRevision), this.getPath(), pageNum);
			bufferedPage.blank();
		}
	}

	@Override
	public void write(byte[] data) throws IOException {
		write(data, 0, data.length);
	}

	/**
	 * Writes data into the file at the current pointer location.
	 * 
	 * @param data      byte array containing data to be written
	 * @param bufOffset offset into buffer of first byte to be written
	 * @param length    number of bytes to write
	 * @throws IOException
	 */
	@Override
	public synchronized void write(byte[] data, int bufOffset, int length) throws IOException {
		assertWritable();

		dirty = true;
		int leftToWrite = length;
		int pageSize = zkfs.archive.config.pageSize;

		while (leftToWrite > 0) {
			int neededPageNum = (int) (this.offset / pageSize);
			bufferPage(neededPageNum);
			int offsetInPage = (int) (offset % zkfs.archive.config.pageSize),
					pageCapacity = (int) (pageSize - offsetInPage);
			bufferedPage.seek(offsetInPage);
			int numWritten = bufferedPage.write(data, bufOffset + length - leftToWrite,
					Math.min(leftToWrite, pageCapacity));

			leftToWrite -= numWritten;
			this.offset += numWritten;
			if (this.offset > pendingSize) {
				pendingSize = this.offset;
			}
		}
	}

	@Override
	public boolean hasData() throws IOException {
		assertReadable();
		return offset < pendingSize;
	}

	@Override
	public void rewind() {
		offset = 0;
	}

	@Override
	public synchronized long seek(long pos, int mode) {
		long newOffset = -1;

		switch (mode) {
		case SEEK_SET:
			newOffset = pos;
			break;
		case SEEK_CUR:
			newOffset = offset + pos;
			break;
		case SEEK_END:
			newOffset = pendingSize;
			break;
		}

		if (newOffset < 0)
			throw new IllegalArgumentException();

		logger.trace("ZKFS {} {}: {} seek pos={} mode={} offset={} newOffset={}",
				Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(zkfs.baseRevision), this.getPath(), pos, mode, offset, newOffset);
		return offset = newOffset;
	}

	@Override
	public void flush() throws IOException {
		logger.trace("ZKFS {} {}: flush {}, dirty={}, closed={}",
				Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(zkfs.getBaseRevision()), path, dirty, closed);
		if (!dirty || closed)
			return;

		zkfs.lockedOperation(() -> {
			synchronized (this) {
				if (!dirty || closed)
					return null;
				long now = Util.currentTimeNanos();
				long mtime = overrideMtime == null ? now : overrideMtime;
				inode.getStat().setMtime(mtime);
				inode.setChangedFrom(zkfs.baseRevision);
				inode.setModifiedTime(mtime);
				if (bufferedPage != null) {
					bufferedPage.flush();
				}
				inode.setRefTag(tree.commit());
				inode.getStat().setSize(pendingSize);
				dirty = false;
				zkfs.inodeTable.setInode(inode);
				zkfs.markDirty();
				setOverrideMtime(null);

				return null;
			}
		});

		logger.trace("ZKFS {} {}: flush {} complete, new reftag={}",
				Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(zkfs.getBaseRevision()), path, Util.formatRefTag(inode.getRefTag()));
	}

	public synchronized ZKFile retain() {
		retainCount++;
		return this;
	}

	@Override
	public void close() throws IOException {
		if (closed || closing)
			return;

		synchronized (this) {
			retainCount--;
			if (retainCount != 0 || closing)
				return;
			closing = true;
		}

		forceClose();
	}

	public void forceClose() throws IOException {
		if (closed)
			return;
		zkfs.lockedOperation(() -> {
			synchronized (this) {
				if (closed)
					return null;
				logger.trace("ZKFS {} {}: close {}, {} open",
						Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
						Util.formatRevisionTag(zkfs.baseRevision), path, fs.getOpenFiles().size());
				flush();
				zkfs.removeOpenFile(this);
				fs.reportClosedFile(this);

				return null;
			}
		});
	}

	@Override
	public void copy(File file) throws IOException {
		assertWritable();
		int pageSize = zkfs.archive.config.pageSize;

		synchronized (this) {
			file.rewind();
			while (file.hasData())
				write(file.read(pageSize));
		}

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
		if (bufferedPage == null)
			return 0;
		return bufferedPage.remaining();
	}

	/**
	 * Throw an exception if the file is not open for reading.
	 * 
	 * @throws IOException
	 */
	protected void assertReadable() throws IOException {
		if ((mode & File.O_RDONLY) == 0)
			throw new EACCESException("File is not opened for reading");
	}

	/**
	 * Throw an exception if the file is not open for writing.
	 * 
	 * @throws IOException
	 */
	protected void assertWritable() throws IOException {
		if ((mode & File.O_WRONLY) == 0)
			throw new EACCESException("File is not opened for writing");
	}

	/**
	 * Throw an exception with an error message if the boolean condition is false.
	 */
	protected void assertIntegrity(boolean condition, String explanation) {
		if (!condition)
			throw new InvalidArchiveException(String.format("%s %s %s (inodeId %d): %s",
					zkfs.getArchive().getMaster().getName(), Util.formatRevisionTag(zkfs.getBaseRevision()), path,
					inode.getStat().getInodeId(), explanation));
	}

	public int getRetainCount() {
		return retainCount;
	}

	public Long getOverrideMtime() {
		return overrideMtime;
	}

	public void setOverrideMtime(Long overrideMtime) {
		this.overrideMtime = overrideMtime;
	}
}
