package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Stack;

/** Lists inode IDs that are "free" (available for allocation). */
public class FreeList extends ZKFile {
	Stack<Long> available = new Stack<Long>();
	boolean dirty; /** freelist has been modified and needs to be flushed */
	long lastReadPage; /** most recent page we read (this is a fifo so we work from high pages to low) */
	public static String FREE_LIST_PATH = "(free list)";
	
	protected class FreeListExhaustedException extends RuntimeException {
		private static final long serialVersionUID = 4057105931778047274L;
	}
	
	/** initialize freelist object for filesystem */
	public FreeList(ZKFS fs) throws IOException {
		this(fs.inodeTable.inodeWithId(InodeTable.INODE_ID_FREELIST));
	}
	
	/** initialize freelist from its inode */
	public FreeList(Inode inode) throws IOException {
		/* This is instantiated automatically when we load an InodeTable, which shouldn't automatically
		 * cause further reads. So ensure this never loads any pages in the constructor. 
		 */
		super(inode.fs);
		this.fs = inode.fs;
		this.path = FREE_LIST_PATH;
		this.mode = O_RDWR;
		this.inode = inode;
		this.tree = new PageTree(this.inode);
		lastReadPage = this.inode.refTag.numPages;
	}
	
	/** returns an available inode ID and removes it from the freelist */
	public long issueInodeId() throws IOException {
		if(available.isEmpty()) loadNextPage();
		dirty = true;
		return available.pop();
	}
	
	/** adds an inode ID to the freelist. take care to check that an inode is not already deleted! */
	public void freeInodeId(long inodeId) {
		dirty = true;
		available.push(inodeId);
	}
	
	/** serialize freelist and write into zkfs */ 
	public void commit() throws IOException {
		if(!dirty) return;
		long offset = Math.max(0, (lastReadPage-1)*zkfs.archive.config.pageSize);
		truncate(offset);
		ByteBuffer buf = ByteBuffer.allocate(8*available.size());
		for(Long inodeId : available) buf.putLong(inodeId);
		seek(offset, SEEK_SET);
		write(buf.array());
		flush();
		
		lastReadPage = inode.refTag.numPages-1;
		available.clear();
		buf.position(buf.capacity() - (int) (buf.capacity() % zkfs.archive.config.pageSize)); // last page
		while(buf.remaining() >= 8) available.push(buf.getLong());
		dirty = false;
	}
	
	/** read the next page of inode IDs */
	protected void loadNextPage() throws IOException {
		if(lastReadPage <= 0) throw new FreeListExhaustedException();
		readPage(--lastReadPage);
	}
	
	/** read a specific page of inode IDs */
	protected void readPage(long pageNum) throws IOException {
		seek(pageNum*zkfs.archive.config.pageSize, SEEK_SET);
		ByteBuffer buf = ByteBuffer.wrap(read((int) zkfs.archive.config.pageSize));
		while(buf.remaining() >= 8) available.push(buf.getLong()); // 8 == sizeof inodeId 
	}
}
