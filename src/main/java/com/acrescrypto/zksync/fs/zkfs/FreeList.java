package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;

import com.acrescrypto.zksync.utility.Util;

/** Lists inode IDs that are "free" (available for allocation). */
public class FreeList extends ZKFile {
	LinkedList<Long> available = new LinkedList<Long>();
	long lastReadPage; /** most recent page we read (this is a fifo so we work from high pages to low) */
	public static String FREE_LIST_PATH = "(free list)";
	
	protected class FreeListExhaustedException extends RuntimeException {
		private static final long serialVersionUID = 4057105931778047274L;
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
		this.pendingSize = inode.getStat().getSize();
		lastReadPage = this.inode.refTag.numPages;
	}
	
	/** Empty the freelist completely. Intended for use in rebuilding freelist manually. 
	 * @throws IOException */
	public synchronized void clearList() throws IOException {
		truncate(0l);
		available.clear();
	}
	
	/** returns an available inode ID and removes it from the freelist. The inode ID could already have been
	 * reissued by other means, so the caller must ensure that the issued inode ID is not already in use! */
	public synchronized long issueInodeId() throws IOException {
		if(available.isEmpty()) loadNextPage();
		dirty = true;
		if(available.isEmpty()) throw new FreeListExhaustedException();
		return available.pop();
	}
	
	/** adds an inode ID to the freelist. take care to check that an inode is not already deleted! */
	public synchronized void freeInodeId(long inodeId) {
		dirty = true;
		available.push(inodeId);
	}
	
	/** serialize freelist and write into zkfs */ 
	public synchronized void commit() throws IOException {
		if(!dirty) return;
		long offset = Math.max(0, (lastReadPage-1)*zkfs.archive.config.pageSize);
		truncate(offset);
		ByteBuffer buf = ByteBuffer.allocate(8*available.size());
		while(!available.isEmpty()) {
			buf.putLong(available.pollLast());
		}
	
		seek(offset, SEEK_SET);
		write(buf.array());
		flush();
		
		lastReadPage = inode.refTag.numPages-1;
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

	/** warning: only checks cached inode IDs, might return false even if inodeId is in uncached portion
	 * of list
	 */
	public synchronized boolean contains(long inodeId) {
		return available.contains(inodeId);
	}
	
	public synchronized Collection<Long> allEntries() throws IOException {
		LinkedList<Long> entries = new LinkedList<>();
		long maxOffset = Math.min(this.getSize(), zkfs.getArchive().getConfig().getPageSize() * lastReadPage);
		long oldPos = pos();
		seek(0, SEEK_SET);
		
		while(this.offset < maxOffset) {
			byte[] pageData = read((int) zkfs.archive.config.pageSize);
			ByteBuffer buf = ByteBuffer.wrap(pageData);
			while(buf.remaining() >= 8) {
				entries.push(buf.getLong()); 
			}
		}
		seek(oldPos, SEEK_SET);
		
		entries.addAll(0, available);
		return entries;
	}
	
	public synchronized String dump() throws IOException {
		StringBuilder sb = new StringBuilder(String.format("FreeList %s, base revision %s, size %d, dirty=%s\n",
				zkfs.archive.master.getName(),
				Util.formatRevisionTag(zkfs.baseRevision),
				pendingSize,
				dirty ? "true" : "false"
				));
		
		int index = 0;
		for(long inodeId : allEntries()) {
			sb.append(String.format("\t#%4d: inodeId %4d\n",
					++index,
					inodeId));
		}
		
		return sb.toString();
	}
}
