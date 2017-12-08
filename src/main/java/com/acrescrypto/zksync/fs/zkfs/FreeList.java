package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Stack;

public class FreeList extends ZKFile {
	Stack<Long> available = new Stack<Long>();
	boolean dirty;
	long lastReadPage;
	public static String FREE_LIST_PATH = "(free list)";
	
	protected class FreeListExhaustedException extends RuntimeException {
		private static final long serialVersionUID = 4057105931778047274L;
	}
	
	public FreeList(ZKFS fs) throws IOException {
		this(fs.inodeTable.inodeWithId(InodeTable.INODE_ID_FREELIST));
	}
	
	public FreeList(Inode inode) throws IOException {
		this.fs = inode.fs;
		this.path = FREE_LIST_PATH;
		this.mode = O_RDWR;
		this.inode = inode;
		this.merkle = new PageMerkle(this.inode.getRefTag());
		lastReadPage = this.inode.refTag.numPages;
	}
	
	public long issueInodeId() throws IOException {
		throw new FreeListExhaustedException();
//		if(available.isEmpty()) loadNextPage();
//		dirty = true;
//		return available.pop();
	}
	
	public void freeInodeId(long inodeId) {
		dirty = true;
		available.push(inodeId);
	}
	
	public void commit() throws IOException {
		if(!dirty) return;
		truncate(Math.max(0, (lastReadPage-1)*fs.archive.privConfig.getPageSize()));
		ByteBuffer buf = ByteBuffer.allocate(8*available.size());
		for(Long inodeId : available) buf.putLong(inodeId);
		write(buf.array());
		flush();
		
		lastReadPage = inode.refTag.numPages-1;
		available.clear();
		buf.position(buf.capacity() - (buf.capacity() % fs.archive.privConfig.getPageSize())); // last page
		while(buf.remaining() > 8) available.push(buf.getLong());
		dirty = false;
	}
	
	protected void loadNextPage() throws IOException {
		if(lastReadPage <= 0) throw new FreeListExhaustedException();
		readPage(--lastReadPage);
	}
	
	protected void readPage(long pageNum) throws IOException {
		seek(pageNum*fs.archive.privConfig.getPageSize(), SEEK_SET);
		ByteBuffer buf = ByteBuffer.wrap(read(fs.archive.privConfig.getPageSize()));
		while(buf.remaining() > 8) available.push(buf.getLong()); // 8 == sizeof inodeId 
	}
}
