package com.acrescrypto.zksync.fs.zkfs;

public class FileDiffResolution {
	long inodeId;
	Inode inode;
	RefTag revision;
	
	public FileDiffResolution(long inodeId, RefTag revision) {
		this.inodeId = inodeId;
		this.revision = revision;
	}
	
	public FileDiffResolution(Inode inode, RefTag revision) {
		this.inode = inode;
		this.inodeId = inode.getStat().getInodeId();
		this.revision = revision;
	}
	
	public RefTag getRevision() {
		return revision;
	}
	
	public Inode getInode() {
		return inode;
	}
	
	public long getInodeId() {
		return inodeId;
	}
}
