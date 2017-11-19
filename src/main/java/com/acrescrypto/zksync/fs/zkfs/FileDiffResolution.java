package com.acrescrypto.zksync.fs.zkfs;

import java.util.ArrayList;

public class FileDiffResolution {
	long inodeId;
	Inode inode;
	ArrayList<RefTag> revisions;
	
	public FileDiffResolution(long inodeId, ArrayList<RefTag> revisions) {
		this.inodeId = inodeId;
		this.revisions = revisions;
	}
	
	public FileDiffResolution(Inode inode, ArrayList<RefTag> revisions) {
		this.inode = inode;
		this.inodeId = inode.getStat().getInodeId();
		this.revisions = revisions;
	}
	
	public ArrayList<RefTag> getRevisions() {
		return revisions;
	}
	
	public Inode getInode() {
		return inode;
	}
	
	public long getInodeId() {
		return inodeId;
	}
}
