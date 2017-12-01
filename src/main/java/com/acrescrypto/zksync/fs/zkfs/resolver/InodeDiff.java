package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.RefTag;

public class InodeDiff {
	protected HashMap<Inode,ArrayList<RefTag>> resolutions = new HashMap<Inode,ArrayList<RefTag>>();
	protected long inodeId;
	protected boolean resolved;
	protected Inode resolution;
	
	public InodeDiff(long inodeId) {
		this.inodeId = inodeId;
	}
	
	public InodeDiff(long inodeId, RefTag[] candidates) throws IOException {
		this.inodeId = inodeId;
		for(RefTag candidate : candidates) {
			Inode inode = null;
			try {
				inode = candidate.readOnlyFS().getInodeTable().inodeWithId(inodeId);
			} catch (ENOENTException e) {}
			getResolutions().putIfAbsent(inode, new ArrayList<RefTag>());
			getResolutions().get(inode).add(candidate);
		}
	}
	
	public boolean isConflict() {
		return getResolutions().size() > 1;
	}
	
	public long getInodeId() {
		return inodeId;
	}

	public HashMap<Inode,ArrayList<RefTag>> getResolutions() {
		return resolutions;
	}
	
	public boolean isResolved() {
		return resolved;
	}
	
	public void setResolution(Inode resolution) {
		this.resolution = resolution;
		this.resolved = true;
	}
	
	public Inode getResolution() {
		return resolution;
	}
	
	public String toString() {
		return "InodeDiff " + inodeId + " (" + resolutions.size() + " versions)";
	}

	public void add(Inode newInode, ArrayList<RefTag> tags) {
		resolutions.put(newInode, tags);
	}
}
