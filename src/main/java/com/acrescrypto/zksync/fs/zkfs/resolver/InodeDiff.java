package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;

public class InodeDiff {
	protected HashMap<Inode,ArrayList<RevisionTag>> resolutions = new HashMap<Inode,ArrayList<RevisionTag>>();
	protected long inodeId, originalInodeId;
	protected boolean resolved;
	protected Inode resolution;
	
	public InodeDiff(long inodeId, long originalInodeId) {
		this.inodeId = inodeId;
		this.originalInodeId = originalInodeId;
	}
	
	public InodeDiff(long inodeId, RevisionTag[] candidates) throws IOException {
		this.inodeId = this.originalInodeId = inodeId;
		for(RevisionTag candidate : candidates) {
			try(ZKFS fs = candidate.readOnlyFS()) {
				Inode inode = fs.getInodeTable().inodeWithId(inodeId);
				if(inode.isDeleted()) inode = null;
				getResolutions().putIfAbsent(inode, new ArrayList<>());
				getResolutions().get(inode).add(candidate);
			}
		}
	}
	
	public boolean isConflict() {
		return getResolutions().size() > 1;
	}
	
	public long getInodeId() {
		return inodeId;
	}

	public HashMap<Inode,ArrayList<RevisionTag>> getResolutions() {
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

	public void add(Inode newInode, ArrayList<RevisionTag> tags) {
		resolutions.put(newInode, tags);
	}
	
	public String dump() {
		String s = "InodeDiff " + inodeId + ": " + resolutions.size() + " resolutions\n";
		for(Inode inode : resolutions.keySet()) {
			s += "  Inode " + (inode == null ? "null" : inode.getIdentity()) + ": " + resolutions.get(inode).size() + " reftags";
			if(resolved && resolution.equals(inode)) s += " (SELECTED)";
			s += "\n   ";
			for(RevisionTag tag : resolutions.get(inode)) s += " " + tag;
			s += "\n";
		}
		return s;
	}

	public long getOriginalInodeId() {
		return originalInodeId;
	}
}
