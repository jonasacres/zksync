package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.utility.Util;

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
			Inode inode = candidate.readOnlyFS().getInodeTable().inodeWithId(inodeId);
			if(inode.isDeleted()) inode = null;
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
	
	public String dump() {
		String s = "InodeDiff " + inodeId + ": " + resolutions.size() + " resolutions\n";
		for(Inode inode : resolutions.keySet()) {
			s += "  Inode " + (inode == null ? "null" : inode.getIdentity()) + ": " + resolutions.get(inode).size() + " reftags";
			if(resolved && resolution.equals(inode)) s += " (SELECTED)";
			s += "\n   ";
			for(RefTag tag : resolutions.get(inode)) s += " " + Util.bytesToHex(tag.getShortHashBytes());
			s += "\n";
		}
		return s;
	}
}
