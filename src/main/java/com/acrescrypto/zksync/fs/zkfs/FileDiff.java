package com.acrescrypto.zksync.fs.zkfs;

import java.util.ArrayList;
import java.util.HashMap;

public class FileDiff {
	String path;
	long inodeId;
	HashMap<Inode,ArrayList<RefTag>> versions = new HashMap<Inode,ArrayList<RefTag>>();
	protected FileDiffResolution resolution;
	protected boolean resolved;
	
	public FileDiff(String path) {
		this.path = path;
	}
	
	public void addVersion(RefTag rev, Inode inode) {
		ArrayList<RefTag> list = versions.getOrDefault(inode, null);
		if(list == null) versions.put(inode, list = new ArrayList<RefTag>());
		list.add(rev);
	}
	
	public boolean hasMultipleVersions() {
		return versions.size() > 1;
	}
	
	public String getPath() {
		return path;
	}
	
	public HashMap<Inode,ArrayList<RefTag>> getVersions() {
		return versions;
	}
	
	public Inode earliestVersion() {
		Inode best = null;
		for(Inode inode : versions.keySet()) {
			if(best == null || inode.modifiedTime < best.modifiedTime) best = inode;
			if(inode.modifiedTime == best.modifiedTime && inode.hashCode() < best.hashCode()) best = inode;
		}
		return best;
	}
	
	public Inode latestVersion() {
		Inode best = null;
		for(Inode inode : versions.keySet()) {
			if(best == null || inode.modifiedTime > best.modifiedTime) best = inode;
			if(inode.modifiedTime == best.modifiedTime && inode.hashCode() < best.hashCode()) best = inode;
		}
		return best;
	}
	
	public boolean isResolved() {
		return resolved;
	}

	public FileDiffResolution getResolution() {
		return resolution;
	}

	public void resolve(Inode result) {
		if(result == null) {
			this.resolution = new FileDiffResolution(inodeId, versions.get(result));
		} else {
			this.resolution = new FileDiffResolution(result, versions.get(result));
		}
		this.resolved = true;
	}
}
