package com.acrescrypto.zksync.fs.zkfs;

import java.util.ArrayList;
import java.util.HashMap;

public class FileDiff {
	String path;
	HashMap<Inode,ArrayList<RevisionInfo>> versions = new HashMap<Inode,ArrayList<RevisionInfo>>();
	protected Inode resolution;
	protected boolean resolved;
	
	public FileDiff(String path) {
		this.path = path;
	}
	
	public void addVersion(RevisionInfo rev, Inode inode) {
		ArrayList<RevisionInfo> list = versions.getOrDefault(inode, null);
		if(list == null) versions.put(inode, list = new ArrayList<RevisionInfo>());
		list.add(rev);
	}
	
	public boolean hasMultipleVersions() {
		return versions.size() > 1;
	}
	
	public String getPath() {
		return path;
	}
	
	public Inode earliestVersion() {
		Inode best = null;
		for(Inode inode : versions.keySet()) {
			if(best == null || inode.getStat().getMtime() < best.getStat().getMtime()) best = inode;
			if(inode.getStat().getMtime() == best.getStat().getMtime() && inode.hashCode() < best.hashCode()) best = inode;
		}
		return best;
	}
	
	public Inode latestVersion() {
		Inode best = null;
		for(Inode inode : versions.keySet()) {
			if(best == null || inode.getStat().getMtime() > best.getStat().getMtime()) best = inode;
			if(inode.getStat().getMtime() == best.getStat().getMtime() && inode.hashCode() < best.hashCode()) best = inode;
		}
		return best;
	}
	
	public boolean isResolved() {
		return resolved;
	}

	public Inode getResolution() {
		return resolution;
	}

	public void resolve(Inode resolution) {
		this.resolution = resolution;
		this.resolved = true;
	}
}
