package com.acrescrypto.zksync.fs.zkfs;

import java.util.ArrayList;
import java.util.HashMap;

public class FileDiff {
	String path;
	HashMap<Inode,ArrayList<RefTag>> versions = new HashMap<Inode,ArrayList<RefTag>>();
	protected Inode resolution;
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
