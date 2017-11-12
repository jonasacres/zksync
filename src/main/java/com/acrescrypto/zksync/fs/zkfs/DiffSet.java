package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import com.acrescrypto.zksync.exceptions.ENOENTException;

public class DiffSet {
	protected RevisionInfo[] revisions;
	RefTag commonAncestor;
	
	private HashMap<RefTag,ZKFS> filesystems = new HashMap<RefTag,ZKFS>();
	HashMap<String,FileDiff> diffs = new HashMap<String,FileDiff>();
	
	public DiffSet(RevisionInfo[] revisions) throws IOException {
		this.revisions = revisions;
		
		RefTag[] tags = new RefTag[revisions.length];
		for(int i = 0; i < revisions.length; i++) tags[i] = revisions[i].tag;
		
		commonAncestor = revisions[0].fs.getRevisionTree().commonAncestorOf(tags);
		
		for(String path : allPaths()) {
			FileDiff diff = versionsOfFile(path);
			if(diff.hasMultipleVersions()) {
				diffs.put(diff.path, diff);
			}
		}
		
		filesystems = null;
	}
	
	public Collection<FileDiff> getDiffs() {
		return diffs.values();
	}
	
	public ZKFS openFS(RefTag tag) throws IOException {
		filesystems.putIfAbsent(tag, new ZKFS(new RevisionInfo(tag)));
		return filesystems.get(tag);
	}
	
	public HashSet<String> allPaths() throws IOException {
		HashSet<String> allPaths = new HashSet<String>();
		allPaths.add("/");
		
		for(RevisionInfo rev : getRevisions()) {
			ZKFS fs = openFS(rev.tag);
			for(String path : fs.opendir("/").listRecursive()) {
				allPaths.add(path);
			}
		}
		return allPaths;
	}
	
	public FileDiff versionsOfFile(String path) throws IOException {
		FileDiff diff = new FileDiff(path);
		for(RevisionInfo rev : getRevisions()) {
			diff.addVersion(rev, versionOfFileForTag(rev.tag, path));
		}
		return diff;
	}
	
	protected Inode versionOfFileForTag(RefTag tag, String path) throws IOException {
		try {
			return openFS(tag).inodeForPath(path);
		} catch (ENOENTException e) {
			return null;
		}
	}

	public RevisionInfo[] getRevisions() {
		return revisions;
	}
	
	public RevisionInfo latestRevision() {
		RevisionInfo latest = null;
		for(RevisionInfo rev : revisions) {
			if(latest == null || rev.generation > latest.generation) latest = rev;
			else if(latest.generation == rev.generation && rev.supernode.stat.getMtime() > latest.supernode.stat.getMtime()) latest = rev;
			// TODO: further tie-breaking on tag
		}
		return latest;
	}
	
	public FileDiff diffForPath(String path) {
		return diffs.getOrDefault(path, null);
	}
}
