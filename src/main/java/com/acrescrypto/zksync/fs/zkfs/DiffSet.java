package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import com.acrescrypto.zksync.exceptions.ENOENTException;

public class DiffSet {
	protected Revision[] revisions;
	RevisionTag commonAncestor;
	
	private HashMap<RevisionTag,ZKFS> filesystems = new HashMap<RevisionTag,ZKFS>();
	HashMap<String,FileDiff> diffs = new HashMap<String,FileDiff>();
	
	public DiffSet(Revision[] revisions) throws IOException {
		this.revisions = revisions;
		
		RevisionTag[] tags = new RevisionTag[revisions.length];
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
	
	public ZKFS openFS(RevisionTag tag) throws IOException {
		filesystems.putIfAbsent(tag, new ZKFS(new Revision(tag)));
		return filesystems.get(tag);
	}
	
	public HashSet<String> allPaths() throws IOException {
		HashSet<String> allPaths = new HashSet<String>();
		allPaths.add("/");
		
		for(Revision rev : getRevisions()) {
			ZKFS fs = openFS(rev.tag);
			for(String path : fs.opendir("/").listRecursive()) {
				allPaths.add(path);
			}
		}
		return allPaths;
	}
	
	public FileDiff versionsOfFile(String path) throws IOException {
		FileDiff diff = new FileDiff(path);
		for(Revision rev : getRevisions()) {
			diff.addVersion(rev, versionOfFileForTag(rev.tag, path));
		}
		return diff;
	}
	
	protected Inode versionOfFileForTag(RevisionTag tag, String path) throws IOException {
		try {
			return openFS(tag).inodeForPath(path);
		} catch (ENOENTException e) {
			return null;
		}
	}

	public Revision[] getRevisions() {
		return revisions;
	}
	
	public Revision latestRevision() {
		Revision latest = null;
		for(Revision rev : revisions) {
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
