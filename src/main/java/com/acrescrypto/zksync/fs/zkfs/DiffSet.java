package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.bouncycastle.util.Arrays;

import com.acrescrypto.zksync.exceptions.ENOENTException;

public class DiffSet {
	protected RefTag[] revisions;
	RefTag commonAncestor;
	
	HashMap<String,FileDiff> diffs = new HashMap<String,FileDiff>();
	
	public DiffSet(RefTag[] revisions) throws IOException {
		this.revisions = revisions;
		
		RefTag[] tags = new RefTag[revisions.length];
		for(int i = 0; i < revisions.length; i++) tags[i] = revisions[i];
		
		commonAncestor = revisions[0].archive.getRevisionTree().commonAncestorOf(tags);
		
		for(String path : allPaths()) {
			FileDiff diff = versionsOfFile(path);
			if(diff.hasMultipleVersions()) {
				diffs.put(diff.path, diff);
			}
		}
	}
	
	public Collection<FileDiff> getDiffs() {
		return diffs.values();
	}
	
	public HashSet<String> allPaths() throws IOException {
		HashSet<String> allPaths = new HashSet<String>();
		allPaths.add("/");
		
		for(RefTag rev : revisions) {
			for(String path : rev.getFS().opendir("/").listRecursive()) {
				allPaths.add(path);
			}
		}
		return allPaths;
	}
	
	public FileDiff versionsOfFile(String path) throws IOException {
		FileDiff diff = new FileDiff(path);
		for(RefTag rev : revisions) {
			diff.addVersion(rev, versionOfFileForTag(rev, path));
		}
		return diff;
	}
	
	protected Inode versionOfFileForTag(RefTag tag, String path) throws IOException {
		try {
			return tag.getFS().inodeForPath(path);
		} catch (ENOENTException e) {
			return null;
		}
	}

	public RefTag[] getRevisions() {
		return revisions;
	}
	
	public RefTag latestRevision() throws IOException {
		RefTag latest = null;
		for(RefTag rev : revisions) {
			if(latest == null) {
				latest = rev;
				continue;
			}
			
			RevisionInfo lInfo = latest.getInfo(), rInfo = rev.getInfo();
			if(rInfo.generation < lInfo.generation) continue;
			else if(lInfo.generation > rInfo.generation) {
				latest = rev;
				continue;
			}
			
			if(rInfo.getStat().getMtime() < lInfo.getStat().getMtime()) {
				latest = rev;
				continue;
			}
			
			if(Arrays.compareUnsigned(rev.hash, latest.hash) < 0) {
				latest = rev;
				continue;
			}
		}
		return latest;
	}
	
	public FileDiff diffForPath(String path) {
		return diffs.getOrDefault(path, null);
	}
}
