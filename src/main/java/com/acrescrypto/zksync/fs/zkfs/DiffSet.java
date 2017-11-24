package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.bouncycastle.util.Arrays;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver.FileDiffResolver;

public class DiffSet {
	protected RefTag[] revisions;
	RefTag commonAncestor;
	
	/* TODO: Diffs aren't really on "files." That's not a meaningful concept in a merge.
	 * There are inodes (which differ in content), and paths (which differ in inode or existence,
	 * with nonexistence possibly being an inode of null.)
	 */
	
	HashMap<String,PathDiff> pathDiffs = new HashMap<String,PathDiff>();
	HashMap<Long,InodeDiff> inodeDiffs = new HashMap<Long,InodeDiff>();
	
	public DiffSet(RefTag[] revisions) throws IOException {
		this.revisions = revisions;
		
		RefTag[] tags = new RefTag[revisions.length];
		for(int i = 0; i < revisions.length; i++) tags[i] = revisions[i];
		
		commonAncestor = revisions[0].archive.getRevisionTree().commonAncestorOf(tags);
	}
	
	public HashSet<Long> allInodes() throws IOException {
		HashSet<Long> allInodes = new HashSet<Long>();
		for(RefTag rev : revisions) {
			for(Inode inode : rev.readOnlyFS().getInodeTable().inodes.values()) {
				allInodes.add(inode.stat.getInodeId());
			}
		}
		
		return allInodes;
	}
	
	public HashSet<String> allPaths() throws IOException {
		HashSet<String> allPaths = new HashSet<String>();
		allPaths.add("/");
		
		for(RefTag rev : revisions) {
			for(String path : rev.readOnlyFS().opendir("/").listRecursive()) {
				allPaths.add(path);
			}
		}
		return allPaths;
	}
	
	public void findInodeDiffs() throws IOException {
		for(long inodeId : allInodes()) {
			InodeDiff diff = new InodeDiff(inodeId, revisions);
			if(!diff.isConflict()) continue;
			inodeDiffs.put(inodeId, diff);
		}
	}
	
	public void findPathDiffs() throws IOException {
		for(String path : allPaths()) {
			PathDiff diff = new PathDiff(path, revisions);
			if(!diff.isConflict()) continue;
			pathDiffs.put(path, diff);
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
	
	public DiffSetResolver resolver(FileDiffResolver lambda) throws IOException {
		return new DiffSetResolver(this, lambda);
	}
}
