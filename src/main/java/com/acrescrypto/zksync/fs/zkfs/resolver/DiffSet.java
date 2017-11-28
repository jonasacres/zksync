package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.InodeTable;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver.InodeDiffResolver;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver.PathDiffResolver;

public class DiffSet {
	protected RefTag[] revisions;
	RefTag commonAncestor;
	
	HashMap<String,PathDiff> pathDiffs = new HashMap<String,PathDiff>();
	HashMap<Long,InodeDiff> inodeDiffs = new HashMap<Long,InodeDiff>();
	
	public static DiffSet withCollection(Collection<RefTag> revisions) throws IOException {
		RefTag[] array = new RefTag[revisions.size()];
		int i = 0;
		
		for(RefTag tag : revisions) array[i++] = tag;
		return new DiffSet(array);
	}
	
	public DiffSet(RefTag[] revisions) throws IOException {
		this.revisions = revisions;
		Arrays.sort(this.revisions);
		
		RefTag[] tags = new RefTag[revisions.length];
		for(int i = 0; i < revisions.length; i++) tags[i] = revisions[i];
		
		commonAncestor = revisions[0].getArchive().getRevisionTree().commonAncestorOf(tags);
		findInodeDiffs();
		findPathDiffs();
	}
	
	public HashSet<Long> allInodes() throws IOException {
		HashSet<Long> allInodes = new HashSet<Long>();
		for(RefTag rev : revisions) {
			for(Inode inode : rev.readOnlyFS().getInodeTable().getInodes().values()) {
				// Disregard the RevisionInfo file in calculating diffs, since it always differs and can't be merged.
				if(inode.getStat().getInodeId() == InodeTable.INODE_ID_REVISION_INFO) continue;
				allInodes.add(inode.getStat().getInodeId());
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
		return revisions[revisions.length-1];
	}
	
	public DiffSetResolver resolver(InodeDiffResolver inodeResolver, PathDiffResolver pathResolver) throws IOException {
		return new DiffSetResolver(this, inodeResolver, pathResolver);
	}
}
