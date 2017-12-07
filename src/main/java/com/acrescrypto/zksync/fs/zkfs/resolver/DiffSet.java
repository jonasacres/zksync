package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.InodeTable;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
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
		findPathDiffs(findInodeDiffs(pickMergeFs()));
	}
	
	public DiffSet(DiffSet original, ArrayList<InodeDiff> inodeDiffList, ArrayList<PathDiff> pathDiffList) {
		this.revisions = original.revisions;
		this.commonAncestor = original.commonAncestor;
		for(InodeDiff inodeDiff : inodeDiffList) inodeDiffs.put(inodeDiff.inodeId, inodeDiff);
		for(PathDiff pathDiff : pathDiffList) pathDiffs.put(pathDiff.path, pathDiff);
	}
	
	public HashSet<Long> allInodes() throws IOException {
		// TODO: allInodes and allPaths makes merging O(n) with the number of total files, changed or not.
		HashSet<Long> allInodes = new HashSet<Long>();
		for(RefTag rev : revisions) {
			for(Inode inode : rev.readOnlyFS().getInodeTable().values()) {
				if(inode.getStat().getInodeId() == InodeTable.INODE_ID_INODE_TABLE) continue;
				if(inode.getStat().getInodeId() == InodeTable.INODE_ID_REVISION_INFO) continue;
				if(inode.isDeleted()) continue;
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
	
	public Map<Long,Map<RefTag,Long>> findInodeDiffs(ZKFS mergeFs) throws IOException {
		Map<Long,Map<RefTag,Long>> idMap = new HashMap<Long,Map<RefTag,Long>>();
		for(long inodeId : allInodes()) {
			InodeDiff diff = new InodeDiff(inodeId, revisions);
			if(!diff.isConflict()) continue;
			renumberInodeDiff(mergeFs, diff, idMap);
			inodeDiffs.put(inodeId, diff);
		}
		
		return idMap;
	}
	
	public void findPathDiffs(Map<Long,Map<RefTag,Long>> idMap) throws IOException {
		for(String path : allPaths()) {
			PathDiff diff = new PathDiff(path, revisions, idMap);
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

	protected void renumberInodeDiff(ZKFS fs, InodeDiff diff, Map<Long,Map<RefTag,Long>> idMap) {
		Map<Long,ArrayList<RefTag>> byIdentity = new HashMap<Long,ArrayList<RefTag>>();
		long minIdent = Long.MAX_VALUE;

		for(Inode inode : diff.resolutions.keySet()) {
			if(inode == null) continue;
			if(inode.getIdentity() < minIdent) minIdent = inode.getIdentity();
			ArrayList<RefTag> tags = byIdentity.getOrDefault(inode.getIdentity(), null);
			if(tags == null) {
				tags = new ArrayList<RefTag>();
				byIdentity.put(inode.getIdentity(), tags);
			}

			for(RefTag tag : diff.resolutions.get(inode)) tags.add(tag);
		}

		for(Long identity : byIdentity.keySet()) {
			long newId = identity.equals(minIdent) ? diff.inodeId : fs.getInodeTable().issueInodeId();
			idMap.putIfAbsent(diff.inodeId, new HashMap<RefTag,Long>());
			for(RefTag tag : byIdentity.get(identity)) idMap.get(diff.inodeId).put(tag, newId);
			inodeDiffs.put(newId, renumberInodeWithIdentity(fs, diff, newId, identity));
		}
	}

	protected InodeDiff renumberInodeWithIdentity(ZKFS fs, InodeDiff diff, long newId, long identity) {
		InodeDiff newDiff = new InodeDiff(newId);
		
		for(Inode inode : diff.resolutions.keySet()) {
			if(inode != null && inode.getIdentity() == identity) {
				Inode newInode = inode.clone(fs);
				newInode.getStat().setInodeId(newId);
				newDiff.add(newInode, diff.resolutions.get(inode));
			} else {
				newDiff.add(null, diff.resolutions.get(inode));
			}
		}
		
		return newDiff;
	}

	protected PathDiff renumberPath(ZKFS fs, PathDiff diff, Map<Long,Map<RefTag,Long>> idMap) {
		PathDiff newDiff = new PathDiff(diff.path);
		
		for(Long inodeId : diff.resolutions.keySet()) {
			for(RefTag tag : diff.resolutions.get(inodeId)) {
				try {
					Long newInodeId = idMap.get(inodeId).get(tag);
					newDiff.add(newInodeId, tag);
				} catch(NullPointerException exc) {
					newDiff.add(inodeId, tag);
				}
			}
		}
		
		return newDiff;
	}
	
	protected ZKFS pickMergeFs() throws IOException {
		RefTag latest = null;
		for(RefTag tag : revisions) {
			if(latest == null || tag.compareTo(latest) > 0) latest = tag;
		}
		
		if(latest != null) return latest.getFS();
		return null;
	}
}
