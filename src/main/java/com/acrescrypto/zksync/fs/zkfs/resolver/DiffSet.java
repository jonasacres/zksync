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
		findInodeDiffs();
		findPathDiffs();
	}
	
	public DiffSet(DiffSet original, ArrayList<InodeDiff> inodeDiffList, ArrayList<PathDiff> pathDiffList) {
		this.revisions = original.revisions;
		this.commonAncestor = original.commonAncestor;
		for(InodeDiff inodeDiff : inodeDiffList) inodeDiffs.put(inodeDiff.inodeId, inodeDiff);
		for(PathDiff pathDiff : pathDiffList) pathDiffs.put(pathDiff.path, pathDiff);
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
	
	public DiffSet renumber(ZKFS fs) {
		Map<Long,Map<RefTag,Long>> idMap = new HashMap<Long,Map<RefTag,Long>>();
		ArrayList<InodeDiff> newInodeDiffs = new ArrayList<InodeDiff>();
		ArrayList<PathDiff> newPathDiffs = new ArrayList<PathDiff>();
		for(InodeDiff diff : inodeDiffs.values()) newInodeDiffs.addAll(renumberInodeDiff(fs, diff, idMap));
		for(PathDiff diff : pathDiffs.values()) newPathDiffs.add(renumberPath(fs, diff, idMap));
		return new DiffSet(this, newInodeDiffs, newPathDiffs);
	}

	protected ArrayList<InodeDiff> renumberInodeDiff(ZKFS fs, InodeDiff diff, Map<Long,Map<RefTag,Long>> idMap) {
		Map<Long,ArrayList<RefTag>> byIdentity = new HashMap<Long,ArrayList<RefTag>>();
		long minId = Long.MAX_VALUE;

		for(Inode inode : diff.resolutions.keySet()) {
			if(inode == null) continue;
			if(inode.getIdentity() < minId) minId = inode.getIdentity();
			ArrayList<RefTag> tags = byIdentity.getOrDefault(inode.getIdentity(), null);
			if(tags == null) {
				tags = new ArrayList<RefTag>();
				byIdentity.put(inode.getIdentity(), tags);
			}

			for(RefTag tag : diff.resolutions.get(inode)) tags.add(tag);
		}

		ArrayList<InodeDiff> newDiffs = new ArrayList<InodeDiff>();
		for(Long identity : byIdentity.keySet()) {
			long newId = identity.equals(minId) ? identity : fs.getInodeTable().issueInodeId();
			idMap.putIfAbsent(identity, new HashMap<RefTag,Long>());
			for(RefTag tag : byIdentity.get(identity)) idMap.get(identity).put(tag, newId);
			newDiffs.add(renumberInodeWithIdentity(fs, diff, newId, identity));
		}

		return newDiffs;
	}

	protected InodeDiff renumberInodeWithIdentity(ZKFS fs, InodeDiff diff, long newId, long identity) {
		InodeDiff newDiff = new InodeDiff(newId);

		for(Inode inode : diff.resolutions.keySet()) {
			if(inode.getIdentity() != identity) continue;
			Inode newInode = inode.clone(fs);
			newInode.getStat().setInodeId(newId);
			newDiff.add(newInode, diff.resolutions.get(inode));
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
				} catch(NullPointerException exc) {}
			}
		}
		
		return newDiff;
	}
}
