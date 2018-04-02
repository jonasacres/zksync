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

/* Describes a difference between a set of revisions. */
public class DiffSet {
	protected RefTag[] revisions; // revisions covered by this DiffSet
	
	/** most recent common ancestor of all revisions in this DiffSet */ 
	RefTag commonAncestor;
	
	/** differences in path listings */
	HashMap<String,PathDiff> pathDiffs = new HashMap<String,PathDiff>();
	
	/** differences in inode listings */
	HashMap<Long,InodeDiff> inodeDiffs = new HashMap<Long,InodeDiff>(); // differences in inode table entries
	
	/** build a DiffSet from a collection of RefTags */
	public static DiffSet withCollection(Collection<RefTag> revisions) throws IOException {
		RefTag[] array = new RefTag[revisions.size()];
		int i = 0;
		
		for(RefTag tag : revisions) array[i++] = tag;
		return new DiffSet(array);
	}
	
	/** build a DiffSet from an array of RefTags */
	public DiffSet(RefTag[] revisions) throws IOException {
		this.revisions = revisions;
		Arrays.sort(this.revisions);
		
		RefTag[] tags = new RefTag[revisions.length];
		for(int i = 0; i < revisions.length; i++) tags[i] = revisions[i];
		
		commonAncestor = revisions[0].getArchive().getRevisionTree().commonAncestorOf(tags);
		findPathDiffs(findInodeDiffs(pickMergeFs()));
	}
	
	/** build a new DiffSet based on an existing one, with some new inode and path diffs */
	public DiffSet(DiffSet original, ArrayList<InodeDiff> inodeDiffList, ArrayList<PathDiff> pathDiffList) {
		this.revisions = original.revisions;
		this.commonAncestor = original.commonAncestor;
		for(InodeDiff inodeDiff : inodeDiffList) inodeDiffs.put(inodeDiff.inodeId, inodeDiff);
		for(PathDiff pathDiff : pathDiffList) pathDiffs.put(pathDiff.path, pathDiff);
	}
	
	/** all inode IDs differing in the revisions of this set, excluding inode table, revision info and freelist */
	public HashSet<Long> allInodes() throws IOException {
		// TODO: allInodes and allPaths makes merging O(n) with the number of total files, changed or not.
		HashSet<Long> allInodes = new HashSet<Long>();
		for(RefTag rev : revisions) {
			for(Inode inode : rev.readOnlyFS().getInodeTable().values()) {
				if(inode.getStat().getInodeId() == InodeTable.INODE_ID_INODE_TABLE) continue;
				if(inode.getStat().getInodeId() == InodeTable.INODE_ID_REVISION_INFO) continue;
				if(inode.getStat().getInodeId() == InodeTable.INODE_ID_FREELIST) continue;
				if(inode.isDeleted()) continue;
				allInodes.add(inode.getStat().getInodeId());
			}
		}
		
		return allInodes;
	}
	
	/** all paths differing in the revisions of this set */
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
	
	/** list of inode id -> reftag, renumbered inode id.
	 * inode IDs are renumbered as necessary to allow preservation of new files created in parallel and issued identical
	 * inode IDs.
	 *  */
	protected Map<Long,Map<RefTag,Long>> findInodeDiffs(ZKFS mergeFs) throws IOException {
		Map<Long,Map<RefTag,Long>> idMap = new HashMap<Long,Map<RefTag,Long>>();
		for(long inodeId : allInodes()) {
			InodeDiff diff = new InodeDiff(inodeId, revisions);
			if(!diff.isConflict()) continue;
			renumberInodeDiff(mergeFs, diff, idMap);
		}
		
		return idMap;
	}
	
	/** detect path differences, taking inode renumberings into account. */
	protected void findPathDiffs(Map<Long,Map<RefTag,Long>> idMap) throws IOException {
		for(String path : allPaths()) {
			PathDiff diff = new PathDiff(path, revisions, idMap);
			if(!diff.isConflict()) continue;
			pathDiffs.put(path, diff);
		}
	}
	
	/** array of reftags in this diffset */
	public RefTag[] getRevisions() {
		return revisions;
	}
	
	/** latest revision in this diffset */
	public RefTag latestRevision() throws IOException {
		return revisions[revisions.length-1];
	}
	
	/** make a DiffSetResolver for this specific diffset */
	public DiffSetResolver resolver(InodeDiffResolver inodeResolver, PathDiffResolver pathResolver) throws IOException {
		return new DiffSetResolver(this, inodeResolver, pathResolver);
	}
	
	/** For inodes whose ID matches but whose identity field does not, we presume they refer to different files that
	 * must each be preserved. The inode with the lowest identity retains its ID number; all others are issued new IDs.
	 * 
	 * @param fs ZKFS to issue inode IDs from
	 * @param diff InodeDiff describing conflicting inode records 
	 * @param idMap maps oldId -> RefTag, newId
	 * @throws IOException
	 */
	protected void renumberInodeDiff(ZKFS fs, InodeDiff diff, Map<Long,Map<RefTag,Long>> idMap) throws IOException {
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
	
	/** assign a new inode ID to a given identity constant in an inode diffset */
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
	
	/** deterministically selects a revision within the diffset, and returns a writable ZKFS from that revision */
	protected ZKFS pickMergeFs() throws IOException {
		return latestRevision().getFS();
	}
}
