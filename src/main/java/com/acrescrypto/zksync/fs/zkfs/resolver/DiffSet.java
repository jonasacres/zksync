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
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKDirectory;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver.InodeDiffResolver;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver.PathDiffResolver;

/* Describes a difference between a set of revisions. */
public class DiffSet {
	protected RevisionTag[] revisions; // revisions covered by this DiffSet
	
	/** differences in path listings */
	HashMap<String,PathDiff> pathDiffs = new HashMap<String,PathDiff>();
	
	/** differences in inode listings */
	HashMap<Long,InodeDiff> inodeDiffs = new HashMap<Long,InodeDiff>(); // differences in inode table entries
	
	/** different versions of inode by identity */
	Map<Long,ArrayList<InodeDiff>> identityVersions = new HashMap<>(); 

	HashSet<Long> issuedInodeIds = new HashSet<Long>();
	
	/** build a DiffSet from a collection of RefTags */
	public static DiffSet withCollection(Collection<RevisionTag> revisions) throws IOException {
		RevisionTag[] array = new RevisionTag[revisions.size()];
		int i = 0;
		
		for(RevisionTag tag : revisions) array[i++] = tag;
		return new DiffSet(array);
	}
	
	/** build a DiffSet from an array of RefTags */
	public DiffSet(RevisionTag[] revisions) throws IOException {
		this.revisions = revisions;
		
		if(revisions.length > 1) {
			Arrays.sort(this.revisions);
			try(ZKFS fs = pickMergeFs()) {
				findPathDiffs(findInodeDiffs(fs));
			}
		}
	}
	
	/** all inode IDs differing in the revisions of this set, excluding inode table, revision info and freelist */
	public HashSet<Long> allInodes() throws IOException {
		/* This is murderous on archives with huge numbers of inodes, and could likely be avoided
		 * with a more thoughtful means of comparing the revision set.
		 * 
		 * Merges in general are expensive on such archives, since they're O(n) with the total
		 * number of inodes. This is an opportunity for considerable improvement...
		 */
		HashSet<Long> allInodes = new HashSet<Long>();
		for(RevisionTag rev : revisions) {
			try(ZKFS fs = rev.readOnlyFS()) {
				for(Inode inode : fs.getInodeTable().values()) {
					issuedInodeIds.add(inode.getStat().getInodeId());
					if(inode.getStat().getInodeId() == InodeTable.INODE_ID_INODE_TABLE) continue;
					if(inode.getStat().getInodeId() == InodeTable.INODE_ID_FREELIST) continue;
					if(inode.isDeleted()) continue;
					allInodes.add(inode.getStat().getInodeId());
				}
			}
		}
		
		return allInodes;
	}
	
	/** all paths differing in the revisions of this set */
	public HashSet<String> allPaths() throws IOException {
		HashSet<String> allPaths = new HashSet<String>();
		allPaths.add("/");
		
		for(RevisionTag rev : revisions) {
			try(
				ZKFS fs = rev.readOnlyFS();
				ZKDirectory dir = fs.opendir("/")
			) {
				dir.walk(ZKDirectory.LIST_OPT_DONT_FOLLOW_SYMLINKS|ZKDirectory.LIST_OPT_INCLUDE_DOT_DOTDOT, (path, stat, isBrokenSymlink, parent)->{
					allPaths.add(path);
				});
			}
		}
		return allPaths;
	}
	
	/** list of inode id -> reftag, renumbered inode id.
	 * inode IDs are renumbered as necessary to allow preservation of new files created in parallel and issued identical
	 * inode IDs.
	 *  */
	protected Map<Long,Map<RevisionTag,Long>> findInodeDiffs(ZKFS mergeFs) throws IOException {
		Map<Long,Map<RevisionTag,Long>> idMap = new HashMap<Long,Map<RevisionTag,Long>>();
		for(long inodeId : allInodes()) {
			InodeDiff diff = new InodeDiff(inodeId, revisions);
			if(!diff.isConflict()) continue;
			renumberInodeDiff(mergeFs, diff, idMap);
		}
		
		/* If we have an identity that's getting assigned to multiple inodeIds, we need to settle on a
		 * single inodeId.
		 */
		for(long identity : identityVersions.keySet()) {
			ArrayList<InodeDiff> diffs = identityVersions.get(identity);
			if(diffs.size() <= 1) continue;
			
			long minId = Long.MAX_VALUE;
			for(InodeDiff diff : diffs) {
				if(minId > diff.inodeId) {
					minId = diff.inodeId;
				}
			}
			
			final long fMinId = minId;
			InodeDiff megadiff = new InodeDiff(minId);
			for(InodeDiff diff : diffs) {
				diff.resolutions.forEach((inode, tags)->{
					if(inode != null) {
						Inode rebuiltInode = null;
						rebuiltInode = inode.clone();
						megadiff.add(rebuiltInode, diff.originalInodeIdForTag(tags.get(0)), tags);
						rebuiltInode.getStat().setInodeId(fMinId);
					} else {
						megadiff.add(null, -1, tags);
					}
					
					for(RevisionTag tag : tags) {
						idMap.putIfAbsent(diff.originalInodeIdForTag(tag), new HashMap<>());
						idMap.get(diff.originalInodeIdForTag(tag)).put(tag, fMinId);
					}
				});
				
				inodeDiffs.remove(diff.inodeId);
			}
			
			inodeDiffs.put(minId, megadiff);
		}
		
		return idMap;
	}
	
	/** detect path differences, taking inode renumberings into account. */
	protected void findPathDiffs(Map<Long,Map<RevisionTag,Long>> idMap) throws IOException {
		for(String path : allPaths()) {
			PathDiff diff = new PathDiff(path, revisions, idMap);
			if(!diff.isConflict()) continue;
			pathDiffs.put(path, diff);
		}
	}
	
	/** array of reftags in this diffset */
	public RevisionTag[] getRevisions() {
		return revisions;
	}
	
	/** latest revision in this diffset */
	public RevisionTag latestRevision() throws IOException {
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
	protected void renumberInodeDiff(ZKFS fs, InodeDiff diff, Map<Long,Map<RevisionTag,Long>> idMap) throws IOException {
		Map<Long,ArrayList<RevisionTag>> byIdentity = new HashMap<>();
		long minIdent = -1; // we're comparing unsigned, so -1 is actually ffff..., which is maximum 

		for(Inode inode : diff.resolutions.keySet()) {
			if(inode == null) continue;
			if(Long.compareUnsigned(minIdent, inode.getIdentity()) > 0) {
				minIdent = inode.getIdentity();
			}
			
			ArrayList<RevisionTag> tags = byIdentity.getOrDefault(inode.getIdentity(), null);
			if(tags == null) {
				tags = new ArrayList<RevisionTag>();
				byIdentity.put(inode.getIdentity(), tags);
			}

			for(RevisionTag tag : diff.resolutions.get(inode)) tags.add(tag);
		}
		
		ArrayList<Long> sortedIdentities = new ArrayList<Long>();
		sortedIdentities.addAll(byIdentity.keySet());
		sortedIdentities.sort(null);
		
		for(Long identity : sortedIdentities) {
			long newId;
			if(identity.equals(minIdent)) {
				newId = diff.inodeId;
			} else {
				newId = issueInodeId(fs);
			}
			
			idMap.putIfAbsent(diff.inodeId, new HashMap<>());
			for(RevisionTag tag : byIdentity.get(identity)) {
				idMap.get(diff.inodeId).put(tag, newId);
			}
			
			InodeDiff renumberedDiff = renumberInodeWithIdentity(fs, diff, newId, identity);
			inodeDiffs.put(newId, renumberedDiff);
			identityVersions.putIfAbsent(identity, new ArrayList<InodeDiff>());
			identityVersions.get(identity).add(renumberedDiff);
		}
	}
	
	/** assign a new inode ID to a given identity constant in an inode diffset */
	protected InodeDiff renumberInodeWithIdentity(ZKFS fs, InodeDiff diff, long newId, long identity) {
		InodeDiff newDiff = new InodeDiff(newId);
		
		for(Inode inode : diff.resolutions.keySet()) {
			long inodeId = diff.originalInodeIdForTag(diff.resolutions.get(inode).get(0));
			if(inode != null && inode.getIdentity() == identity) {
				Inode newInode = inode.clone(fs);
				newDiff.add(newInode, inodeId, diff.resolutions.get(inode));
				newInode.getStat().setInodeId(newId);
			} else if(inode == null) {
				newDiff.add(null, inodeId, diff.resolutions.get(inode));
			}
		}
		
		return newDiff;
	}
	
	/** deterministically selects a revision within the diffset, and returns a writable ZKFS from that revision */
	protected ZKFS pickMergeFs() throws IOException {
		return latestRevision().getFS();
	}
	
	protected long issueInodeId(ZKFS mergeFs) throws IOException {
		long inodeId = -1;
		while(inodeId < 0 || issuedInodeIds.contains(inodeId)) {
			inodeId = mergeFs.getInodeTable().issueInodeId();
		}
		
		return inodeId;
	}
}
