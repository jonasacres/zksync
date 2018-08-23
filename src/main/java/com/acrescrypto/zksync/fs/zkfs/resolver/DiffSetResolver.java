package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.exceptions.SearchFailedException;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKDirectory;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;

/* TODO DHT: (design) Remerges
 * Problem: Order matters when merging multiple simultaneous revisions. If there are 3 revisions,
 * r1, r2 and r3, then ((r1 + r2) + r3) produces a different reftag from (r1 + (r2 + r3)). In p2p syncing,
 * any permutation of merge orders might happen for a given peer. Each permutation is then shared
 * and merged, creating an endless cycle of merging.
 * 
 * Idea: label merges as "pure merges." A pure merge has a flag set somewhere (e.g. RevisionInfo) indicating
 * that no writes, deletions or other changes have been made to the filesystem, and all versions of
 * the data are to be found in at least one of the named parents.
 * 
 * When "pure merges" are merged, the resulting merge lists the parents of the pure merges together,
 * and does NOT list the pure merges themselves as parents. But how to ensure that inode reallocation is
 * done consistently, without requiring clients to reacquire the original parents?
 */

public class DiffSetResolver {
	public interface InodeDiffResolver {
		public Inode resolve(DiffSetResolver setResolver, InodeDiff diff);
	}
	
	public interface PathDiffResolver {
		public Long resolve(DiffSetResolver setResolver, PathDiff diff);
	}
	
	DiffSet diffset;
	ZKFS fs;
	InodeDiffResolver inodeResolver;
	PathDiffResolver pathResolver;
	
	public static DiffSetResolver canonicalMergeResolver(ZKArchive archive) throws IOException {
		// TODO DHT: withTangledCollection
		DiffSet diffSet = DiffSet.withCollection(archive.getConfig().getRevisionList().branchTips());
		return latestVersionResolver(diffSet);
	}
	
	public static DiffSetResolver latestVersionResolver(DiffSet diffset) throws IOException {
		return new DiffSetResolver(diffset, latestInodeResolver(), latestPathResolver());
	}
	
	public static InodeDiffResolver latestInodeResolver() {
		return (DiffSetResolver setResolver, InodeDiff diff) -> {
			Inode result = null;
			for(Inode inode : diff.getResolutions().keySet()) {
				if(result == null || (inode != null && result.compareTo(inode) < 0)) result = inode;
			}
			return result;
		};
	}
	
	public static PathDiffResolver latestPathResolver() {
		return (DiffSetResolver setResolver, PathDiff diff) -> {
			Inode result = null;
			RevisionTag ancestorTag;
			try {
				ancestorTag = setResolver.fs.getArchive().getConfig().getRevisionTree().commonAncestor(setResolver.diffset.revisions);
			} catch (SearchFailedException e) {
				// TODO DHT: (refactor) Trigger merge failure
				return null;
			}
			Long defaultId = null;
			
			try {
				defaultId = ancestorTag.readOnlyFS().inodeForPath(diff.path).getStat().getInodeId();
			} catch(IOException exc) {
			}
			
			if(diff.getResolutions().size() == 2 && diff.getResolutions().containsKey(null)) {
				// if it's null and one thing, and the one thing is what used to be there, take the null.
				// else, take the one thing.
				for(Long inodeId : diff.getResolutions().keySet()) {
					if(inodeId != null && inodeId != defaultId) return inodeId;
				}
				
				if(defaultId != null) return null;
			}
			
			for(Long inodeId : diff.getResolutions().keySet()) {
				if(inodeId == null) continue;

				Inode inode = null;
				try {
					InodeDiff idiff = setResolver.diffset.inodeDiffs.getOrDefault(inodeId, null);
					if(idiff == null) {
						// no inode diff for this id; so any reftag's version will work since they're all identical
						inode = setResolver.fs.getInodeTable().inodeWithId(inodeId);
					} else {
						inode = idiff.getResolution();
					}
				} catch (IOException exc) {
					throw new IllegalStateException("Encountered exception resolving path collision information for inode " + inodeId);
				}
				
				if(result == null || result.compareTo(inode) < 0) result = inode;
			}
			
			return result != null ? result.getStat().getInodeId() : null;
		};
	}
	
	public DiffSetResolver(DiffSet diffset, InodeDiffResolver inodeResolver, PathDiffResolver pathResolver) throws IOException {
		this.diffset = diffset;
		this.inodeResolver = inodeResolver;
		this.pathResolver = pathResolver;
		this.fs = new ZKFS(diffset.latestRevision());
	}
	
	public RevisionTag resolve() throws IOException, DiffResolutionException {
		if(diffset.revisions.length == 1) return diffset.revisions[0];
		
		selectResolutions();
		applyResolutions();
		RevisionTag revTag = fs.commitWithTimestamp(diffset.revisions, 0);
		return revTag;
	}
	
	protected void selectResolutions() {
		for(InodeDiff diff : diffset.inodeDiffs.values()) {
			diff.setResolution(inodeResolver.resolve(this, diff));
		}

		for(PathDiff diff : diffset.pathDiffs.values()) {
			diff.setResolution(pathResolver.resolve(this, diff));
		}
	}
	
	protected void applyResolutions() throws IOException {
		for(InodeDiff diff : diffset.inodeDiffs.values()) {
			fs.getInodeTable().replaceInode(diff);
		}
		
		for(PathDiff diff : sortedPathDiffs()) { // need to sort so we do parent directories before children
			assert(diff.isResolved());
			if(!parentExists(diff.path)) continue;
			ZKDirectory dir = fs.opendir(fs.dirname(diff.path));
			dir.updateLink(diff.resolution, fs.basename(diff.path));
			dir.close();
		}
	}
	
	protected List<PathDiff> sortedPathDiffs() {
		List<PathDiff> pathList = new ArrayList<PathDiff>(diffset.pathDiffs.values());
		Collections.sort(pathList);
		return pathList;
	}
	
	protected boolean parentExists(String path) {
		String dirname = fs.dirname(path);
		if(dirname.equals("/")) return true;
		if(diffset.pathDiffs.containsKey(dirname)) {
			PathDiff diff = diffset.pathDiffs.get(dirname);
			if(diff.resolution == null) return false;
		}
		return parentExists(dirname);
	}
	
	public DiffSet getDiffSet() {
		return diffset;
	}
	
	public ZKFS getFs() {
		return fs;
	}
}
