package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.SearchFailedException;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.RevisionInfo;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKDirectory;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;

public class DiffSetResolver {
	public interface InodeDiffResolver {
		public Inode resolve(DiffSetResolver setResolver, InodeDiff diff) throws IOException, DiffResolutionException;
	}
	
	public interface PathDiffResolver {
		public Long resolve(DiffSetResolver setResolver, PathDiff diff) throws IOException, DiffResolutionException;
	}
	
	DiffSet diffset;
	ZKFS fs;
	InodeDiffResolver inodeResolver;
	PathDiffResolver pathResolver;
	RevisionTag commonAncestor;
	
	public static DiffSetResolver canonicalMergeResolver(ZKArchive archive) throws IOException {
		Collection<RevisionTag> tips = archive.getConfig().getRevisionList().branchTips();
		Collection<RevisionTag> minimalTips = archive.getConfig().getRevisionTree().minimalSet(tips);
		Collection<RevisionTag> baseTips = archive.getConfig().getRevisionTree().canonicalBases(minimalTips);
		DiffSet diffSet = DiffSet.withCollection(baseTips);
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
			if(diff.getResolutions().size() == 2 && diff.getResolutions().containsKey(null)) {
				ArrayList<RevisionTag> revsWithPath = null, revsWithoutPath = null;
				long inodeId = -1, inodeIdentity = -1, originalInodeId = -1;
				boolean existed;
				RevisionTag ancestor;
				
				for(Long listedInodeId : diff.getResolutions().keySet()) {
					ArrayList<RevisionTag> revs = diff.getResolutions().get(listedInodeId);
					if(listedInodeId == null) revsWithoutPath = revs;
					else {
						inodeId = listedInodeId;
						revsWithPath = revs;
					}
				}
				
				inodeIdentity = revsWithPath.get(0).readOnlyFS().inodeForPath(diff.path).getIdentity();
				
				try {
					ancestor = setResolver.commonAncestor();
				} catch (SearchFailedException exc) {
					throw new DiffResolutionException("Cannot find common ancestor to resolve merge");
				}
				
				InodeDiff inodeDiff = setResolver.diffset.inodeDiffs.get(inodeId);
				if(inodeDiff != null) originalInodeId = inodeDiff.originalInodeId;
				else originalInodeId = inodeId;
				
				// TODO Release: (refactor) try using revtag height first before counting on having ancestor inode table
				existed = ancestor.readOnlyFS().exists(diff.path);
				if(!existed) return inodeId; // the path was created after the fork, so keep it.
				
				for(RevisionTag tag : revsWithoutPath) {
					// if one of the revisions without this path has the same inode, then it must have moved
					Inode inode = tag.readOnlyFS().getInodeTable().inodeWithId(originalInodeId);
					if(inode.isDeleted()) continue;
					if(inode.getIdentity() == inodeIdentity) return null; // we moved the inode
				}
				
				// it was created before the fork and no one moved it, so did someone edit it?
				for(RevisionTag tag : revsWithPath) {
					long changeHeight = tag.readOnlyFS().inodeForPath(diff.path).getChangedFrom().getHeight() + 1;
					if(changeHeight > ancestor.getHeight()) return inodeId; // edited; keep the file.
				}
				
				// the file is deleted on one side of the fork, and unchanged on the other. keep the deletion.
				return null;
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
		if(diffset.revisions.length == 1) {
			fs.getArchive().getConfig().getRevisionList().consolidate(diffset.revisions[0]);
			return diffset.revisions[0];
		}
		
		if(diffset.revisions.length > RevisionInfo.maxParentsForConfig(fs.getArchive().getConfig())) {
			return resolveExcessive();
		}
		
		selectResolutions();
		applyResolutions();
		RevisionTag revTag = fs.commitWithTimestamp(diffset.revisions, 0);
		fs.getArchive().getConfig().getRevisionList().consolidate(revTag);
		return revTag;
	}
	
	protected RevisionTag resolveExcessive() throws IOException, DiffResolutionException {
		/* There's a cap on how many parents one revision can have. If we exceed that, then we need to
		 * do a bunch of partial merges containing a subset of all the revisions we want to merge. Then
		 * we try again with the revtags generated by the partial merges.
		 */
		int maxParents = RevisionInfo.maxParentsForConfig(fs.getArchive().getConfig());
		ArrayList<RevisionTag> sourceList = new ArrayList<>();
		LinkedList<RevisionTag> partialMerges = new LinkedList<>();
		LinkedList<RevisionTag> mergeList = new LinkedList<>();
		
		for(RevisionTag revTag : diffset.revisions) {
			sourceList.add(revTag);
		}
		sourceList.sort(null);
		
		for(int i = 0; i < sourceList.size(); i++) {
			mergeList.add(sourceList.get(i));
			if(mergeList.size() == maxParents || i == diffset.revisions.length-1) {
				DiffSet mergeDiffset = DiffSet.withCollection(mergeList);
				DiffSetResolver resolver = new DiffSetResolver(mergeDiffset, inodeResolver, pathResolver);
				RevisionTag partialMerge = resolver.resolve();
				partialMerges.add(partialMerge);
				mergeList.clear();
			}
		}
		
		partialMerges.sort(null);
		DiffSet partialDiffset = DiffSet.withCollection(partialMerges);
		DiffSetResolver partialResolver = new DiffSetResolver(partialDiffset, inodeResolver, pathResolver);
		return partialResolver.resolve();
	}
	
	protected void selectResolutions() throws IOException, DiffResolutionException {
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
		
		ArrayList<Inode> toUnlink = new ArrayList<>();
		for(PathDiff diff : sortedPathDiffs()) { // need to sort so we do parent directories before children
			assert(diff.isResolved());
			if(!parentExists(diff.path)) continue;
			ZKDirectory dir = fs.opendir(fs.dirname(diff.path));
			dir.updateLink(diff.resolution, fs.basename(diff.path), toUnlink);
			dir.close();
		}
		
		for(Inode inode : toUnlink) {
			try {
				inode.removeLink();
			} catch(ENOENTException exc) {}
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
	
	protected RevisionTag commonAncestor() throws SearchFailedException {
		if(commonAncestor != null) return commonAncestor;
		return commonAncestor = fs.getArchive().getConfig().getRevisionTree().commonAncestor(diffset.revisions);
	}
	
	public DiffSet getDiffSet() {
		return diffset;
	}
	
	public ZKFS getFs() {
		return fs;
	}
}
