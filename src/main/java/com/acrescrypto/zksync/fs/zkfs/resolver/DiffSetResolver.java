package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKDirectory;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;

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
		return defaultResolver(DiffSet.withCollection(archive.getRevisionTree().branchTips()));
	}
	
	public static DiffSetResolver defaultResolver(DiffSet diffset) throws IOException {
		return latestVersionResolver(diffset);
	}
	
	public static DiffSetResolver latestVersionResolver(DiffSet diffset) throws IOException {
		InodeDiffResolver inodeResolver = (DiffSetResolver setResolver, InodeDiff diff) -> {
			Inode result = null;
			for(Inode inode : diff.getResolutions().keySet()) {
				if(result == null || (inode != null && result.compareTo(inode) < 0)) result = inode;
			}
			return result;
		};
		
		PathDiffResolver pathResolver = (DiffSetResolver setResolver, PathDiff diff) -> {
			Inode result = null;
			
			for(Long inodeId : diff.getResolutions().keySet()) {
				if(inodeId == null) continue;
				Inode inode = null;
				try {
					InodeDiff idiff = diffset.inodeDiffs.getOrDefault(inodeId, null);
					if(idiff == null) {
						// no inode diff for this id; so any reftag's version will work since they're all identical
						inode = setResolver.fs.getInodeTable().inodeWithId(inodeId);
					} else {
						inode = idiff.getResolution();
					}
				} catch (IOException e) {
					throw new IllegalStateException("Encountered exception resolving path collision information for inode " + inodeId);
				}
				
				if(result == null || result.compareTo(inode) < 0) result = inode;
			}
			
			/* TODO: This doesn't allow for deletion!
			 * Or at least, merges will countermand deletion, even if no one else touched the deleted file.
			 * But if the best alternative pre-dates the diffset, let's prefer null. How to tell if alternative
			 * predates diffset?
			 */
			
			return result != null ? result.getStat().getInodeId() : null;
		};
		
		return new DiffSetResolver(diffset, inodeResolver, pathResolver);
	}
	
	public DiffSetResolver(DiffSet diffset, InodeDiffResolver inodeResolver, PathDiffResolver pathResolver) throws IOException {
		this.diffset = diffset;
		this.inodeResolver = inodeResolver;
		this.pathResolver = pathResolver;
		this.fs = new ZKFS(diffset.latestRevision());
	}
	
	public RefTag resolve() throws IOException, DiffResolutionException {
		selectResolutions();
		enforceDirectoryConsistency();
		applyResolutions();
		return fs.commit(diffset.revisions, null); // TODO: need a consistent seed here
	}
	
	protected void selectResolutions() {
		for(InodeDiff diff : diffset.inodeDiffs.values()) {
			diff.setResolution(inodeResolver.resolve(this, diff));
		}

		for(PathDiff diff : diffset.pathDiffs.values()) {
			diff.setResolution(pathResolver.resolve(this, diff));
		}
	}
	
	protected void enforceDirectoryConsistency() throws IOException, DiffResolutionException {
		for(PathDiff diff : diffset.pathDiffs.values()) {
			if(diff.resolution == null || parentExists(diff.path)) continue;
			diff.setResolution(null);
		}
	}
	
	protected void applyResolutions() throws IOException {
		for(InodeDiff diff : diffset.inodeDiffs.values()) {
			fs.getInodeTable().replaceInode(diff);
		}
		
		// need to sort so we do parent directories before children
		List<PathDiff> pathList = new ArrayList<PathDiff>(diffset.pathDiffs.values());
		Collections.sort(pathList);
		
		for(PathDiff diff : pathList) {
			assert(diff.isResolved());
			ZKDirectory dir = fs.opendir(fs.dirname(diff.path));
			dir.updateLink(diff.resolution, fs.basename(diff.path));
			dir.close();
		}
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
