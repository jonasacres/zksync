package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.SearchFailedException;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.RevisionInfo;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKDirectory;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.utility.Util;

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
	Logger logger = LoggerFactory.getLogger(DiffSetResolver.class);
	
	public static DiffSetResolver canonicalMergeResolver(ZKArchive archive) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("DiffSetResolver %s: Assembling branch tips.\nBranch tips:\n", archive.getMaster().getName()));
		Collection<RevisionTag> tips = archive.getConfig().getRevisionList().branchTips();
		for(RevisionTag tag : tips) {
			sb.append(String.format("\t%s\n", Util.formatRevisionTag(tag)));
		}
		
		sb.append("Minimal tips:\n");
		Collection<RevisionTag> minimalTips = archive.getConfig().getRevisionTree().minimalSet(tips);
		for(RevisionTag tag : minimalTips) {
			sb.append(String.format("\t%s\n", Util.formatRevisionTag(tag)));
		}
		
		sb.append("Base tips:\n");
		Collection<RevisionTag> baseTips = archive.getConfig().getRevisionTree().canonicalBases(minimalTips);
		for(RevisionTag tag : baseTips) {
			sb.append(String.format("\t%s\n", Util.formatRevisionTag(tag)));
		}
		
		Util.debugLog(sb.toString());
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
			if(diff.getResolutions().size() == 2 && diff.getResolutions().containsKey(null)) {
				/* In this case, the only disagreement is whether the path exists or not -- everyone who
				 * says it exists agrees on the inode. Since this comes AFTER inode renumbering, we also
				 * know that they also agree on inode identity.
				 * 
				 * This comes up when:
				 *   - someone deletes a pre-existing file in one side of a fork
				 *      (we want to delete in the merge to iff it wasn't also modified on the other side)
				 *   - someone creates a file in one side of a fork (we want to keep it)
				 * */
				ArrayList<RevisionTag> revsWithPath = null, revsWithoutPath = null;
				long inodeId = -1, inodeIdentity = -1, originalInodeId = -1;
				boolean existed;
				RevisionTag ancestor;
				
				// figure out which revisions have the path and which don't
				for(Long listedInodeId : diff.getResolutions().keySet()) {
					ArrayList<RevisionTag> revs = diff.getResolutions().get(listedInodeId);
					if(listedInodeId == null) {
						revsWithoutPath = revs;
					} else {
						inodeId = listedInodeId;
						revsWithPath = revs;
					}
				}
				
				// get the inode identity (remember, this is after renumbering, so all revsWithPath agree on this)
				try(ZKFS fsWithPath = revsWithPath.get(0).readOnlyFS()) {
					inodeIdentity = fsWithPath.inodeForPath(diff.path, false).getIdentity();
				} catch(ENOENTException exc) {
					StringBuilder sb = new StringBuilder();
					sb.append(String.format("DiffSetResolver %s: ENOENTException on path %s in revision %s\n%d revisions with path %s (inodeId %d):",
							revsWithPath.get(0).getArchive().getMaster().getName(),
							diff.path,
							Util.formatRevisionTag(revsWithPath.get(0)),
							revsWithPath.size(),
							diff.path,
							inodeId));
					
					for(RevisionTag tag : revsWithPath) {
						sb.append(String.format("\t%s\n", Util.formatRevisionTag(tag)));
					}
					
					sb.append(String.format("%d revisions without path %s:\n",
							revsWithoutPath.size(),
							diff.path));
					for(RevisionTag tag : revsWithoutPath) {
						sb.append(String.format("\t%s\n", Util.formatRevisionTag(tag)));
					}
					exc.printStackTrace();
					Util.debugLog(sb.toString());
					throw exc;
				}
				
				/* now figure out if the path was created before or after the fork by seeing if it exists
				 * in the common ancestor */
				try {
					ancestor = setResolver.commonAncestor();
					try(ZKFS fs = ancestor.readOnlyFS()) {
						existed = fs.exists(diff.path);
					}
					
					if(!existed) return inodeId; // not in ancestor => path created after the fork => keep it
				} catch (SearchFailedException exc) {
					throw new DiffResolutionException("Cannot find common ancestor to resolve merge");
				}
								
				// if the inode was renumbered, get the original inodeId so we can find look it up
				InodeDiff inodeDiff = setResolver.diffset.inodeDiffs.get(inodeId);
				originalInodeId = inodeDiff != null ? inodeDiff.originalInodeId : inodeId;
				
				for(RevisionTag tag : revsWithoutPath) {
					// if one of the revisions without this path has the same inode, then it must have moved
					try(ZKFS fs = tag.readOnlyFS()) {
						Inode inode = fs.getInodeTable().inodeWithId(originalInodeId);
						if(inode.isDeleted()) continue;
						if(inode.getIdentity() == inodeIdentity) return null; // we moved the inode
					}
				}
				
				// it was created before the fork and no one moved it, so did someone edit it?
				for(RevisionTag tag : revsWithPath) {
					try(ZKFS fs = tag.readOnlyFS()) {
						long changeHeight = fs.inodeForPath(diff.path).getChangedFrom().getHeight() + 1;
						if(changeHeight > ancestor.getHeight()) return inodeId; // edited; keep the file.
					}
				}
				
				// the file is deleted on one side of the fork, and unchanged on the other. keep the deletion.
				return null;
			}
			
			/* We have a path conflict between multiple inodeIds. In this case, we will not accept a resolution
			 * where the path is unlinked, which simplifies the logic considerably. We'll pick whichever inode
			 * sorts "highest" using compareTo, which is objective and typically the most recently modified.
			 */
			Inode result = null;
			for(Long inodeId : diff.getResolutions().keySet()) {
				if(inodeId == null) continue;

				Inode inode = null;
				try {
					InodeDiff idiff = setResolver.diffset.inodeDiffs.getOrDefault(inodeId, null);
					if(idiff == null) {
						// no inode diff for this id; so any reftag's version will work since they're all identical
						inode = setResolver.fs.getInodeTable().inodeWithId(inodeId);
					} else {
						/* there WAS an inode diff, but path diffs are resolved after inode diffs, so a resolution
						 * must be available */
						inode = idiff.getResolution();
					}
				} catch (IOException exc) {
					throw new IllegalStateException("Encountered exception resolving path collision information for inode " + inodeId);
				}
				
				// take the highest-sorted inode as the result
				if(result == null || result.compareTo(inode) < 0) result = inode;
			}
			
			return result != null ? result.getStat().getInodeId() : null;
		};
	}
	
	public DiffSetResolver(DiffSet diffset, InodeDiffResolver inodeResolver, PathDiffResolver pathResolver) throws IOException {
		this.diffset = diffset;
		this.inodeResolver = inodeResolver;
		this.pathResolver = pathResolver;
		if(diffset.revisions.length > 1) {
			// no need to open an FS if we're not going to have to merge anything
			this.fs = new ZKFS(diffset.latestRevision());
		}
	}
	
	public RevisionTag resolve() throws IOException, DiffResolutionException {
		ZKArchiveConfig config = diffset.revisions[0].getArchive().getConfig();
		
		String revList = "";
		for(RevisionTag rev : diffset.revisions) {
			if(revList.length() != 0) revList += ", ";
			revList += Util.formatRevisionTag(rev);
		}
		
		logger.info("Diff {}: Merging {} revisions ({})",
				Util.formatArchiveId(config.getArchiveId()),
				diffset.revisions.length,
				revList);
		try {
			Util.debugLog("Diff " + fs.getArchive().getMaster().getName() + ": Merging " + revList);
		} catch(NullPointerException exc) {}
		
		try {
			if(diffset.revisions.length == 1) {
				config.getRevisionList().consolidate(diffset.revisions[0]);
				return diffset.revisions[0];
			}
			
			if(diffset.revisions.length > RevisionInfo.maxParentsForConfig(fs.getArchive().getConfig())) {
				return resolveExcessive();
			}
			
			selectResolutions();
			applyResolutions();
			RevisionTag revTag = fs.commitWithTimestamp(diffset.revisions, 0);
			fs.getArchive().getConfig().getRevisionList().consolidate(revTag);
			Util.debugLog("Diff " + fs.getArchive().getMaster().getName() + ": Produced merged revision " + Util.formatRevisionTag(revTag) + " from " + revList);
			return revTag;
		} catch(Throwable exc) {
			throw exc;
		} finally {
			if(fs != null && !fs.isClosed()) {
				fs.close();
			}
		}
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
		fs.close();
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
		try(ZKFS originalFs = fs.getBaseRevision().readOnlyFS()) {
			dump();
			for(InodeDiff diff : diffset.inodeDiffs.values()) {
				Util.debugLog("DiffSetResolver " + fs.getArchive().getMaster().getName() + ": replace inode " + diff.getInodeId());
				fs.getInodeTable().replaceInode(diff);
			}
			
			List<PathDiff> sortedDiffs = sortedPathDiffs();
			for(PathDiff diff : sortedDiffs) { // need to sort so we do parent directories before children
				assert(diff.isResolved());
				if(!parentExists(diff.path)) continue;
				try(ZKDirectory dir = fs.opendir(fs.dirname(diff.path))) {
					dir.setOverrideMtime(fs.getInodeTable().getStat().getMtime());
					dir.updateLink(diff.resolution, fs.basename(diff.path));
				}
			}
			
			fs.getInodeTable().rebuildLinkCounts();
		}
	}
	
	public void dump() {
		StringBuilder sb = new StringBuilder();
		sb.append("DiffSetResolver " + fs.getArchive().getMaster().getName() + ": " + diffset.revisions.length + " revisions\n");
		for(RevisionTag tag : diffset.revisions) {
			String symbol = tag.equals(fs.getBaseRevision()) ? "* " : "  "; 
			sb.append("\t" + symbol + Util.formatRevisionTag(tag) + "\n");
		}
		sb.append("\n");
		
		sb.append("Common ancestor: " + Util.formatRevisionTag(commonAncestor) + "\n");
		sb.append("Affected inodes: " + diffset.inodeDiffs.size() + "\n");
		for(InodeDiff diff : diffset.inodeDiffs.values()) {
			sb.append("Inode diff: inodeId " + diff.inodeId + " (" + diff.resolutions.size() + " versions, original inodeId " + diff.originalInodeId + ")\n");
			diff.resolutions.forEach((inode, tags)->{
				String tagStr = "";
				for(RevisionTag tag : tags) {
					if(tagStr.length() > 0) tagStr += " ";
					tagStr += Util.formatRevisionTag(tag);
				}
				
				if(inode != null) {
					sb.append(String.format("\t%sIdentity %016x, type %02x, mtime %19d, size %8d, nlink %d, changed from %s [%s]\n",
							inode.equals(diff.resolution) ? "* " : "  ",
							inode.getIdentity(),
							inode.getStat().getType(),
							inode.getStat().getMtime(),
							inode.getStat().getSize(),
							inode.getNlink(),
							Util.formatRevisionTag(inode.getChangedFrom()),
							tagStr));
				} else {
					sb.append(String.format("\t%sDeleted [%s]\n",
							diff.resolution == null ? "* " : "  ",
							tagStr));
				}
				
				HashSet<String> inodePaths = new HashSet<>();
				for(RevisionTag tag : tags) {
					try(
						ZKFS tfs = tag.readOnlyFS();
						ZKDirectory dir = tfs.opendir("/")
					) {
						inodePaths.addAll(dir.findPathsForInode(diff.inodeId));
					} catch(IOException exc) {
						exc.printStackTrace();
					}
				}
				
				for(String path : inodePaths) {
					sb.append("\t\t" + path + "\n");
				}
			});
		}
		
		sb.append("Affected paths: " + diffset.pathDiffs.size() + "\n");
		for(PathDiff diff : diffset.pathDiffs.values()) {
			sb.append("\t" + diff.path + " (" + diff.resolutions.size() + " versions)\n");
			diff.resolutions.forEach((inodeId, tags)->{
				String tagStr = "";
				for(RevisionTag tag : tags) {
					if(tagStr.length() > 0) tagStr += " ";
					tagStr += Util.formatRevisionTag(tag);
				}
				
				sb.append(String.format("\t\t%sinodeId %d [%s]\n",
						diff.resolution == inodeId ? "* " : "  ",
						inodeId,
						tagStr));
			});
		}
		
		Util.debugLog(sb.toString());
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
