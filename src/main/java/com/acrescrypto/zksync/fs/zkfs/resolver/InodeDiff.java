package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;

public class InodeDiff {
	protected HashMap<Inode,ArrayList<RevisionTag>> resolutions = new HashMap<Inode,ArrayList<RevisionTag>>();
	protected long inodeId;
	protected boolean resolved;
	protected Inode resolution;
	protected HashMap<RevisionTag,Long> originalInodeIds = new HashMap<>();
	protected RevisionTag canonicalSourceRevision;
	protected long canonicalOriginalInodeId;
	
	public InodeDiff(long inodeId) {
		this.inodeId = inodeId;
	}
	
	public InodeDiff(long inodeId, RevisionTag[] candidates) throws IOException {
		this.inodeId = inodeId;
		for(RevisionTag candidate : candidates) {
			try(ZKFS fs = candidate.readOnlyFS()) {
				Inode inode = fs.getInodeTable().inodeWithId(inodeId);
				if(inode.isDeleted()) inode = null;
				getResolutions().putIfAbsent(inode, new ArrayList<>());
				getResolutions().get(inode).add(candidate);
				originalInodeIds.put(candidate, inodeId);
			}
		}
	}
	
	public boolean isConflict() {
		return getResolutions().size() > 1;
	}
	
	public long getInodeId() {
		return inodeId;
	}

	public HashMap<Inode,ArrayList<RevisionTag>> getResolutions() {
		return resolutions;
	}
	
	public boolean isResolved() {
		return resolved;
	}
	
	public void setResolution(Inode resolution) {
		this.resolution = resolution;
		this.resolved = true;
		canonicalSourceRevision = resolutions.get(resolution).get(0);
		canonicalOriginalInodeId = originalInodeIdForTag(canonicalSourceRevision);
	}
	
	public Inode getResolution() {
		return resolution;
	}
	
	public String toString() {
		return "InodeDiff " + inodeId + " (" + resolutions.size() + " versions)";
	}

	public void add(Inode newInode, long inodeId, ArrayList<RevisionTag> tags) {
		ArrayList<RevisionTag> prunedTags = new ArrayList<>(tags);
		if(newInode == null) {
			// don't let nulls override non-null values in standardization
			prunedTags.removeIf((tag)->originalInodeIds.containsKey(tag));
		}
		
		resolutions.put(newInode, prunedTags);
		for(RevisionTag tag : prunedTags) {
			if(newInode == null) {
				originalInodeIds.put(tag, (long) -1);
			} else {
				originalInodeIds.put(tag, inodeId);
			}
		}
		
		/* when we standardize on one inodeId for an identity, we can wind up with the same tag
		 * appearing with a null solution and a non-null solution. Call pruneAlternatives to remove
		 * everything except the version we just added of each tag if we are non-null. (Don't prune to
		 * null so that the non-null versions override the nulls.)
		 */
		if(newInode != null) {
			pruneAlternatives(newInode, prunedTags);
		}
	}
	
	public void pruneAlternatives(Inode newInode, ArrayList<RevisionTag> tags) {
		for(Inode inode : resolutions.keySet()) {
			boolean match = inode == null ? inode == newInode : inode.equals(newInode);
			if(match) continue;
			ArrayList<RevisionTag> inodeTags = resolutions.get(inode);
			for(RevisionTag tag : tags) {
				while(inodeTags.remove(tag));
			}
		}
	}
	
	public String dump() {
		String s = "InodeDiff " + inodeId + ": " + resolutions.size() + " resolutions\n";
		for(Inode inode : resolutions.keySet()) {
			s += "  Inode " + (inode == null ? "null" : inode.getIdentity()) + ": " + resolutions.get(inode).size() + " reftags";
			if(resolved && resolution.equals(inode)) s += " (SELECTED)";
			s += "\n   ";
			for(RevisionTag tag : resolutions.get(inode)) s += " " + tag;
			s += "\n";
		}
		return s;
	}

	public long originalInodeIdForTag(RevisionTag tag) {
		return originalInodeIds.get(tag);
	}

	public long getCanonicalOriginalInodeId() {
		return canonicalOriginalInodeId;
	}

	public RevisionTag getCanonicalSourceRevision() {
		return canonicalSourceRevision;
	}
}
