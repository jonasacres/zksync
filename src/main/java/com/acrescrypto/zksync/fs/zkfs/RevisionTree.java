package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.bouncycastle.util.Arrays;

import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.fs.Directory;

public class RevisionTree {
	protected HashMap<Long,ArrayList<RevisionTag>> tree = new HashMap<Long,ArrayList<RevisionTag>>();
	protected HashMap<Long,ArrayList<RevisionTag>> byPrefix = new HashMap<Long,ArrayList<RevisionTag>>();
	protected ZKFS fs;
	protected int size;
	
	public RevisionTree(ZKFS fs) {
		this.fs = fs;
	}
	
	public void scan() throws IOException {
		Directory revisionDir = fs.storage.opendir(ZKFS.REVISION_DIR);
		tree.clear();
		for(String revPath : revisionDir.listRecursive(Directory.LIST_OPT_OMIT_DIRECTORIES)) {
			recordEntry(revPath);
		}
	}
	
	public int size() {
		return size;
	}
	
	public ArrayList<RevisionTag> revisionTags() {
		ArrayList<RevisionTag> allRevisions = new ArrayList<RevisionTag>();
		for(ArrayList<RevisionTag> childList : tree.values()) {
			allRevisions.addAll(childList);
		}
		
		return allRevisions;
	}
	
	public ArrayList<RevisionTag> rootRevisions() {
		return tree.getOrDefault(0l, new ArrayList<RevisionTag>());
	}
	
	public RevisionTag earliestRoot() {
		RevisionTag earliest = null;
		for(RevisionTag root : rootRevisions()) {
			if(earliest == null || root.getTimestamp() < earliest.getTimestamp()) earliest = root;
		}
		
		return earliest;
	}
	
	public RevisionTag defaultRevision() {
		return heirOf(earliestRoot());
	}
	
	public ArrayList<RevisionTag> descendantsOf(RevisionTag tag) {
		return tree.getOrDefault(tag.getShortTag(), new ArrayList<RevisionTag>());
	}
	
	public ArrayList<RevisionTag> leaves() {
		ArrayList<RevisionTag> leaves = new ArrayList<RevisionTag>();
		for(RevisionTag tag : revisionTags()) {
			if(tree.containsKey(tag.getShortTag())) continue;
			leaves.add(tag);
		}
		
		return leaves;
	}
	
	public RevisionTag firstDescendantOf(RevisionTag tag) {
		RevisionTag best = null;
		for(RevisionTag descendent : descendantsOf(tag)) {
			if(best == null || best.getTimestamp() > descendent.getTimestamp()) best = descendent;
			else if(best.getTimestamp() == descendent.getTimestamp()) {
				if(Arrays.compareUnsigned(best.getTag(), descendent.getTag()) < 0) best = descendent;
			}
		}
		
		return best;
	}
	
	public RevisionTag heirOf(RevisionTag tag) {
		// if a tag has children, the heir of the tag is the heir of the earliest child.
		// if a tag has no children, the tag is its own heir.
		RevisionTag heir = firstDescendantOf(tag);
		while(heir != null) {
			tag = heir;
			heir = firstDescendantOf(tag);
		}
		
		return tag;
	}
	
	public ArrayList<RevisionTag> ancestorsOf(RevisionTag tag) throws IOException {
		ArrayList<RevisionTag> list = new ArrayList<RevisionTag>();
		list.add(tag);
		
		if(!tag.hasMultipleParents()) {
			list.addAll(ancestorsOf(parentOf(tag)));
		} else {
			Revision rev = new Revision(tag);
			for(RevisionTag parent : rev.parents) {
				list.addAll(ancestorsOf(parent));
			}
		}
		
		return list;
	}
	
	public RevisionTag parentOf(RevisionTag tag) {
		ArrayList<RevisionTag> candidates = byPrefix.getOrDefault(tag.parentShortTag, null);
		if(candidates.size() == 0) return null;
		if(candidates.size() == 1) return candidates.get(0);
		
		ArrayList<RevisionTag> winnowed = new ArrayList<RevisionTag>();
		for(RevisionTag candidate : candidates) {
			if(candidate.generation == tag.generation-1) winnowed.add(candidate);
		}
		
		if(winnowed.size() == 1) return candidates.get(0);
		
		RevisionTag youngest = null;
		for(RevisionTag candidate : candidates) {
			if(youngest == null || candidate.timestamp < youngest.timestamp) youngest = candidate;
			else if(Arrays.compareUnsigned(youngest.tag, candidate.tag) < 0) youngest = candidate;
		}
		return youngest;
	}
	
	public RevisionTag commonAncestorOf(RevisionTag[] tags) throws IOException {
		HashSet<RevisionTag> allAncestors = null;
		for(RevisionTag tag : tags) {
			Collection<RevisionTag> ancestors = ancestorsOf(tag);
			if(allAncestors == null) allAncestors = new HashSet<RevisionTag>(ancestors);
			else allAncestors.retainAll(ancestors);
		}
		
		RevisionTag youngest = null;
		for(RevisionTag tag : allAncestors) {
			if(youngest == null || tag.generation > youngest.generation) youngest = tag;
		}
		return youngest;
	}
	
	protected void recordEntry(String revPath) throws IOException {
		RevisionTag revTag = new RevisionTag(fs,revPath);
		
		if(revTag.hasMultipleParents()) {
			Revision rev = new Revision(revTag);
			for(RevisionTag parent : rev.parents) {
				addParentRef(parent.getShortTag(), revTag, tree);
				addParentRef(parent.getShortTag(), revTag, byPrefix);
			}
		} else {
			addParentRef(revTag.getParentShortTag(), revTag, tree);
			addParentRef(revTag.getParentShortTag(), revTag, byPrefix);
		}
		
		size += 1;
	}
	
	protected void addParentRef(long parentShortRef, RevisionTag tag, HashMap<Long, ArrayList<RevisionTag>> map) throws EINVALException {
		ArrayList<RevisionTag> list = map.getOrDefault(parentShortRef, null);
		if(list == null) {
			list = new ArrayList<RevisionTag>();
			map.put(parentShortRef, list);
		}
		list.add(tag);
	}
}
