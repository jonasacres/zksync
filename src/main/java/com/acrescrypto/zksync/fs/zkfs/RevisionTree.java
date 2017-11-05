package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.bouncycastle.util.Arrays;

import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.fs.Directory;

public class RevisionTree {
	protected HashMap<Long,ArrayList<RevisionTag>> tree = new HashMap<Long,ArrayList<RevisionTag>>();
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
		// TODO: what if we preferentially follow the parent author in some way?
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
	
	protected void recordEntry(String revPath) throws IOException {
		RevisionTag revTag = new RevisionTag(fs,revPath);
		
		if(revTag.hasMultipleParents()) {
			Revision rev = new Revision(revTag);
			for(RevisionTag parent : rev.parents) {
				addParentRef(parent.getShortTag(), revTag);
			}
		} else {
			addParentRef(revTag.getParentShortTag(), revTag);
		}
		
		size += 1;
	}
	
	protected void addParentRef(long parentShortRef, RevisionTag tag) throws EINVALException {
		ArrayList<RevisionTag> list = tree.getOrDefault(parentShortRef, null);
		if(list == null) {
			list = new ArrayList<RevisionTag>();
			tree.put(parentShortRef, list);
		}
		list.add(tag);
	}
}
