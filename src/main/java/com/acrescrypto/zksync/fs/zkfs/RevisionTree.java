package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;

public class RevisionTree {
	protected ArrayList<ObfuscatedRefTag> branchTips = new ArrayList<ObfuscatedRefTag>();
	protected ArrayList<RefTag> plainBranchTips = new ArrayList<RefTag>();
	protected ZKArchive archive;
	
	public RevisionTree(ZKArchive archive) throws IOException {
		this.archive = archive;
		try {
			read();
		} catch(ENOENTException exc) {
			branchTips = new ArrayList<ObfuscatedRefTag>();
		}
	}
	
	public ArrayList<ObfuscatedRefTag> branchTips() {
		return branchTips;
	}
	
	public void addBranchTip(RefTag newBranch) {
		branchTips.add(newBranch.obfuscate());
	}
	
	public void addBranchTip(ObfuscatedRefTag newBranch) {
		branchTips.add(newBranch);
	}
	
	public void removeBranchTip(RefTag oldBranch) {
		branchTips.remove(oldBranch.obfuscate());
	}
	
	public void removeBranchTip(ObfuscatedRefTag oldBranch) {
		branchTips.remove(oldBranch);
	}
	
	public HashSet<RefTag> ancestorsOf(RefTag revision) throws IOException {
		HashSet<RefTag> set = new HashSet<RefTag>();
		addAncestorsOf(revision, set);
		return set;
	}
	
	protected void addAncestorsOf(RefTag revision, HashSet<RefTag> set) throws IOException {
		if(revision == null) return;
		set.add(revision);
		
		Collection<RefTag> parents = revision.getInfo().parents;
		for(RefTag parent : parents) {
			addAncestorsOf(parent, set);
		}
	}
	
	public RefTag commonAncestorOf(RefTag[] revisions) throws IOException {
		HashSet<RefTag> allAncestors = null;
		for(RefTag rev : revisions) {
			Collection<RefTag> ancestors = ancestorsOf(rev);
			
			if(allAncestors == null) allAncestors = new HashSet<RefTag>(ancestors);
			else allAncestors.retainAll(ancestors);
		}
		
		RefTag youngest = null;
		for(RefTag tag : allAncestors) {
			if(youngest == null || tag.getInfo().generation > youngest.getInfo().generation) youngest = tag;
		}
		return youngest;
	}
	
	public String getPath() {
		return Paths.get(ZKArchive.REVISION_DIR, "branch-tips").toString();
	}
	
	public void write() throws IOException {
		MutableSecureFile
		  .atPath(archive.storage, getPath(), branchTipKey())
		  .write(serialize(), 65536);
	}
	
	protected void read() throws IOException {
		deserialize(MutableSecureFile
		  .atPath(archive.storage, getPath(), branchTipKey())
		  .read());
	}
	
	protected void deserialize(byte[] serialized) {
		branchTips.clear();
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		byte[] tag = new byte[ObfuscatedRefTag.sizeForArchive(archive)];
		while(buf.remaining() >= archive.refTagSize()) {
			buf.get(tag);
			branchTips.add(new ObfuscatedRefTag(archive, tag));
		}
		
		if(buf.hasRemaining()) throw new InvalidArchiveException("branch tip file appears corrupt: " + getPath());
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(ObfuscatedRefTag.sizeForArchive(archive)*branchTips.size());
		for(ObfuscatedRefTag tag : branchTips) buf.put(tag.serialize());
		return buf.array();
	}
	
	protected Key branchTipKey() {
		return archive.config.deriveKey(ArchiveAccessor.KEY_INDEX_LOCAL, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_REVISION_TREE);
	}
}
