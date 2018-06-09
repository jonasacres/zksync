package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.exceptions.InvalidSignatureException;

public class RevisionTree {
	protected ArrayList<ObfuscatedRefTag> branchTips = new ArrayList<ObfuscatedRefTag>();
	protected ArrayList<RefTag> plainBranchTips = new ArrayList<RefTag>();
	protected ZKArchiveConfig config;
	private Logger logger = LoggerFactory.getLogger(RevisionTree.class);
	
	public RevisionTree(ZKArchiveConfig config) throws IOException {
		this.config = config;
		try {
			read();
		} catch(ENOENTException exc) {
			branchTips = new ArrayList<ObfuscatedRefTag>();
		}
	}
	
	public ArrayList<ObfuscatedRefTag> branchTips() {
		return branchTips;
	}
	
	public ArrayList<RefTag> plainBranchTips() {
		return plainBranchTips;
	}
	
	public synchronized void addBranchTip(RefTag newBranch) throws IOException {
		plainBranchTips.add(newBranch);
		branchTips.add(newBranch.obfuscate());
	}
	
	public synchronized void addBranchTip(ObfuscatedRefTag newBranch) throws InvalidSignatureException {
		newBranch.assertValid();
		branchTips.add(newBranch);
		if(!config.accessor.isSeedOnly()) {
			plainBranchTips.add(newBranch.reveal());
		}
	}
	
	public synchronized void removeBranchTip(RefTag oldBranch) throws IOException {
		plainBranchTips.remove(oldBranch);
		branchTips.remove(oldBranch.obfuscate());
	}
	
	public synchronized void removeBranchTip(ObfuscatedRefTag oldBranch) {
		branchTips.remove(oldBranch);
		if(!config.accessor.isSeedOnly()) {
			try {
				plainBranchTips.remove(oldBranch.reveal());
			} catch (InvalidSignatureException exc) {
				logger.info("Signature verification failed on obfuscated reftag meant for deletion", exc);
			}
		}
	}
	
	public HashSet<RefTag> ancestorsOf(RefTag revision) throws IOException {
		HashSet<RefTag> set = new HashSet<RefTag>();
		addAncestorsOf(revision, set);
		return set;
	}
	
	protected synchronized void addAncestorsOf(RefTag revision, HashSet<RefTag> set) throws IOException {
		if(revision == null) return;
		set.add(revision);
		
		Collection<RefTag> parents = revision.getInfo().parents;
		for(RefTag parent : parents) {
			addAncestorsOf(parent, set);
		}
	}
	
	public synchronized RefTag commonAncestorOf(RefTag[] revisions) throws IOException {
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
	
	public synchronized void write() throws IOException {
		MutableSecureFile
		  .atPath(config.localStorage, getPath(), branchTipKey())
		  .write(serialize(), 65536);
	}
	
	public synchronized void read() throws IOException {
		deserialize(MutableSecureFile
		  .atPath(config.localStorage, getPath(), branchTipKey())
		  .read());
	}
	
	protected synchronized void deserialize(byte[] serialized) {
		branchTips.clear();
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		byte[] tag = new byte[ObfuscatedRefTag.sizeForConfig(config)];
		while(buf.remaining() >= config.refTagSize()) {
			buf.get(tag);
			try {
				ObfuscatedRefTag obfTag = new ObfuscatedRefTag(config, tag);
				obfTag.assertValid();
				branchTips.add(obfTag);
				plainBranchTips.add(obfTag.reveal());
			} catch (InvalidSignatureException exc) {
				logger.error("Invalid signature on stored obfuscating reftag; skipping", exc);
			}
		}
		
		if(buf.hasRemaining()) throw new InvalidArchiveException("branch tip file appears corrupt: " + getPath());
	}
	
	protected synchronized byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(ObfuscatedRefTag.sizeForConfig(config)*branchTips.size());
		for(ObfuscatedRefTag tag : branchTips) {
			try {
				tag.assertValid();
			} catch (InvalidSignatureException exc) {
				logger.error("Invalid signature on obfuscated reftag; omitting from storage", exc);
			}
			buf.put(tag.serialize());
		}
		return buf.array();
	}
	
	protected Key branchTipKey() {
		return config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_REVISION_TREE);
	}
}
