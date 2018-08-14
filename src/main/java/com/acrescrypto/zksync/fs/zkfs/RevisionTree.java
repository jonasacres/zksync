package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.exceptions.InvalidSignatureException;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver;
import com.acrescrypto.zksync.utility.Util;

public class RevisionTree {
	protected ArrayList<ObfuscatedRefTag> branchTips = new ArrayList<ObfuscatedRefTag>();
	protected ArrayList<RefTag> plainBranchTips = new ArrayList<RefTag>();
	protected HashMap<ObfuscatedRefTag,Long> prunedTips = new HashMap<>();
	protected ZKArchiveConfig config;
	protected RefTag latest;
	protected boolean automerge;
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
		if(plainBranchTips.contains(newBranch)) return;
		ObfuscatedRefTag obf = newBranch.obfuscate();
		if(prunedTips.containsKey(obf)) return;
		
		plainBranchTips.add(newBranch);
		branchTips.add(obf);
//		System.out.println(Util.bytesToHex(config.swarm.getPublicIdentityKey().getBytes(), 6) + " added " + newBranch.hasFlag(RefTag.FLAG_NO_NEW_CONTENT) + " " + Util.bytesToHex(newBranch.obfuscate().serialize(), 4) + " - " + branchTips.size());
//		if(automerge && !newBranch.hasFlag(RefTag.FLAG_NO_NEW_CONTENT)) executeAutomerge();
//		if(latest == null || latest.compareTo(newBranch) < 0 || tagSupercedes(newBranch, latest)) {
//			latest = newBranch;
//		}
	}
	
	public synchronized void addBranchTip(ObfuscatedRefTag newBranch) throws InvalidSignatureException, IOException {
		if(branchTips.contains(newBranch) || prunedTips.containsKey(newBranch)) return;
		if(!config.accessor.isSeedOnly()) {
			addBranchTip(newBranch.reveal());
			return;
		}

		newBranch.assertValid();
		if(branchTips.contains(newBranch)) return;
		branchTips.add(newBranch);
		config.swarm.announceTip(newBranch);
	}
	
	public synchronized void removeBranchTip(RefTag oldBranch) throws IOException {
		ObfuscatedRefTag obf = oldBranch.obfuscate();
		prunedTips.put(obf, Util.currentTimeMillis());
		plainBranchTips.remove(oldBranch);
		branchTips.remove(obf);
	}
	
	public synchronized void removeBranchTip(ObfuscatedRefTag oldBranch) {
		prunedTips.put(oldBranch, Util.currentTimeMillis());
		branchTips.remove(oldBranch);
		if(!config.accessor.isSeedOnly()) {
			try {
				plainBranchTips.remove(oldBranch.reveal());
			} catch (InvalidSignatureException exc) {
				logger.info("Signature verification failed on obfuscated reftag meant for deletion", exc);
			}
		}
	}
	
	public boolean tagHasAncestor(RefTag tag, RefTag ancestor) throws IOException {
		if(tag.equals(ancestor)) return true;
		
		for(RefTag parent : tag.getInfo().parents) {
			if(parent.equals(ancestor)) return true;
		}
		
		for(RefTag parent : tag.getInfo().parents) {
			if(tagHasAncestor(parent, ancestor)) return true;
		}
		
		return false;
	}
	
	public boolean tagSupercedes(RefTag tag, RefTag other) throws IOException {
		if(tagHasAncestor(tag, other)) return true;
		
		// this is redundant with the tagHasAncestor check, but faster if we're merging parallel changes to the same generation, common in automerges
		if(tag.getInfo().getParents().containsAll(other.getInfo().getParents())) return true;
		
		for(RefTag parent : other.getInfo().getParents()) {
			if(!tagHasAncestor(tag, parent)) return false;
		}
		
		return true;
	}
	
	public HashSet<RefTag> ancestorsOf(RefTag revision) throws IOException {
		HashSet<RefTag> set = new HashSet<RefTag>();
		addAncestorsOf(revision, set);
		return set;
	}
	
	public synchronized void consolidate() throws IOException {
		HashSet<RefTag> obsoleted = new HashSet<RefTag>();
		
		for(RefTag tag : plainBranchTips) {
			for(RefTag otherTag : plainBranchTips) {
				if(otherTag == tag) continue;
				if(tagSupercedes(tag, otherTag)) {
					obsoleted.add(otherTag);
				}
			}
		}
		
		for(RefTag obsoleteTag : obsoleted) {
			removeBranchTip(obsoleteTag);
			System.out.println(Util.bytesToHex(config.swarm.getPublicIdentityKey().getBytes(), 6) + " pruned " + obsoleteTag.hasFlag(RefTag.FLAG_NO_NEW_CONTENT) + " " + Util.bytesToHex(obsoleteTag.obfuscate().serialize(), 4) + " - " + branchTips.size());
		}
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
				RefTag refTag = obfTag.reveal();
				plainBranchTips.add(refTag);
				// if(latest == null || latest.compareTo(refTag) < 0) latest = refTag;
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
	
	public void setAutomerge(boolean automerge) {
		this.automerge = automerge;
	}
	
	public boolean getAutomerge() {
		return automerge;
	}
	
	public synchronized void dump() {
		System.out.println(Util.bytesToHex(config.swarm.getPublicIdentityKey().getBytes(), 6) + " Revision tree: " + branchTips.size());
		int i = 0;
		for(ObfuscatedRefTag tag : branchTips()) {
			i++;
			System.out.println("\t" + i + ": " + Util.bytesToHex(tag.serialize(), 4));
			try {
				for(RefTag ancestor : ancestorsOf(tag.reveal())) {
					System.out.println("\t\t" + Util.bytesToHex(ancestor.obfuscate().serialize(), 4));
				}
			} catch(Exception exc) {
				System.out.println("\t\tCan't dump tags " + exc.getClass());
			}
		}
	}
	
	public RefTag latest() {
		return latest;
	}
	
	protected void executeAutomerge() {
		try {
			// TODO DHT: (test) Test automerges
			RefTag tag = DiffSetResolver.canonicalMergeResolver(config.getArchive()).resolve();
			consolidate();
			System.out.println(Util.bytesToHex(config.swarm.getPublicIdentityKey().getBytes(), 6) + " Merged into " + Util.bytesToHex(tag.obfuscate().serialize(), 4) + " - " + branchTips.size() + " total");
		} catch (DiffResolutionException | IOException exc) {
			logger.error("Error automerging branch tip", exc);
		}
	}
}
