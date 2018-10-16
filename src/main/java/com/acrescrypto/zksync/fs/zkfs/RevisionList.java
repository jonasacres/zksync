package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver;
import com.acrescrypto.zksync.utility.Util;

public class RevisionList {
	protected ArrayList<RevisionTag> branchTips = new ArrayList<>();
	protected ZKArchiveConfig config;
	protected RevisionTag latest;
	protected boolean automerge;
	private Logger logger = LoggerFactory.getLogger(RevisionList.class);
	
	public RevisionList(ZKArchiveConfig config) throws IOException {
		this.config = config;
		
		try {
			read();
		} catch(ENOENTException exc) {
			branchTips = new ArrayList<>();
		}
	}
	
	public ArrayList<RevisionTag> branchTips() {
		return branchTips;
	}
	
	public void addBranchTip(RevisionTag newBranch) throws IOException {
		/* TODO DHT: Handle a SearchFailedException more gracefully. Should we store the tag for
		 * later parent lookup? Accept it provisionally?
		 */
		if(config.revisionTree.isSuperceded(newBranch)) return;
		
		synchronized(this) {
			if(branchTips.contains(newBranch)) return;
			branchTips.add(newBranch);
			config.swarm.announceTip(newBranch);
			updateLatest(newBranch);
		}
		
		if(getAutomerge()) {
			try {
				executeAutomerge();
			} catch (DiffResolutionException exc) {
				logger.error("Unable to automerge with new branch " + newBranch + ": ", exc);
			}
		}
	}
	
	public void consolidate(RevisionTag newBranch) throws IOException {
		Collection<RevisionTag> tips, parents;
		ArrayList<RevisionTag> toRemove = new ArrayList<>();
		RevisionTree tree = config.getRevisionTree();
		parents = tree.parentsForTag(newBranch, RevisionTree.treeSearchTimeoutMs);
		
		synchronized(this) {
			tips = new ArrayList<>(branchTips);
		}
		
		for(RevisionTag tip : tips) {
			if(tip.equals(newBranch)) continue;
			if(tree.descendentOf(newBranch, tip)) {
				toRemove.add(tip);
			} else if(parents.containsAll(tree.parentsForTag(tip, RevisionTree.treeSearchTimeoutMs))) {
				toRemove.add(tip);
			}
		}
		
		synchronized(this) {
			for(RevisionTag tip : toRemove) {
				removeBranchTip(tip);
			}
		}
	}
	
	public synchronized void removeBranchTip(RevisionTag oldBranch) throws IOException {
		branchTips.remove(oldBranch);
		if(latest.equals(oldBranch)) recalculateLatest();
	}
	
	public String getPath() {
		return Paths.get(ZKArchive.REVISION_DIR, "revision-list").toString();
	}
	
	public void clear() throws IOException {
		branchTips.clear();
		write();
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
		latest = null;
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		
		while(buf.remaining() >= RevisionTag.sizeForConfig(config)) {
			byte[] rawTag = new byte[RevisionTag.sizeForConfig(config)];
			buf.get(rawTag);
			try {
				RevisionTag revTag = new RevisionTag(config, rawTag);
				branchTips.add(revTag);
				updateLatest(revTag);
			} catch (SecurityException exc) {
				logger.error("Invalid signature on stored revision tag; skipping", exc);
			}
		}
		
		if(buf.hasRemaining()) throw new InvalidArchiveException("branch tip file appears corrupt: " + getPath());
	}
	
	protected synchronized byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(RevisionTag.sizeForConfig(config)*branchTips.size());
		for(RevisionTag tag : branchTips) {
			buf.put(tag.serialize());
		}
		return buf.array();
	}
	
	protected Key branchTipKey() {
		return config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_REVISION_LIST);
	}
	
	public void setAutomerge(boolean automerge) {
		this.automerge = automerge;
	}
	
	public boolean getAutomerge() {
		return automerge;
	}
	
	public synchronized void dump() {
		System.out.println(Util.bytesToHex(config.swarm.getPublicIdentityKey().getBytes(), 6) + " Revision list: " + branchTips.size());
		int i = 0;
		ArrayList<RevisionTag> sortedTips = new ArrayList<>(branchTips());
		sortedTips.sort(null);
		for(RevisionTag tag : sortedTips) {
			i++;
			System.out.println("\t" + i + ": " + tag);
		}
	}
	
	public RevisionTag latest() {
		return latest;
	}
	
	protected void updateLatest(RevisionTag newTip) {
		if(latest == null || newTip.compareTo(latest) > 0) {
			latest = newTip;
		}		
	}
	
	protected void recalculateLatest() {
		latest = null;
		for(RevisionTag tip : branchTips) {
			if(latest == null || tip.compareTo(latest) > 0) {
				latest = tip;
			}
		}
	}
	
	protected RevisionTag executeAutomerge() throws IOException, DiffResolutionException {
		return DiffSetResolver.canonicalMergeResolver(config.getArchive()).resolve();
	}
}
