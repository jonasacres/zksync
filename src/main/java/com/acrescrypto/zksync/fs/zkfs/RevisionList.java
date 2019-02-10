package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;

public class RevisionList {
	public interface RevisionMonitor {
		public void notifyNewRevision(RevisionTag revTag);
	}
	
	public final static long DEFAULT_AUTOMERGE_DELAY_MS = 10000;
	public final static long DEFAULT_MAX_AUTOMERGE_DELAY_MS = 60000;
	
	protected ArrayList<RevisionTag> branchTips = new ArrayList<>();
	protected LinkedList<RevisionMonitor> monitors = new LinkedList<>();
	protected ZKArchiveConfig config;
	protected RevisionTag latest; // "latest" tip; understood to mean tip with greatest height, using hash comparison as tiebreaker.
	protected boolean automerge;
	protected SnoozeThread automergeSnoozeThread;
	public long automergeDelayMs = DEFAULT_AUTOMERGE_DELAY_MS;
	public long maxAutomergeDelayMs = DEFAULT_MAX_AUTOMERGE_DELAY_MS;
	private Logger logger = LoggerFactory.getLogger(RevisionList.class);
	
	public RevisionList(ZKArchiveConfig config) throws IOException {
		this.config = config;
		
		try {
			read();
		} catch(ENOENTException exc) {
			branchTips = new ArrayList<>();
		}
	}
	
	public synchronized ArrayList<RevisionTag> branchTips() {
		return new ArrayList<>(branchTips);
	}
	
	public void addMonitor(RevisionMonitor monitor) {
		this.monitors.add(monitor);
	}
	
	public void removeMonitor(RevisionMonitor monitor) {
		this.monitors.remove(monitor);
	}
	
	public boolean addBranchTip(RevisionTag newBranch) throws IOException {
		if(config.revisionTree.isSuperceded(newBranch)) return false;
		
		synchronized(this) {
			if(config.revisionTree.isSuperceded(newBranch)) return false;
			logger.info("FS {}: Adding branch tip {} to revision list",
					Util.formatArchiveId(config.archiveId),
					Util.formatRevisionTag(newBranch));
			if(branchTips.contains(newBranch)) return false;
			branchTips.add(newBranch);
			config.swarm.announceTip(newBranch);
			updateLatest(newBranch);
		}
		
		for(RevisionMonitor monitor : monitors) {
			monitor.notifyNewRevision(newBranch);
		}
		
		if(getAutomerge() && branchTips.size() > 1) {
			try {
				queueAutomerge();
			} catch (DiffResolutionException exc) {
				logger.error("FS {}: Unable to automerge with new branch {}: ",
						Util.formatArchiveId(config.archiveId),
						newBranch,
						exc);
			}
		}
		
		return true;
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
			} else {
				Collection<RevisionTag> tipParents = tree.parentsForTag(tip, RevisionTree.treeSearchTimeoutMs);
				if(parents.size() > tipParents.size() && parents.containsAll(tipParents)) {
					toRemove.add(tip);
				}
			}
		}
		
		synchronized(this) {
			for(RevisionTag tip : toRemove) {
				removeBranchTip(tip);
			}
		}
	}
	
	public void consolidate() throws IOException {
		ArrayList<RevisionTag> tips, toRemove = new ArrayList<>();
		RevisionTree tree = config.getRevisionTree();

		synchronized(this) {
			tips = new ArrayList<>(branchTips);
		}
		
		for(RevisionTag tip : tips) {
			if(tree.isSuperceded(tip)) {
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
		logger.info("FS {}: Removed branch tip {}",
				Util.formatArchiveId(config.archiveId),
				Util.formatRevisionTag(oldBranch));
		branchTips.remove(oldBranch);
		if(latest != null && latest.equals(oldBranch)) recalculateLatest();
	}
	
	public String getPath() {
		return Paths.get(ZKArchive.REVISION_DIR, "revision-list").toString();
	}
	
	public void clear() throws IOException {
		branchTips.clear();
		latest = null;
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
				RevisionTag revTag = new RevisionTag(config, rawTag, false);
				branchTips.add(revTag);
				updateLatest(revTag);
			} catch (SecurityException exc) {
				logger.error("FS {}: Invalid signature on stored revision tag; skipping",
						Util.formatArchiveId(config.archiveId),
						exc);
			}
		}
		
		if(buf.hasRemaining()) throw new InvalidArchiveException("branch tip file appears corrupt: " + getPath());
	}
	
	protected synchronized byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(RevisionTag.sizeForConfig(config)*branchTips.size());
		for(RevisionTag tag : branchTips) {
			buf.put(tag.getBytes());
		}
		return buf.array();
	}
	
	protected Key branchTipKey() {
		return config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, "easysafe-revision-list-key");
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
		ArrayList<RevisionTag> sortedTips = branchTips();
		sortedTips.sort(null);
		for(RevisionTag tag : sortedTips) {
			i++;
			System.out.println("\t" + i + ": " + tag);
		}
	}
	
	public RevisionTag latest() {
		return latest == null && !config.isReadOnly() ? RevisionTag.blank(config) : latest;
	}
	
	protected void updateLatest(RevisionTag newTip) {
		if(latest == null || newTip.compareTo(latest) > 0) {
			logger.info("FS {}: New latest revtag {}, was {}",
					Util.formatArchiveId(config.archiveId),
					Util.formatRevisionTag(newTip),
					latest != null ? Util.formatRevisionTag(latest) : "null");
			latest = newTip;
		}
	}
	
	protected void recalculateLatest() {
		latest = null;
		for(RevisionTag tip : branchTips) {
			if(latest == null || tip.compareTo(latest) > 0) {
				logger.info("FS {}: Recalculated latest revtag {}, was {}",
						Util.formatArchiveId(config.archiveId),
						Util.formatRevisionTag(tip),
						latest != null ? Util.formatRevisionTag(latest) : "null");
				latest = tip;
			}
		}
	}
	
	protected void queueAutomerge() throws IOException, DiffResolutionException {
		if(automergeSnoozeThread == null || automergeSnoozeThread.isCancelled()) {
			automergeSnoozeThread = new SnoozeThread(automergeDelayMs, maxAutomergeDelayMs, true, ()-> {
				if(config.getArchive().isClosed()) return;
				try {
					if(config.isReadOnly()) {
						/* obviously we're not doing any merging if we don't have the write key, so
						 * let's just consolidate instead.
						 */
						// TODO API: (coverage) branch
						consolidate();
					} else {
						DiffSetResolver.canonicalMergeResolver(config.getArchive()).resolve();
					}
				} catch (IOException|DiffResolutionException exc) {
					logger.error("Error performing automerge", exc);
				}
			});
		} else {
			automergeSnoozeThread.snooze();
		}
	}
}
