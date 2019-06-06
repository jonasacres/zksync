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
import com.acrescrypto.zksync.exceptions.ClosedException;
import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionToken;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;

public class RevisionList implements AutoCloseable {
	public interface RevisionMonitor {
		public void notifyNewRevision(RevisionTag revTag);
	}
	
	protected ArrayList<RevisionTag> branchTips = new ArrayList<>();
	protected LinkedList<RevisionMonitor> monitors = new LinkedList<>();
	protected ZKArchiveConfig config;
	protected RevisionTag latest; // "latest" tip; understood to mean tip with greatest height, using hash comparison as tiebreaker.
	protected boolean automerge;
	protected SnoozeThread automergeSnoozeThread;
	public long automergeDelayMs;
	public long maxAutomergeDelayMs;
	private Logger logger = LoggerFactory.getLogger(RevisionList.class);
	protected LinkedList<SubscriptionToken<?>> toks = new LinkedList<>();
	
	public RevisionList(ZKArchiveConfig config) throws IOException {
		this.config = config;
		automergeDelayMs = config.getMaster().getGlobalConfig().getInt("fs.settings.automergeDelayMs");
		maxAutomergeDelayMs = config.getMaster().getGlobalConfig().getInt("fs.settings.maxAutomergeDelayMs");
		
		toks.add(config.getMaster().getGlobalConfig().subscribe("fs.settings.automergeDelayMs").asInt((delay)->{
			this.automergeDelayMs = delay;
		}));
		
		toks.add(config.getMaster().getGlobalConfig().subscribe("fs.settings.maxAutomergeDelayMs").asInt((delay)->{
			this.maxAutomergeDelayMs = delay;
		}));
		
		try {
			read();
		} catch(ENOENTException exc) {
			branchTips = new ArrayList<>();
		}
	}
	
	public void close() {
		for(SubscriptionToken<?> tok : toks) {
			tok.close();
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
		if(config.revisionTree.isSuperceded(newBranch)) {
			System.out.println("RevisionList " + config.getMaster().getName() + ": Not adding branch tip " + Util.formatRevisionTag(newBranch) + " (superceded)");
			return false;
		}
		
		synchronized(this) {
			if(config.revisionTree.isSuperceded(newBranch)) return false;
			if(branchTips.contains(newBranch)) {
				System.out.println("RevisionList " + config.getMaster().getName() + ": Not adding branch tip " + Util.formatRevisionTag(newBranch) + " (duplicate)");
				logger.debug("RevisionList {}: Not adding branch tip {} to revision list, already in list, current size = {}",
						Util.formatArchiveId(config.archiveId),
						Util.formatRevisionTag(newBranch),
						branchTips.size());
				return false;
			}
			branchTips.add(newBranch);
			logger.info("RevisionList {}: Added branch tip {} to revision list, current size = {}",
					Util.formatArchiveId(config.archiveId),
					Util.formatRevisionTag(newBranch),
					branchTips.size());
			System.out.println("RevisionList " + config.getMaster().getName() + ": Adding branch tip " + Util.formatRevisionTag(newBranch));
			config.swarm.announceTip(newBranch);
			updateLatest(newBranch);
		}
		
		for(RevisionMonitor monitor : monitors) {
			monitor.notifyNewRevision(newBranch);
		}
		
		if(getAutomerge() && branchTips.size() > 1) {
			queueAutomerge();
		}
		
		return true;
	}
	
	public void consolidate(RevisionTag newBranch) throws IOException {
		logger.info("RevisionList {}: Consolidating to branch tip {}",
				Util.formatArchiveId(config.getArchiveId()),
				Util.formatRevisionTag(newBranch));

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
			logger.info("RevisionList {}: Removing {} branch tips due to consolidation to tip {}",
					Util.formatArchiveId(config.getArchiveId()),
					toRemove.size(),
					Util.formatRevisionTag(newBranch));
			for(RevisionTag tip : toRemove) {
				System.out.println("RevisionList " + config.getArchive().getMaster().getName() + ": Removing branch tip " + Util.formatRevisionTag(tip) + " in consolidation to " + Util.formatRevisionTag(newBranch));
				removeBranchTip(tip);
			}
		}
	}
	
	public void consolidate() throws IOException {
		ArrayList<RevisionTag> tips, toRemove = new ArrayList<>();
		RevisionTree tree = config.getRevisionTree();
		
		logger.info("RevisionList {}: Starting general consolidation",
				Util.formatArchiveId(config.getArchiveId()));

		synchronized(this) {
			tips = new ArrayList<>(branchTips);
		}
		
		for(RevisionTag tip : tips) {
			if(tree.isSuperceded(tip)) {
				toRemove.add(tip);
			}
		}
		
		synchronized(this) {
			logger.info("RevisionList {}: Removing {} branch tips due to general consolidation",
					Util.formatArchiveId(config.getArchiveId()),
					toRemove.size());
			for(RevisionTag tip : toRemove) {
				System.out.println("RevisionList " + config.getArchive().getMaster().getName() + ": Removing branch tip " + Util.formatRevisionTag(tip) + " in general consolidation");
				removeBranchTip(tip);
			}
		}
	}
	
	public synchronized void removeBranchTip(RevisionTag oldBranch) throws IOException {
		branchTips.remove(oldBranch);
		logger.info("RevisionList {}: Removed branch tip {}, remaining list size = {}",
				Util.formatArchiveId(config.archiveId),
				Util.formatRevisionTag(oldBranch),
				branchTips.size());
		if(latest != null && latest.equals(oldBranch)) {
			logger.debug("RevisionList {}: Recalculating latest branch tip due to removal of previous latest tip {}",
					Util.formatArchiveId(config.archiveId),
					Util.formatRevisionTag(oldBranch));
			recalculateLatest();
		}
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
				logger.error("RevisionList {}: Invalid signature on stored revision tag; skipping",
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
		logger.info("RevisionList {}: Set automerge = {}",
				Util.formatArchiveId(config.getArchiveId()),
				automerge);
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
			System.out.println("\t" + i + ": " + Util.formatRevisionTag(tag) + " parentHash=" + Util.bytesToHex(Util.serializeLong(tag.getParentHash())));
		}
	}
	
	public synchronized String recursiveDumpStr() {
		String s = Util.formatArchiveId(config.getArchiveId()) + " recursive revision listing\n";
		int i = 0;
		
		LinkedList<RevisionTag> sortedTips = new LinkedList<>(branchTips());
		while(!sortedTips.isEmpty()) {
			int j = 0;
			sortedTips.sort(null);
			RevisionTag tag = sortedTips.pollLast();
			s += String.format("\tTag %3d %s\n", i++, Util.formatRevisionTag(tag));
			try {
				for(RevisionTag parent : tag.getInfo().getParents()) {
					s += String.format("\t\tParent %3d %s\n", j++, Util.formatRevisionTag(parent));
				}
			} catch (IOException exc) {
				s += "\t\t(Caught IOException processing parents, " + exc.getClass().getSimpleName() + " " + exc.getMessage() + ")";
				exc.printStackTrace();
			}
		}
		
		return s;
	}
	
	public RevisionTag latest() {
		return latest == null && !config.isReadOnly() ? RevisionTag.blank(config) : latest;
	}
	
	protected void updateLatest(RevisionTag newTip) {
		if(latest == null || newTip.compareTo(latest) > 0) {
			logger.info("RevisionList {}: New latest revtag {}, was {}",
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
				logger.info("RevisionList {}: Recalculated latest revtag {}, was {}",
						Util.formatArchiveId(config.archiveId),
						Util.formatRevisionTag(tip),
						latest != null ? Util.formatRevisionTag(latest) : "null");
				latest = tip;
			}
		}
	}
	
	public synchronized void executeAutomerge() {
		if(config.getArchive().isClosed()) {
			logger.debug("RevisionList {}: Skipping automerge of closed archive",
					Util.formatArchiveId(config.getArchiveId()));
			return;
		}
		try {
			if(config.isReadOnly()) {
				/* obviously we're not doing any merging if we don't have the write key, so
				 * let's just consolidate instead.
				 */
				// TODO API: (coverage) branch
				logger.debug("RevisionList {}: Consolidating branches on read-only archive due to automerge thread",
						Util.formatArchiveId(config.getArchiveId()));
				consolidate();
			} else {
				logger.info("RevisionList {}: Automerge started",
						Util.formatArchiveId(config.getArchiveId()));
				DiffSetResolver.canonicalMergeResolver(config.getArchive()).resolve();
			}
		} catch(ClosedException exc) {
			logger.debug("RevisionList {}: Automerge aborted since archive clsoed",
					Util.formatArchiveId(config.getArchiveId()));
		} catch (IOException|DiffResolutionException exc) {
			logger.error("RevisionList {}: Error performing automerge",
					Util.formatArchiveId(config.archiveId),
					exc);
		}
	}
	
	protected synchronized void queueAutomerge() throws IOException {
		logger.debug("RevisionList {}: Queuing automerge",
				Util.formatArchiveId(config.getArchiveId()));

		if(automergeSnoozeThread == null || automergeSnoozeThread.isCancelled()) {
			automergeSnoozeThread = new SnoozeThread(automergeDelayMs, maxAutomergeDelayMs, true, ()-> {
				executeAutomerge();
			});
		} else {
			automergeSnoozeThread.snooze();
		}
	}
}
