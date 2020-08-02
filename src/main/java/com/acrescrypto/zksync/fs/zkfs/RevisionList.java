package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.ClosedException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.exceptions.SearchFailedException;
import com.acrescrypto.zksync.fs.swarmfs.SwarmFS;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionToken;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;

public class RevisionList implements AutoCloseable {
	public interface RevisionMonitor {
		public void notifyNewRevision(RevisionTag revTag);
	}

	private ArrayList<RevisionTag> branchTips = new ArrayList<>();
	protected LinkedList<RevisionMonitor> monitors = new LinkedList<>();
	protected ZKArchiveConfig config;
	protected RevisionTag latest; // "latest" tip; understood to mean tip with greatest height, using hash
									// comparison as tiebreaker.
	protected boolean automerge;
	protected SnoozeThread automergeSnoozeThread;
	protected GroupedThreadPool threadPool;
	protected int totalAdds;
	
	public long automergeDelayMs;
	public long maxAutomergeDelayMs;
	private Logger logger = LoggerFactory.getLogger(RevisionList.class);
	protected LinkedList<SubscriptionToken<?>> toks = new LinkedList<>();

	public RevisionList(ZKArchiveConfig config) throws IOException {
		this.config = config;
		threadPool = GroupedThreadPool.newWorkStealingThreadPool(config.getThreadGroup(), "RevisionList callback");
		automergeDelayMs = config.getMaster().getGlobalConfig().getInt("fs.settings.automergeDelayMs");
		maxAutomergeDelayMs = config.getMaster().getGlobalConfig().getInt("fs.settings.maxAutomergeDelayMs");

		toks.add(config.getMaster().getGlobalConfig().subscribe("fs.settings.automergeDelayMs").asInt((delay) -> {
			this.automergeDelayMs = delay;
			this.updateAutomergeDelayParams(automergeDelayMs, maxAutomergeDelayMs);
		}));

		toks.add(config.getMaster().getGlobalConfig().subscribe("fs.settings.maxAutomergeDelayMs").asInt((maxDelay) -> {
			this.maxAutomergeDelayMs = maxDelay;
			this.updateAutomergeDelayParams(automergeDelayMs, maxAutomergeDelayMs);
		}));

		try {
			read();
		} catch (ENOENTException exc) {
			branchTips = new ArrayList<>();
		}
	}

	public void close() {
		for (SubscriptionToken<?> tok : toks) {
			tok.close();
		}
	}

	/** All revisions we know of that are not yet merged into other revisions. */
	public synchronized ArrayList<RevisionTag> branchTips() {
		if(branchTips.isEmpty() && config.hasKey() && !config.isReadOnly()) {
			branchTips.add(config.blankRevisionTag());
		}
		
		return new ArrayList<>(branchTips);
	}
	
	/** Branch tips (as from branchTips()) that we presently have inode and directory
	 * information for, e.g. we can perform diff merges and show directory listings.
	 * Waits up to timeoutMs for ALL tags to arrive. Set timeoutMs=0 for no waiting, or
	 * timeoutMs=-1 for indefinite waiting. If timeoutMs != 0, then requests are automatically
	 * made to the swarm for pages related to tags not cached locally.
	 * 
	 * @throws IOException 
	 */
	public ArrayList<RevisionTag> availableTags(Collection<RevisionTag> tags, long timeoutMs) throws IOException {
		long deadline = System.currentTimeMillis() + timeoutMs;
		ArrayList<RevisionTag> available = new ArrayList<>(tags.size());
		for(RevisionTag tag : tags) {
			if(tag.hasStructureLocally()) {
				logger.debug("RevisionList {} {}: Revision {} is available",
						config.getArchive().getMaster().getName(),
						Util.formatArchiveId(config.archiveId),
						Util.formatRevisionTag(tag));
				available.add(tag);
			}
		}
		
		if(timeoutMs == 0) {
			// we will not be requesting or waiting for anything we don't already have
			return available;
		}
		
		for(RevisionTag tag : tags) {
			// send swarm requests for any tags we don't have structure data for yet
			if(available.contains(tag)) continue;
			config.getSwarm().requestRevisionStructure(SwarmFS.REQUEST_TAG_STRUCTURE_PRIORITY, tag);
		}

		for(RevisionTag tag : tags) {
			// wait for everything to come in
			if(available.contains(tag)) continue;
			long timeRemaining = timeoutMs < 0 ? Long.MAX_VALUE : deadline - System.currentTimeMillis();
			if(timeRemaining < 0) break;
			logger.debug("RevisionList {} {}: Waiting up to {}ms to acquire revision {}",
					config.getArchive().getMaster().getName(),
					Util.formatArchiveId(config.archiveId),
					timeRemaining,
					Util.formatRevisionTag(tag));
			if(tag.waitForStructure(timeRemaining)) {
				logger.debug("RevisionList {} {}: Revision {} now available after acquisition",
						config.getArchive().getMaster().getName(),
						Util.formatArchiveId(config.archiveId),
						Util.formatRevisionTag(tag));
				available.add(tag);
			}
		}
		
		return available;
	}

	public void addMonitor(RevisionMonitor monitor) {
		this.monitors.add(monitor);
	}

	public void removeMonitor(RevisionMonitor monitor) {
		this.monitors.remove(monitor);
	}
	
	public RevisionTag tipWithPrefix(String prefix) {
		for(RevisionTag tip : this.branchTips()) {
			if(tip.matchesPrefix(prefix)) {
				return tip;
			}
		}
		
		return null;
	}
	
	public boolean addBranchTip(RevisionTag newBranch) throws IOException {
		return addBranchTip(newBranch, false);
	}

	public boolean addBranchTip(RevisionTag newBranch, boolean verify) throws IOException {
		if(config.revisionTree.isSuperceded(newBranch)) {
			return false;
		}
		
		int totalAddsPrevious = totalAdds;
		if(verify) {
			/* public key verifications are expensive, so we defer revtag validation until after we know
			 * we'd even consider inserting it */

			if(!shouldAcceptBranchTip(newBranch)) {
				return false;
			}
			
			newBranch.assertValid();
		}

		synchronized (this) {
			/* check superceded if we didn't do that last time, or if we added another branch tip before
			 * we got into the synchronized block 
			 */
			boolean lastCheckStillGood = verify && totalAdds == totalAddsPrevious;
			if(!lastCheckStillGood && !shouldAcceptBranchTip(newBranch)) {
				return false;
			}
			
			totalAdds++;
			
			branchTips.add(newBranch);
			logger.info("RevisionList {} {}: Added branch tip {} to revision list, current size = {}",
					config.getArchive().getMaster().getName(), Util.formatArchiveId(config.archiveId),
					Util.formatRevisionTag(newBranch), branchTips.size());
			config.swarm.announceTip(newBranch);
			updateLatest(newBranch);
		}

		for (RevisionMonitor monitor : monitors) {
			threadPool.submit(()->monitor.notifyNewRevision(newBranch));
		}

		if (getAutomerge() && branchTips.size() > 1) {
			queueAutomerge();
		}

		return true;
	}
	
	protected boolean shouldAcceptBranchTip(RevisionTag newBranch) throws SearchFailedException {
		if (config.revisionTree.isSuperceded(newBranch)) {
			return false;
		}
		
		if (branchTips.contains(newBranch)) {
			logger.debug(
					"RevisionList {} {}: Not adding branch tip {} to revision list, already in list, current size = {}",
					config.getArchive().getMaster().getName(), Util.formatArchiveId(config.archiveId),
					Util.formatRevisionTag(newBranch), branchTips.size());
			return false;
		}
		
		return true;
	}

	public void consolidate(RevisionTag newBranch) throws IOException {
		logger.info("RevisionList {} {}: Consolidating to branch tip {}", config.getArchive().getMaster().getName(),
				Util.formatArchiveId(config.getArchiveId()), Util.formatRevisionTag(newBranch));

		Collection<RevisionTag> tips, parents;
		ArrayList<RevisionTag> toRemove = new ArrayList<>();
		RevisionTree tree = config.getRevisionTree();
		parents = tree.parentsForTag(newBranch, RevisionTree.treeSearchTimeoutMs);

		synchronized (this) {
			tips = new ArrayList<>(branchTips);
		}

		for (RevisionTag tip : tips) {
			if (tip.equals(newBranch)) {
				continue;
			}

			if(tree.descendentOf(newBranch, tip)) {
				toRemove.add(tip);
			} else {
				Collection<RevisionTag> tipParents = tree.parentsForTag(tip, RevisionTree.treeSearchTimeoutMs);
				if(tipParents != null && parents != null
						&& tipParents.size() > 1
						&& parents.size() > tipParents.size()
						&& parents.containsAll(tipParents)) {
					toRemove.add(tip);
				}
			}
		}

		if(!toRemove.isEmpty()) {
			synchronized (this) {
				logger.info("RevisionList {} {}: Removing {} branch tips due to consolidation to tip {}",
						config.getArchive().getMaster().getName(), Util.formatArchiveId(config.getArchiveId()),
						toRemove.size(), Util.formatRevisionTag(newBranch));
				for (RevisionTag tip : toRemove) {
					removeBranchTip(tip);
				}
				
				write();
			}
		}
	}

	public void consolidate() throws IOException {
		ArrayList<RevisionTag> tips, toRemove = new ArrayList<>();
		RevisionTree tree = config.getRevisionTree();

		logger.info("RevisionList {} {}: Starting general consolidation",
				config.getArchive().getMaster().getName(),
				Util.formatArchiveId(config.getArchiveId()));

		synchronized (this) {
			tips = new ArrayList<>(branchTips);
		}

		for (RevisionTag tip : tips) {
			if (tree.isSuperceded(tip)) {
				toRemove.add(tip);
			}
		}

		if(!toRemove.isEmpty()) {
			synchronized (this) {
				logger.info("RevisionList {} {} {}: Removing {} branch tips due to general consolidation",
						config.getMaster().getName(),
						config.getArchive().getMaster().getName(), Util.formatArchiveId(config.getArchiveId()),
						toRemove.size());
				for (RevisionTag tip : toRemove) {
					removeBranchTip(tip);
				}
				
				write();
			}
		}
	}

	public synchronized void removeBranchTip(RevisionTag oldBranch) throws IOException {
		branchTips.remove(oldBranch);
		logger.info("RevisionList {} {}: Removed branch tip {}, remaining list size = {}",
				config.getArchive().getMaster().getName(),
				Util.formatArchiveId(config.archiveId),
				Util.formatRevisionTag(oldBranch), branchTips.size());
		if (latest != null && latest.equals(oldBranch)) {
			logger.debug("RevisionList {} {}: Recalculating latest branch tip due to removal of previous latest tip {}",
					config.getArchive().getMaster().getName(),
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
		MutableSecureFile.atPath(
				config.localStorage,
				getPath(),
				branchTipKey()
			).write(serialize(), 65536);
	}

	public synchronized void read() throws IOException {
		deserialize(MutableSecureFile.atPath(
				config.localStorage,
				getPath(),
				branchTipKey()
		   ).read());
	}

	protected synchronized void deserialize(byte[] serialized) throws IOException {
		branchTips.clear();
		latest = null;
		ByteBuffer buf = ByteBuffer.wrap(serialized);

		while (buf.remaining() >= RevisionTag.sizeForConfig(config)) {
			byte[] rawTag = new byte[RevisionTag.sizeForConfig(config)];
			buf.get(rawTag);
			try {
				RevisionTag revTag = new RevisionTag(config, rawTag, false);
				branchTips.add(revTag);
				updateLatest(revTag);
			} catch (SecurityException exc) {
				logger.error("RevisionList {} {}: Invalid signature on stored revision tag; skipping",
						config.getArchive().getMaster().getName(),
						Util.formatArchiveId(config.archiveId),
						exc);
			}
		}

		if (buf.hasRemaining())
			throw new InvalidArchiveException("branch tip file appears corrupt: " + getPath());
	}

	protected synchronized byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(RevisionTag.sizeForConfig(config) * branchTips.size());
		for (RevisionTag tag : branchTips) {
			buf.put(tag.getBytes());
		}
		
		return buf.array();
	}

	protected Key branchTipKey() {
		return config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, "easysafe-revision-list-key");
	}

	public void setAutomerge(boolean automerge) {
		// guard against null pointer exception if this is set early on
		logger.info("RevisionList {} {}: Set automerge = {}",
				config.getMaster().getName(),
				Util.formatArchiveId(config.getArchiveId()),
				automerge);
		this.automerge = automerge;
	}

	public boolean getAutomerge() {
		return automerge;
	}

	public synchronized void dump() {
		Util.debugLog(config.getArchive().getMaster().getName() + " "
				+ Util.bytesToHex(config.swarm.getPublicIdentityKey().getBytes(), 6) + " Revision list: "
				+ branchTips.size());
		int i = 0;
		ArrayList<RevisionTag> sortedTips = branchTips();
		sortedTips.sort(null);
		for (RevisionTag tag : sortedTips) {
			i++;
			Util.debugLog("\t" + i + ": " + Util.formatRevisionTag(tag) + " parentHash="
					+ Util.bytesToHex(Util.serializeLong(tag.getParentHash())));
		}
	}

	public synchronized void dumpDot() throws IOException {
		String path = "revision-graph.dot";
		StringBuilder sb = new StringBuilder();
		sb.append("digraph \"" + config.getMaster().getName() + "\" {\n");
		HashSet<RevisionTag> seen = new HashSet<>();
		LinkedList<RevisionTag> toProcess = new LinkedList<>(branchTips);
		while (!toProcess.isEmpty()) {
			RevisionTag tag = toProcess.poll();
			Collection<RevisionTag> parents = config.revisionTree.parentsForTag(tag);

			sb.append("\t\"" + Util.formatRevisionTag(tag) + "\" [\n");
			if (tag.equals(latest)) {
				sb.append("\t\tcolor=red\n");
			} else if (branchTips.contains(tag)) {
				sb.append("\t\tcolor=blue\n");
			}
			if (parents.size() <= 1) {
				sb.append("\t\tstyle=\"filled\"\n");
				sb.append("\t\tfillcolor=lightgray\n");
			}
			sb.append("\t];\n");

			for (RevisionTag parent : parents) {
				String line = String.format("\t\"%s\" -> \"%s\"", Util.formatRevisionTag(parent),
						Util.formatRevisionTag(tag));

				if (tag.equals(latest)) {
					line += " [color=red]";
				} else if (branchTips.contains(tag)) {
					line += " [color=blue]";
				}

				sb.append(line + ";\n");
				if (!seen.contains(parent)) {
					seen.add(parent);
					toProcess.add(parent);
				}
			}
		}
		sb.append("}\n");

		config.getMaster().getStorage().write(path, sb.toString().getBytes());
	}

	public synchronized String recursiveDumpStr() {
		String s = Util.formatArchiveId(config.getArchiveId()) + " recursive revision listing\n";
		int i = 0;

		LinkedList<RevisionTag> sortedTips = new LinkedList<>(branchTips());
		while (!sortedTips.isEmpty()) {
			int j = 0;
			sortedTips.sort(null);
			RevisionTag tag = sortedTips.pollLast();
			s += String.format("\tTag %3d %s\n", i++, Util.formatRevisionTag(tag));
			try {
				for (RevisionTag parent : tag.getInfo().getParents()) {
					s += String.format("\t\tParent %3d %s\n", j++, Util.formatRevisionTag(parent));
				}
			} catch (IOException exc) {
				s += "\t\t(Caught IOException processing parents, " + exc.getClass().getSimpleName() + " "
						+ exc.getMessage() + ")";
				exc.printStackTrace();
			}
		}

		return s;
	}

	public RevisionTag latest() {
		return latest == null && !config.isReadOnly() ? RevisionTag.blank(config) : latest;
	}

	protected void updateLatest(RevisionTag newTip) throws IOException {
		if(latest == null
			|| newTip.compareTo(latest) > 0
			|| (config.getRevisionTree() != null && config.getRevisionTree().supercededBy(newTip, latest)))
		{
			try {
				logger.info("RevisionList {} {}: New latest revtag {}, was {}",
						config.getArchive().getMaster().getName(),
						Util.formatArchiveId(config.archiveId),
						Util.formatRevisionTag(newTip),
						latest != null ? Util.formatRevisionTag(latest) : "null");
			} catch(NullPointerException exc) {} // ignore null pointer for certain unit tests
			latest = newTip;
		}
	}

	protected void recalculateLatest() {
		latest = null;
		for (RevisionTag tip : branchTips) {
			if (latest == null || tip.compareTo(latest) > 0) {
				logger.info("RevisionList {} {}: Recalculated latest revtag {}, was {}",
						config.getArchive().getMaster().getName(),
						Util.formatArchiveId(config.archiveId),
						Util.formatRevisionTag(tip),
						latest != null ? Util.formatRevisionTag(latest) : "null");
				latest = tip;
			}
		}
	}

	public void executeAutomerge() {
		if (config.getArchive().isClosed()) {
			logger.debug("RevisionList {} {}: Skipping automerge of closed archive",
					config.getArchive().getMaster().getName(),
					Util.formatArchiveId(config.getArchiveId()));
			return;
		}
		try {
			if(config.isReadOnly()) {
				/*
				 * obviously we're not doing any merging if we don't have the write key, so
				 * let's just consolidate instead.
				 */
				// TODO API: (coverage) branch
				logger.debug("RevisionList {} {}: Consolidating branches on read-only archive due to automerge thread",
						config.getArchive().getMaster().getName(),
						Util.formatArchiveId(config.getArchiveId()));
				consolidate();
			} else {
				logger.info("RevisionList {} {}: Automerge started", config.getArchive().getMaster().getName(),
						Util.formatArchiveId(config.getArchiveId()));
				Collection<RevisionTag> tips;
				
				synchronized(this) {
					consolidate();
					tips = branchTips();
				}
				
				DiffSetResolver.canonicalMergeResolver(tips).resolve();
			}
		} catch (ClosedException exc) {
			logger.debug("RevisionList {} {}: Automerge aborted since archive closed",
					config.getArchive().getMaster().getName(),
					Util.formatArchiveId(config.getArchiveId()));
		} catch (Throwable exc) {
			logger.error("RevisionList {} {}: Error performing automerge",
					config.getArchive().getMaster().getName(),
					Util.formatArchiveId(config.archiveId),
					exc);
			exc.printStackTrace();
		}
	}

	protected synchronized void queueAutomerge() throws IOException {
		logger.debug("RevisionList {} {}: Queuing automerge",
				config.getArchive().getMaster().getName(),
				Util.formatArchiveId(config.getArchiveId()));

		if (automergeSnoozeThread == null || automergeSnoozeThread.isCancelled()) {
			automergeSnoozeThread = new SnoozeThread(automergeDelayMs, maxAutomergeDelayMs, true, () -> {
				executeAutomerge();
			});
		} else {
			automergeSnoozeThread.snooze();
		}
	}
	
	protected void updateAutomergeDelayParams(long automergeDelayMs, long maxAutomergeDelayMs) {
		this.automergeDelayMs = automergeDelayMs;
		this.maxAutomergeDelayMs = maxAutomergeDelayMs;
		
		if(automergeSnoozeThread == null || automergeSnoozeThread.isCancelled()) {
			return;
		}
		
		long currentDeadline = automergeSnoozeThread.getDeadline();
		long currentTrigger = automergeSnoozeThread.getExpirationMs();

		long newTrigger = System.currentTimeMillis() + automergeDelayMs;
		long newDeadline = maxAutomergeDelayMs < 0
			? Long.MAX_VALUE
			: automergeSnoozeThread.getDeadline() - automergeSnoozeThread.getMaxTimeMs() + maxAutomergeDelayMs;
		
		if(newDeadline < currentDeadline || newTrigger < currentTrigger) {
			// cleans up old timer + invokes automerge (snoozethread set to invoke callback on cancel)
			automergeSnoozeThread.cancel();
		} else {
			automergeSnoozeThread.setDelayMs(automergeDelayMs);
			automergeSnoozeThread.setMaxTimeMs(maxAutomergeDelayMs);
		}
	}
}
