package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.HashContext;
import com.acrescrypto.zksync.exceptions.ClosedException;
import com.acrescrypto.zksync.exceptions.SearchFailedException;
import com.acrescrypto.zksync.fs.swarmfs.SwarmFS;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionToken;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.HashCache;
import com.acrescrypto.zksync.utility.Util;

public class RevisionTree implements AutoCloseable {
	public final static int DEFAULT_TREE_SEARCH_TIMEOUT_MS = 30000; // when finding ancestors, how long to wait before giving up on a lookup?
	public static int treeSearchTimeoutMs = DEFAULT_TREE_SEARCH_TIMEOUT_MS;
	
	class TreeSearchItem {
		ArrayList<RevisionTag> revTags = new ArrayList<>();
		HashMap<Long,ArrayList<RevisionTag>> deferred = new HashMap<>();
		HashSet<RevisionTag> seen = new HashSet<>();
		long height;
		
		TreeSearchItem(RevisionTag tag) {
			this.height = tag.getHeight();
			revTags.add(tag);
		}
		
		boolean hasAncestor(RevisionTag tag) throws SearchFailedException {
			if(this.height <= tag.getHeight()) return false;
			
			while(height >= 0) {
				if(this.height < tag.getHeight()) return false;
				
				if(revTags.contains(tag)) {
					return true;
				}
				
				synchronized(this) {
					for(ArrayList<RevisionTag> list : deferred.values()) {
						if(list.contains(tag)) return true;
					}
				}
				
				if(height > 0) recurse();
				else return false;
			}
			
			return false;
		}
		
		void recurse() throws SearchFailedException {
			ArrayList<RevisionTag> newRevTags = new ArrayList<>();
			ArrayList<RevisionTag> lookups = new ArrayList<>();
			height--;
			
			synchronized(this) {
				if(deferred.containsKey(height)) {
					newRevTags.addAll(deferred.get(height));
					deferred.remove(height);
					newRevTags.removeIf((tag)->seen.contains(tag));
				}
				
				for(RevisionTag revTag : revTags) {
					if(seen.contains(revTag)) continue;
					seen.add(revTag);
					Collection<RevisionTag> parents = parentsForTagLocal(revTag);
					if(parents == null) {
						lookups.add(revTag);
					} else {
						for(RevisionTag parent : parents) {
							if(parent.getHeight() == height) {
								newRevTags.add(parent);
							} else {
								deferred.putIfAbsent(parent.getHeight(), new ArrayList<>());
								deferred.get(parent.getHeight()).add(parent);
							}
						}
					}
				}
			}
			
			// TODO API: (coverage) branch (it's worrying that this is never called)
			ArrayList<Future<?>> futures = new ArrayList<>();
			for(RevisionTag revTag : lookups) {
				Future<?> future = threadPool.submit(()->{
					Collection<RevisionTag> parents = parentsForTag(revTag, treeSearchTimeoutMs);
					if(parents == null) return;
					synchronized(this) {
						for(RevisionTag parent : parents) {
							if(parent.getHeight() == height) {
								newRevTags.add(parent);
							} else {
								deferred.putIfAbsent(parent.getHeight(), new ArrayList<>());
								deferred.get(parent.getHeight()).add(parent);
							}
						}
					}
				});
				
				futures.add(future);
			}
			
			for(Future<?> future : futures) {
				try {
					while(true) {
						try {
							future.get(treeSearchTimeoutMs+100, TimeUnit.MILLISECONDS);
							break;
						} catch(InterruptedException exc) {}
					}
				} catch (ExecutionException|TimeoutException exc) {
					if(exc instanceof ExecutionException) {
						Throwable e = exc;
						while(e instanceof ExecutionException && e.getCause() != null) {
							e = e.getCause();
						}
						
						e.printStackTrace();
					}
					
					throw new SearchFailedException();
				}
			}
			revTags = newRevTags;
		}
		
		void recurseToLevel(long newHeight) throws SearchFailedException {
			while(height > newHeight) {
				recurse();
			}
		}
	}
	
	class TreeSearch {
		ArrayList<TreeSearchItem> items = new ArrayList<>();
		
		TreeSearch(Collection<RevisionTag> tags) {
			for(RevisionTag tag : tags) {
				items.add(new TreeSearchItem(tag));
			}
		}
		
		TreeSearch(RevisionTag[] tags) {
			for(RevisionTag tag : tags) {
				items.add(new TreeSearchItem(tag));
			}
		}
		
		RevisionTag tagWithPrefix(String prefix) throws SearchFailedException {
			while(true) {
				boolean hasNonRoot = false;
				
				for(TreeSearchItem item : items) {
					for(RevisionTag tag : item.revTags) {
						if(tag.matchesPrefix(prefix)) {
							return tag;
						}
						
						if(tag.getHeight() > 0) {
							hasNonRoot = true;
						}
					}
				}
				
				if(!hasNonRoot) return null;
				
				for(TreeSearchItem item : items) {
					item.recurse();
				}
			}
		}
		
		RevisionTag commonAncestor() throws SearchFailedException {
			makeFlush();
			while(true) {
				RevisionTag common = commonAncestorAtLevel();
				
				if(common != null) {
					return common;
				}
				
				if(items.get(0).height == 0) {
					return RevisionTag.blank(config);
				}
				
				recurse();
			}
		}
		
		void makeFlush() throws SearchFailedException {
			// bring all the items down to the height of the lowest item
			long minHeight = Long.MAX_VALUE;
			for(TreeSearchItem item : items) {
				if(minHeight > item.height) minHeight = item.height;
			}
			
			for(TreeSearchItem item : items) {
				item.recurseToLevel(minHeight);
			}
		}
		
		void recurse() throws SearchFailedException {
			for(TreeSearchItem item : items) {
				item.recurse();
			}
		}
		
		RevisionTag commonAncestorAtLevel() {
			HashMap<RevisionTag, Integer> seenTags = new HashMap<>();
			
			for(TreeSearchItem item : items) {
				for(RevisionTag tag : item.revTags) {
					int count = seenTags.getOrDefault(tag, 0) + 1;
					seenTags.put(tag, count);
				}
			}
			
			RevisionTag bestMatch = null;
			for(RevisionTag tag : seenTags.keySet()) {
				if(seenTags.get(tag) != items.size()) continue;
				if(bestMatch == null || tag.compareTo(bestMatch) < 0) {
					bestMatch = tag;
				}
			}
			
			return bestMatch;
		}
	}
	
	ZKArchiveConfig config;
	protected final Logger logger = LoggerFactory.getLogger(RevisionTree.class);
	HashCache<RevisionTag, HashSet<RevisionTag>> map = new HashCache<>(256,
			(tag)->{
				if(hasParentsForTag(tag)) {
					logger.trace("RevisionTree {}: Caching parent list for locally-stored revision {}",
							Util.formatArchiveId(config.getArchiveId()),
							Util.formatRevisionTag(tag));
					return new HashSet<>(tag.getInfo().parents);
				} else {
					logger.trace("RevisionTree {}: Caching parent list for non-locally-stored revision {}",
							Util.formatArchiveId(config.getArchiveId()),
							Util.formatRevisionTag(tag));
					Collection<RevisionTag> parents = parentsForTag(tag);
					if(parents == null) {
						throw new SearchFailedException();
					}
					
					return new HashSet<>(parents);
				}
			},
			(tag,parents)->{
				logger.trace("RevisionTree {}: Evicting cached parent list for revision {}",
						Util.formatArchiveId(config.getArchiveId()),
						Util.formatRevisionTag(tag));
			});
	protected GroupedThreadPool threadPool;
	protected LinkedList<SubscriptionToken<?>> subscriptions = new LinkedList<>();
	
	public RevisionTree(ZKArchiveConfig config) {
		this.config = config;
		
		try {
			this.map.setCapacity(config.getMaster().getGlobalConfig().getInt("fs.settings.revisionTreeCacheSize"));
		} catch(IOException exc) {
			logger.error("RevisionTree {}: Caught exception setting RevisionTree capacity; proceeding anyway",
					Util.formatArchiveId(config.getArchiveId()),
					exc);
		}
		
		subscriptions.add(config.getMaster().getGlobalConfig().subscribe("fs.settings.revisionTreeCacheSize").asInt((s)-> {
			try {
				logger.info("RevisionTree {}: Setting revision tree capacity to {}; was {}",
						Util.formatArchiveId(config.getArchiveId()),
						s,
						this.map.getCapacity());
				this.map.setCapacity(s);
			} catch(IOException exc) {
				logger.error("RevisionTree {}: Caught exception setting RevisionTree capacity",
						Util.formatArchiveId(config.getArchiveId()),
						exc);
			}
		}));
		
		threadPool = GroupedThreadPool.newWorkStealingThreadPool(config.getThreadGroup(), "RevisionTree lookup");
	}
	
	public void close() {
		for(SubscriptionToken<?> subscription : subscriptions) {
			subscription.close();
		}
		
		try {
			map.removeAll();
		} catch (IOException exc) {
			logger.error("RevisionTree {}: Caught exception purging RevisionTree cache",
					Util.formatArchiveId(config.getArchiveId()),
					exc);
		}
	}
	
	public synchronized void clear() throws IOException {
		map.removeAll();
	}
	
	public RevisionTag tagWithPrefix(String prefix) throws SearchFailedException {
		TreeSearch search = new TreeSearch(config.getRevisionList().branchTips());
		return search.tagWithPrefix(prefix);
	}
	
	public void addParentsForTag(RevisionTag revTag, Collection<RevisionTag> parents) {
		if(map.hasCached(revTag)) return;
		validateParentList(revTag, parents);
		
		synchronized(this) {
			HashSet<RevisionTag> parentSet = new HashSet<>(parents);
			try {
				map.add(revTag, parentSet);
				this.notifyAll();
			} catch(IOException exc) {
				logger.error("RevisionTree {}: Caught exception adding parents for tag",
						Util.formatArchiveId(config.getArchiveId()),
						exc);
			}
		}
	}

	public boolean hasParentsForTag(RevisionTag revTag) {
		try {
			if(map.hasCached(revTag)) return true;
			if(config.archive.isClosed()) return false;
			if(config.archive.hasInodeTableFirstPage(revTag)) return true;
		} catch(IOException exc) {
			logger.error("RevisionTree {}: Caught IOException checking status of revTag",
					Util.formatArchiveId(config.getArchiveId()),
					exc);
		}
		
		return false;
	}
	
	public Collection<RevisionTag> parentsForTag(RevisionTag revTag) {
		return parentsForTag(revTag, treeSearchTimeoutMs);
	}
	
	public Collection<RevisionTag> parentsForTag(RevisionTag revTag, long timeoutMs) {
		if(revTag.getHeight() == 0) return new ArrayList<>(); // top-level revisions have no parents
		Collection<RevisionTag> r = parentsForTagLocal(revTag);
		if(r != null) return r;
		fetchParentsForTag(revTag, timeoutMs);
		return parentsForTagLocal(revTag);
	}
	
	public synchronized Collection<RevisionTag> parentsForTagLocal(RevisionTag revTag) {
		if(hasParentsForTag(revTag)) {
			try {
				return map.get(revTag);
			} catch(SearchFailedException exc) {
				if(config.isClosed()) return null;
				logger.error("RevisionTree {}: Encountered IOException looking up cached revTag",
						Util.formatArchiveId(config.getArchiveId()),
						exc);
			} catch(ClosedException exc) {
				logger.info("RevisionTree {}: Cannot look up cached revTag {} in closed archive",
						Util.formatArchiveId(config.getArchiveId()),
						Util.formatRevisionTag(revTag));
			} catch (IOException exc) {
				logger.error("RevisionTree {}: Encountered IOException looking up cached revTag",
						Util.formatArchiveId(config.getArchiveId()),
						exc);
			}
		}
		
		return null;
	}
	
	public RevisionTag commonAncestor(Collection<RevisionTag> revTags) throws SearchFailedException {
		return new TreeSearch(revTags).commonAncestor();
	}
	
	public RevisionTag commonAncestor(RevisionTag[] revTags) throws SearchFailedException {
		return new TreeSearch(revTags).commonAncestor();
	}
	
	public boolean descendentOf(RevisionTag tag, RevisionTag possibleAncestor) throws SearchFailedException {
		if(possibleAncestor.equals(config.blankRevisionTag())) return true;
		if(tag.equals(possibleAncestor)) return true;
		return new TreeSearchItem(tag).hasAncestor(possibleAncestor);
	}
	
	/** Is an existing tag superceded by a new tag? (ie. the new one is descendent from the existing,
	 * or is a merge containing the same ancestors)
	 * @throws IOException 
	 */
	public boolean supercededBy(RevisionTag newTag, RevisionTag existing) throws IOException {
		if(newTag.equals(existing)) return false;
		
		if(newTag.getHeight() < existing.getHeight()) {
			return false;
		}
		
		if(descendentOf(newTag, existing)) {
			return true;
		}
		
		Collection<RevisionTag> parents = parentsForTag(existing);
		
		if(parents.size() <= 1) {
			return false;
		}
		
		for(RevisionTag parent : parents) {
			if(!descendentOf(newTag, parent)) {
				return false;
			}
		}
		
		if(existing.compareTo(newTag) < 0) return false;
		
		return true;
	}
	
	/** Given a revision set, eliminate any revisions that are ancestral to other revisions in
	 * the set.
	 */
	public Collection<RevisionTag> minimalSet(Collection<RevisionTag> revTags) throws SearchFailedException {
		LinkedList<RevisionTag> minimal = new LinkedList<>();
		for(RevisionTag tag : revTags) {
			boolean redundant = false;
			for(RevisionTag other : revTags) {
				if(other == tag) continue;
				if(descendentOf(other, tag)) {
					redundant = true;
					break;
				}
			}
			
			if(!redundant) {
				minimal.add(tag);
			}
		}
		
		minimal.sort(null);
		return minimal;
	}
	
	/** Take a set of revtags and transform it to the most recent set of ancestors of each
	 * revtag containing only one parent. (In other words, because we don't merge merges
	 * directly, we want to find the last "real" content-bearing revtags underpinning
	 * each revtag in a revision set.) */
	public Collection<RevisionTag> canonicalBases(Collection<RevisionTag> revTags) throws SearchFailedException {
		/* Common ancestry is expensive to find and calculated twice right now: once when determining the revisions
		 * to merge, and again when doing the merge. It'd be good to refactor to avoid sometime.
		 */
		RevisionTag ancestor = commonAncestor(revTags);
		
		HashSet<RevisionTag> bases = new HashSet<>();
		LinkedList<RevisionTag> toProcess = new LinkedList<>(revTags);
		while(!toProcess.isEmpty()) {
			RevisionTag current = toProcess.pop();
			if(current.equals(ancestor)) {
				bases.add(current);
			} else {				
				Collection<RevisionTag> parents = parentsForTag(current, treeSearchTimeoutMs);
				if(parents != null && parents.size() > 1) {
					toProcess.addAll(parents);
				} else {
					// Note that we get here if parents.size() == 1, or if parents is null.
					/* null parents => couldn't find parent information, so we run the risk of a non-canonical
					 * merge, but we don't have much choice. */
					bases.add(current);
				}
			}
		}
		
		return minimalSet(bases);
	}
	
	/** Do we already have a revtag superceding all the data in this revtag? 
	 * @throws SearchFailedException */
	public boolean isSuperceded(RevisionTag revTag) throws SearchFailedException {
		ArrayList<RevisionTag> tips = config.getRevisionList().branchTips();
		logger.debug("RevisionTree {}: Checking superceded status of {}",
				Util.formatArchiveId(config.getArchiveId()),
				Util.formatRevisionTag(revTag));

		for(RevisionTag tip : tips) {
			if(tip.equals(revTag)) continue;
			if(descendentOf(tip, revTag)) {
				return true; // we have a tip that descends from this tag
			}
		}
		
		if(revTag.getHeight() <= 1) return false; // this micro-optimization helps simplify test-writing (no need to provide parent lists for revtags of height 1)
		
		Collection<RevisionTag> parents = parentsForTag(revTag);
		if(parents == null) {
			if(config.isClosed()) return false; // avoid needless log spam
			throw new SearchFailedException();
		}
		
		if(parents.size() > 1) {
			for(RevisionTag possibleSuperset : tips) {
				if(possibleSuperset.equals(revTag)) continue;
				// if this is a merge, do we already have a merge including everything this one does?
				Collection<RevisionTag> tipParents = parentsForTagLocal(possibleSuperset);
				if(tipParents != null && tipParents.containsAll(parents)) {
					return true;
				}
				
				boolean containsParents = true;
				for(RevisionTag parent : parents) {
					if(!descendentOf(possibleSuperset, parent)) {
						containsParents = false;
						break;
					}
				}
				
				if(containsParents) {
					return true;
				}
			}
		}
		
		return false;
	}
	
	protected void validateParentList(RevisionTag revTag, Collection<RevisionTag> parents) {
		long parentHash;
		
		ArrayList<RevisionTag> sorted = new ArrayList<>(parents);
		sorted.sort(null);
		
		HashContext ctx = config.getCrypto().startHash();
		for(RevisionTag parent : sorted) {
			ctx.update(parent.getBytes());
		}
		
		parentHash = Util.shortTag(ctx.finish());
		
		if(parentHash != revTag.getParentHash()) {
			throw new SecurityException("parent hash for " + Util.bytesToHex(revTag.getBytes(), 4) + " does not match; expected " + String.format("%016x", revTag.getParentHash()) + " got " + String.format("%016x", parentHash));
		}
	}
	
	protected synchronized boolean fetchParentsForTag(RevisionTag revTag, long timeoutMs) {
		// priority just a bit superior to the default for file lookups since these should go fast
		logger.debug("RevisionList {}: Fetching parents for tag {}, timeout {}ms",
				Util.formatArchiveId(config.getArchiveId()),
				Util.formatRevisionTag(revTag),
				timeoutMs);
		config.swarm.requestRevisionDetails(SwarmFS.REQUEST_PRIORITY+1, revTag);
		long endTime = timeoutMs < 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;
		
		while(parentsForTagLocal(revTag) == null && !config.isClosed() && System.currentTimeMillis() < endTime) {
			try {
				this.wait(Math.min(100, endTime - System.currentTimeMillis()));
			} catch(InterruptedException exc) {}
		}
		
		if(parentsForTagLocal(revTag) == null) {
			logger.info("RevisionList {}: Timed out fetching parents for tag {}, timeout {}ms",
					Util.formatArchiveId(config.getArchiveId()),
					timeoutMs);
			return false;
		}
		
		return true;
	}
}
