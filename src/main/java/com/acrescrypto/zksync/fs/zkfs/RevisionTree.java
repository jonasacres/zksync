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
import com.acrescrypto.zksync.exceptions.SearchFailedException;
import com.acrescrypto.zksync.fs.swarmfs.SwarmFS;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.HashCache;
import com.acrescrypto.zksync.utility.Util;

public class RevisionTree {
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
			while(height >= 0) {
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
	HashCache<RevisionTag, HashSet<RevisionTag>> map = new HashCache<>(256,
			(tag)->{
				if(hasParentsForTag(tag)) {
					return new HashSet<>(tag.getInfo().parents);
				} else {
					Collection<RevisionTag> parents = parentsForTag(tag);
					if(parents == null) {
						throw new SearchFailedException();
					}
					
					return new HashSet<>(parents);
				}
			},
			(tag,parents)->{});
	protected GroupedThreadPool threadPool;
	protected final Logger logger = LoggerFactory.getLogger(RevisionTree.class); 
	
	public RevisionTree(ZKArchiveConfig config) {
		this.config = config;
		
		try {
			this.map.setCapacity(config.getMaster().getGlobalConfig().getInt("fs.settings.revisionTreeCacheSize"));
		} catch(IOException exc) {
			logger.error("Caught exception setting RevisionTree capacity; proceeding anyway", exc);
		}
		
		config.getMaster().getGlobalConfig().subscribe("fs.settings.revisionTreeCacheSize").asInt((s)-> {
			try {
				logger.info("Setting revision tree capacity to {}; was {}",
						s,
						this.map.getCapacity());
				this.map.setCapacity(s);
			} catch(IOException exc) {
				logger.error("Caught exception setting RevisionTree capacity", exc);
			}
		});
		
		threadPool = GroupedThreadPool.newWorkStealingThreadPool(config.getThreadGroup(), "RevisionTree lookup");
	}
	
	public synchronized void clear() throws IOException {
		map.removeAll();
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
				logger.error("Caught exception adding parents for tag", exc);
			}
		}
	}

	public boolean hasParentsForTag(RevisionTag revTag) {
		try {
			if(map.hasCached(revTag)) return true;
			if(config.archive.isClosed()) return false;
			if(config.archive.hasInodeTableFirstPage(revTag)) return true;
		} catch(IOException exc) {
			logger.error("Caught IOException checking status of revTag", exc);
		}
		
		return false;
	}
	
	public Collection<RevisionTag> parentsForTag(RevisionTag revTag) {
		return parentsForTag(revTag, treeSearchTimeoutMs);
	}
	
	public Collection<RevisionTag> parentsForTag(RevisionTag revTag, long timeoutMs) {
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
				logger.error("Encountered IOException looking up cached revTag", exc);
			} catch (IOException exc) {
				logger.error("Encountered IOException looking up cached revTag", exc);
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
		return new TreeSearchItem(tag).hasAncestor(possibleAncestor);
	}
	
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
	
	public Collection<RevisionTag> canonicalBases(Collection<RevisionTag> revTags) throws SearchFailedException {
		HashSet<RevisionTag> bases = new HashSet<>();
		LinkedList<RevisionTag> toProcess = new LinkedList<>(revTags);
		while(!toProcess.isEmpty()) {
			RevisionTag current = toProcess.pop();
			Collection<RevisionTag> parents = parentsForTag(current, treeSearchTimeoutMs);
			if(parents.size() > 1) {
				toProcess.addAll(parents);
			} else {
				bases.add(current);
			}
		}
		
		return bases;
	}
	
	/** Do we already have a revtag superceding all the data in this revtag? 
	 * @throws SearchFailedException */
	public boolean isSuperceded(RevisionTag revTag) throws SearchFailedException {
		ArrayList<RevisionTag> tips = config.getRevisionList().branchTips();

		for(RevisionTag tip : tips) {
			if(tip.equals(revTag)) continue;
			if(descendentOf(tip, revTag)) {
				return true; // we have a tip that descends from this tag
			}
		}
		
		if(revTag.getHeight() > 1) { // this micro-optimization helps simplify test-writing (no need to provide parent lists for revtags of height 1)
			Collection<RevisionTag> parents = parentsForTag(revTag);
			if(parents == null) {
				if(config.isClosed()) return false; // avoid needless log spam
				throw new SearchFailedException();
			}
			
			if(parents.size() > 1) {
				for(RevisionTag tip : tips) {
					if(tip.equals(revTag)) continue;
					// if this is a merge, do we already have a merge including everything this one does?
					Collection<RevisionTag> tipParents = parentsForTagLocal(tip);
					if(tipParents != null && tipParents.containsAll(parents)) {
						return true;
					}
					
					boolean containsParents = true;
					for(RevisionTag parent : parents) {
						if(!descendentOf(tip, parent)) {
							containsParents = false;
							break;
						}
					}
					
					if(containsParents) return true;
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
		config.swarm.requestRevisionDetails(SwarmFS.REQUEST_PRIORITY+1, revTag);
		long endTime = timeoutMs < 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;
		
		while(parentsForTagLocal(revTag) == null && !config.isClosed() && System.currentTimeMillis() < endTime) {
			try {
				this.wait(Math.min(100, endTime - System.currentTimeMillis()));
			} catch(InterruptedException exc) {}
		}
		
		return parentsForTagLocal(revTag) != null;
	}
}
