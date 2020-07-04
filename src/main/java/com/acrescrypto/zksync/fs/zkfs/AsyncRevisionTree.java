package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.HashContext;
import com.acrescrypto.zksync.exceptions.SearchFailedException;
import com.acrescrypto.zksync.fs.swarmfs.SwarmFS;
import com.acrescrypto.zksync.utility.Gather;
import com.acrescrypto.zksync.utility.Util.OpportunisticExceptionHandler;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;

public class AsyncRevisionTree {
	public interface ExceptionCallback {
		void exception(Exception exc);
	}
	public interface ParentsNotFoundCallback {
		void failed() throws Exception;
	}
	
	public interface ParentsFoundCallback {
		void found(Collection<RevisionTag> parents) throws Exception;
	}
	
	public interface DetailedParentsNotFoundCallback {
		void failed(RevisionTag tag) throws Exception;
	}
	
	public interface DetailedParentsFoundCallback {
		void found(RevisionTag tag, Collection<RevisionTag> parents) throws Exception;
	}
	
	public interface TraversalCallback {
		boolean encountered(RevisionTag tag) throws Exception;
	}
	
	public interface ParallelTraversalCallback {
		boolean found(long height, Map<RevisionTag,Collection<RevisionTag>> parentsByTag) throws Exception;
	}
	
	public interface TraversalFinishedCallback {
		void finished() throws Exception;
	}
	
	public interface TraversalAllowedCallback {
		void allowed() throws Exception;
	}
	
	public interface TagWithPrefixCallback {
		void foundTag(RevisionTag tag) throws Exception;
	}
	
	public interface DescendentOfCallback {
		void descendentResult(boolean isDescendent) throws Exception;
	}
	
	public interface IsSupercededCallback {
		void supercededResult(boolean isSuperceded) throws Exception;
	}
	
	public interface CommonAncestorCallback {
		void foundAncestor(RevisionTag ancestor) throws Exception;
	}
	
	public interface RevisionSetCallback {
		void revisionSet(Collection<RevisionTag> revisions) throws Exception;
	}
	
	public class Traversal {
		protected long                            lookupTimeoutMs;
		protected int                             activeQueries;
		protected DetailedParentsNotFoundCallback notFoundCallback;
		protected DetailedParentsFoundCallback    foundCallback;
		protected TraversalCallback               traversalCallback;
		protected TraversalFinishedCallback       finishedCallback;
		protected long                            minHeight;
		protected HashSet<RevisionTag>            seen                   = new HashSet<>(),
				                                  pending                = new HashSet<>(),
		                                          queried                = new HashSet<>();
		protected boolean                         searchComplete;
		protected Queue<PendingHeightCallback>    pendingHeightCallbacks = new LinkedList<>();
		
		protected class PendingHeightCallback {
			long                     height;
			TraversalAllowedCallback callback;
			
			public PendingHeightCallback(long height, TraversalAllowedCallback callback) {
				this.height   = height;
				this.callback = callback;
			}
		}
		
		public Traversal() {
			this.minHeight       = 0;
			this.lookupTimeoutMs = defaultLookupTimeout();
		}
		
		public Traversal traverseFromBranchTips() throws Exception {
			ArrayList<RevisionTag> tips = config.getRevisionList().branchTips();
			
			for(RevisionTag tip : tips) {
				traverseFromTag(tip);
			}
			
			return this;
		}
		
		public Traversal traverseFromTag(RevisionTag tag) throws Exception {
			if(queried.contains(tag)) return this;
			queried.add(tag);
			pending.add(tag);
			
			return whenHeightAllowed(tag.getHeight(), ()->{
				activeQueries++;
				
				parentsForTag(
						tag,
						this.lookupTimeoutMs,
						(tag_, parents)->{
							pending.remove(tag);
							found(tag, parents);
							if(isComplete())                 return;
							
							for(RevisionTag parent : parents) {
								traverseFromTag(parent);
							}
							
							finishedQuery();
						},
						(tag_)->{
							pending.remove(tag);
							notFound(tag);
							finishedQuery();
						});
			});
		}
		
		public Traversal whenHeightAllowed(long height, TraversalAllowedCallback callback) throws Exception {
			if(minHeight <= height) {
				callback.allowed();
				return this;
			}
			
			pendingHeightCallbacks.add(new PendingHeightCallback(height, callback));
			return this;
		}
		
		public Traversal setLookupTimeoutMs(long lookupTimeoutMs) {
			this.lookupTimeoutMs = lookupTimeoutMs;
			return this;
		}
		
		public Traversal setNotFoundCallback(DetailedParentsNotFoundCallback notFoundCallback) {
			this.notFoundCallback = notFoundCallback;
			return this;
		}
		
		public Traversal setFoundCallback(DetailedParentsFoundCallback foundCallback) {
			this.foundCallback = foundCallback;
			return this;
		}
		
		public Traversal setTraversalCallback(TraversalCallback traversalCallback) {
			this.traversalCallback = traversalCallback;
			return this;
		}
		
		public Traversal setFinishedCallback(TraversalFinishedCallback finishedCallback) {
			this.finishedCallback = finishedCallback;
			return this;
		}
		
		public Traversal setMinHeight(long minHeight) throws Exception {
			this.minHeight = minHeight;
			Queue<PendingHeightCallback> remainingCallbacks = new LinkedList<>();
			
			for(PendingHeightCallback callback : pendingHeightCallbacks) {
				if(callback.height >= minHeight) {
					callback.callback.allowed();
					continue;
				}
				
				remainingCallbacks.add(callback);
			}
			return this;
		}
		
		public long getLookupTimeoutMs() {
			return lookupTimeoutMs;
		}
		
		public DetailedParentsNotFoundCallback getNotFoundCallback() {
			return notFoundCallback;
		}
		
		public DetailedParentsFoundCallback getFoundCallback() {
			return foundCallback;
		}
		
		public TraversalCallback getTraversalCallback() {
			return traversalCallback;
		}
		
		public TraversalFinishedCallback getFinishedCallback() {
			return finishedCallback;
		}
		
		public long getMinHeight() {
			return minHeight;
		}
		
		public boolean isComplete() {
			return searchComplete;
		}
		
		protected boolean hasQueuedAtHeight(long height) {
			for(RevisionTag tag : pending) {
				if(tag.getHeight() >= height) return true;
			}
			
			return false;
		}
		
		protected boolean encountered(RevisionTag tag) throws Exception {
			if(isComplete())              return  true;
			if(traversalCallback == null) return false;
			if(seen.contains(tag))        return false;
			
			seen.add(tag);
			if(traversalCallback.encountered(tag)) {
				markComplete();
				return true;
			}
			
			return false;
		}
		
		protected boolean found(RevisionTag tag, Collection<RevisionTag> parents) throws Exception {
			if(isComplete()) return true;
			if(foundCallback != null) {
				foundCallback.found(tag, parents);
			}
			
			if(encountered(tag)) return true;
			for(RevisionTag parent : parents) {
				if(encountered(parent)) return true;
			}
			
			return false;
		}
		
		protected boolean notFound(RevisionTag tag) throws Exception {
			if(isComplete()) return true;
			if(notFoundCallback != null) {
				notFoundCallback.failed(tag);
			}
			
			return encountered(tag);
		}
		
		protected void finishedQuery() throws Exception {
			activeQueries--;
			if(activeQueries == 0 && pendingHeightCallbacks.isEmpty()) {
				markComplete();
			}
		}
		
		protected void markComplete() throws Exception {
			if(isComplete()) return;
			
			searchComplete = true;
			if(finishedCallback != null) finishedCallback.finished();
		}
	}
	
	public class ParallelTraversal {
		protected long                            lookupTimeoutMs;
		protected ParallelTraversalCallback       tierCallback;
		protected DetailedParentsNotFoundCallback notFoundCallback;
		protected TraversalFinishedCallback       finishedCallback;
		protected long                            minHeight,
		                                          presentHeight;
		protected HashSet<RevisionTag>            pending             = new HashSet<>();
		protected HashMap<RevisionTag,Traversal>  traversals          = new HashMap<>();
		protected HashMap<RevisionTag,
		                  HashMap<Long,HashSet<RevisionTag>>
		                 >                        parentsByTagAndTier = new HashMap<>();
		protected boolean                         searchComplete;
		
		public ParallelTraversal() {
			this.minHeight = Long.MAX_VALUE;
			this.lookupTimeoutMs = defaultLookupTimeout();
		}
		
		public ParallelTraversal setTierCallback(ParallelTraversalCallback tierCallback) {
			this.tierCallback = tierCallback;
			return this;
		}
		
		public ParallelTraversal setFinishedCallback(TraversalFinishedCallback finishedCallback) {
			this.finishedCallback = finishedCallback;
			return this;
		}
		
		public ParallelTraversal setNotFoundCallback(DetailedParentsNotFoundCallback notFoundCallback) {
			this.notFoundCallback = notFoundCallback;
			return this;
		}
		
		public void traverseFromTags(Collection<RevisionTag> tags) throws Exception {
			for(RevisionTag tag : tags) {
				// set minHeight to 1 + tallest tag, since runNextTraversalRound decrements
				this.minHeight = Math.max(this.minHeight, tag.getHeight() + 1);
				
				Traversal traversal = new Traversal()
					.setTraversalCallback((ancestor)->{
						long height = ancestor.getHeight();
						parentsByTagAndTier.putIfAbsent(tag, new HashMap<>());
						HashMap<Long, HashSet<RevisionTag>> parents = new HashMap<>();
						parents.putIfAbsent(height, new HashSet<>());
						parents.get(height).add(ancestor);
						
						if(isDoneWithTier(height)) {
							makeTierCallback(height);
						}
						
					    return true;
				  }).setNotFoundCallback((missing)->{
					    if(notFoundCallback != null) {
					    	this.notFoundCallback.failed(missing);
					    }
				  }).setFinishedCallback(()->{
					  this.finishedCallback.finished();
				  }).setMinHeight(Long.MAX_VALUE)
				    .traverseFromTag(tag);
				
				traversals.put(tag, traversal);
			}
			
			runNextTraversalRound();
		}
		
		protected void makeTierCallback(long height) throws Exception {
			HashMap<RevisionTag, Collection<RevisionTag>> parentsByTagAtTier = new HashMap<>();
			
			for(RevisionTag tag : parentsByTagAndTier.keySet()) {
				LinkedList<RevisionTag> tagsForTier = new LinkedList<>();
				tagsForTier.addAll(parentsByTagAndTier.get(tag).getOrDefault(height, new HashSet<>()));
				parentsByTagAtTier.put(tag, tagsForTier);
			}
			
			if(!tierCallback.found(height, parentsByTagAtTier)) {
				runNextTraversalRound();
			}
		}
		
		protected void runNextTraversalRound() throws Exception {
			minHeight--;
			for(Traversal traversal : traversals.values()) {
				traversal.setMinHeight(minHeight);
			}
		}
		
		protected boolean isDoneWithTier(long height) {
			for(Traversal traversal : traversals.values()) {
				if(traversal.hasQueuedAtHeight(height)) return false;
			}
			
			return true;
		}
	}
	
	protected ConcurrentHashMap<RevisionTag,LinkedList<DetailedParentsFoundCallback>> pendingTags      = new ConcurrentHashMap<>();
	protected ConcurrentHashMap<RevisionTag,LinkedList<RevisionTag>>                  cache            = new ConcurrentHashMap<>();
	protected ConcurrentHashMap<RevisionTag,Long>                                     lastRequestTimes = new ConcurrentHashMap<>();
	protected ZKArchiveConfig config;
	protected Logger          logger   = LoggerFactory.getLogger(AsyncRevisionTree.class);
	
	public AsyncRevisionTree(ZKArchiveConfig config) {
	}
	
	public void close() {
	}
	
	public void clear() {
		// wipe cache
	}
	
	public void addParentsForTag(RevisionTag revTag, Collection<RevisionTag> parents) {
		validateParentList(revTag, parents);
		cache.put(revTag, new LinkedList<>(parents));
	}
	
	public boolean hasParentsForTag(RevisionTag revTag) throws IOException {
		if(cache.containsKey(revTag))                     return  true;
		if(config.archive.isClosed())                     return false;
		if(config.archive.hasInodeTableFirstPage(revTag)) {
			this.addParentsForTag(revTag, revTag.getInfo().getParents());
			return true;
		}
		
		return false;
	}
	
	public void parentsForTag(
		            RevisionTag revTag,
		   ParentsFoundCallback onFound,
		ParentsNotFoundCallback onNotFound)
			             throws Exception
	{
		parentsForTag(
				revTag,
				defaultLookupTimeout(),
				(tag, parents) -> onFound   .found (parents),
				(tag         ) -> onNotFound.failed()
			);
	}
	
	public void parentsForTag(
		                   RevisionTag revTag,
			                      long timeoutMs,
	      DetailedParentsFoundCallback onFound,
	   DetailedParentsNotFoundCallback onNotFound)
				                throws Exception
	{
		class Wrapper {
			DetailedParentsFoundCallback callback;
		}
		
		Wrapper wrapper = new Wrapper();
		
		if(hasParentsForTag(revTag)) {
			onFound.found(revTag, cache.get(revTag));
			return;
		}
		
		// request details, unless we already did that recently
		long lastTime = lastRequestTimes.getOrDefault(revTag, 0L);
		long retryInterval = config.getMaster().getGlobalConfig().getInt("net.swarm.revisionLookupRetryIntervalMs");
		if(Util.currentTimeMillis() - lastTime >= retryInterval) {
			config.getSwarm().requestRevisionDetails(SwarmFS.REQUEST_PRIORITY + 1, revTag);
			lastRequestTimes.put(revTag, Util.currentTimeMillis());
		}
		
		pendingTags.putIfAbsent(revTag, new LinkedList<>());
		LinkedList<DetailedParentsFoundCallback> callbackList = pendingTags.get(revTag);

		SnoozeThread snooze = new SnoozeThread(timeoutMs, false, ()->{
			callbackList.remove(wrapper.callback);
			if(callbackList.isEmpty()) {
				pendingTags.remove(revTag);
			}
			
			try {
				onNotFound.failed(revTag);
			} catch(Exception exc) {
				logger.error("RevisionTree {}: Caught exception invoking notFound callback for look up of tag {}",
						Util.formatArchiveId(config.getArchiveId()),
						Util.formatRevisionTag(revTag));
			}
			
		});
		
		wrapper.callback = (tag, parents) -> {
			boolean alreadyTimedOut = snooze.isExpired();
			snooze.cancel();
			if(alreadyTimedOut) return;
			
			callbackList.remove(wrapper.callback);
			if(callbackList.isEmpty()) {
				pendingTags.remove(revTag);
			}
			
			onFound.found(revTag, parents);
		};
		
		callbackList.add(wrapper.callback);
	}
	
	public Collection<RevisionTag> parentsForTagLocal(RevisionTag revTag) throws IOException {
		if(hasParentsForTag(revTag)) {
			return cache.get(revTag);
		} else {
			return null;
		}
	}
	
	public void tagWithPrefix(String prefix, TagWithPrefixCallback callback) throws Exception {
		new Traversal()
			.setTraversalCallback((tag) -> {
				if(!tag.matchesPrefix(prefix)) return false;
				callback.foundTag(tag);
				return true;
		  }).traverseFromBranchTips();
	}
	
	public void commonAncestor(
			Collection<RevisionTag>           revTags,
			CommonAncestorCallback            callback,
			OpportunisticExceptionHandler     exceptionCallback
		) 
	{
		Util.handleExceptions(exceptionCallback, ()->{
			MutableBoolean madeCallback = new MutableBoolean();
			new ParallelTraversal()
			  .setTierCallback( (height, listsByTag) -> {
				HashMap<RevisionTag,Integer> parentCounts = new HashMap<>();
				for(Collection<RevisionTag> tags : listsByTag.values()) {
					for(RevisionTag tag : tags) {
						int existingValue = parentCounts.getOrDefault(tag, 0),
								 newValue = existingValue + 1; 
						if(newValue == revTags.size()) {
							madeCallback.setTrue();
							callback.foundAncestor(tag);
							return true;
						}
						
						parentCounts.put(tag, newValue);
					}
				}
				
				return madeCallback.isTrue();
		    }).setNotFoundCallback( (missing)->{
			  madeCallback.setTrue();
			  exceptionCallback.exception(new SearchFailedException(missing));
		    }).setFinishedCallback( ()->{
			  if(!madeCallback.isTrue()) {
				  madeCallback.setTrue();
				  callback.foundAncestor(null);
			  }
		    }).traverseFromTags(revTags);
		});
		
		// breadth-first search of each tag's parent tree for any common parent tag 
	}
	
	public void commonAncestor(
			RevisionTag[]                    revTags,
			CommonAncestorCallback           callback,
			OpportunisticExceptionHandler    exceptionCallback
		)
	{
		LinkedList<RevisionTag> revTagsList = new LinkedList<>();
		for(RevisionTag tag : revTags) {
			revTagsList.add(tag);
		}
		
		commonAncestor(revTagsList, callback, exceptionCallback);
	}
	
	public void descendentOf(
			RevisionTag                   tag,
		    RevisionTag                   possibleAncestor,
			DescendentOfCallback          callback,
			OpportunisticExceptionHandler exceptionCallback
	)
	{
		Util.handleExceptions(exceptionCallback, ()->{
			boolean     isBlank = possibleAncestor.equals(config.blankRevisionTag()),
					isSameAsTag = possibleAncestor.equals(tag);
				if(isBlank || isSameAsTag) {
					callback.descendentResult(true);
					return;
				}
				
				if(tag.getHeight() <= possibleAncestor.getHeight()) {
					callback.descendentResult(false);
					return;
				}
			
				MutableBoolean isDescendent = new MutableBoolean();
				new Traversal()
					.setMinHeight(possibleAncestor.getHeight())
					.setTraversalCallback((ancestor)->{
						if(ancestor.equals(possibleAncestor)) {
							isDescendent.setTrue();
							return true;
						}
					
						return false;
				  }).setNotFoundCallback((missing)->{
					  exceptionCallback.exception(new SearchFailedException(missing));
				  }).setFinishedCallback(()->{
						callback.descendentResult(isDescendent.booleanValue());
				  }).traverseFromBranchTips();
		});
	}
	
	/** Is an existing tag superceded by a new tag? (ie. the new one is descendent from the existing,
	 * or is a merge containing the same ancestors)
	 * @throws IOException 
	 */
	public void supercededBy(
			RevisionTag                      newTag,
			RevisionTag                      existing,
			IsSupercededCallback             callback,
			OpportunisticExceptionHandler    exceptionCallback
		)
	{
		new Gather<RevisionTag,Boolean>()
			// tags don't supercede themselves
			.check((g) -> { if(newTag.equals       (existing)           ) g.finish(false); })
			
			// newTag can't supercede existing if newTag is at a lower height
			.check((g) -> { if(newTag.getHeight() < existing.getHeight()) g.finish(false); })
			
			// newTag definitely supercedes existing if newTag descends from existing
			.check((g) -> {
				descendentOf(
						newTag,
						existing,
						(isDescendent) -> {
							if(isDescendent) {
								g.finish(true);
								return;
							}
						},
						(exc) -> g.exception(exc)
					);
			
			// look up parents of existing to see if newTag supercedes by merge superset rule
		  }).with((g, ready) -> {
				parentsForTag(
					existing,
					(parents) -> {
						/* if existing has 0 or 1 parents (i.e. is not a merge), merge superset
						 * rule do not apply, and existing is not superceded. */
						if(parents.size() <= 1) {
							g.finish(false);
							return;
						}
						
						// add parents to processing queue
						ready.found(parents);
					},
					() -> {
						// error looking up parents of existing
						logger.warn("Unable to get parent information for " + existing + ", assuming tag is not superceded by " + newTag);
						g.exception(new SearchFailedException(existing));
					});
				
			/* newTag supercedes existing by merge superset rule if:
			 *   1) existing is a merge (i.e. existing has >= 2 parents)
			 *   2) for each parent p of existing, newTag is a descendent of p
			 */
		  }).find(true, (g, parent)->{
			  descendentOf(
				newTag,
				parent,
				(isDescendent)->{
					if(!isDescendent) {
						/* found a parent of existing that newTag is NOT descended from;
						 * newTag does not supercede existing by merge superset rule. */
						g.finish(false);
					}
				},
				(exc)->g.exception(exc));
		  }).result((g, newTagIsSuperceded) -> {
			  callback.supercededResult(newTagIsSuperceded);
		  }).passExceptions(exceptionCallback)
		    .run();
	}
	
	/** Do we already have a revtag superceding all the data in this revtag? 
	 * @throws IOException */
	public void isSuperceded(
			RevisionTag                      revTag,
			IsSupercededCallback             callback,
			OpportunisticExceptionHandler    exceptionCallback
		) {

		new Gather<RevisionTag, Boolean>()
		  .add(config.getRevisionList().branchTips())
		  .find(false, (g, tip)->{
			// First check if revTag is an ancestor of any branch tip. If yes, it is superceded.
			descendentOf(
					tip,
					revTag,
					(isDescendent)->{
						if(isDescendent) {
							g.finish(true);
							return;
						}
					},
					(exc)->g.exception(exc));
		}).result((g, revTagDescendsFromTip)->{
			if(revTagDescendsFromTip) {
				callback.supercededResult(true);
				return;
			}
			
			parentsForTag(
				revTag,
				(parents)->{
					/* Maybe revTag is a merge, and one of our branch tips is a supermerge of
					 * revTag (i.e. all the parents of revTag are ancestors of one of our tips.)
					 * If so, revTag is superceded.
					 */
					// TODO: what if high order bit of revtag parent hash or height encoded whether multiple parents are present?
					if(parents.size() <= 1) {
						// revTag is not a merge, and so it is not superceded.
						callback.supercededResult(false);
						return;
					}
					
					new Gather<RevisionTag, Boolean>()
					  .add(g.arguments()) // parents
					  .find(true, (gg, tip) -> {
						new Gather<RevisionTag, Boolean>()
						  .add(parents)
						  .find(true, (ggg, parent)->{
							/* for each branch tip, check every parent of revTag,
							** and if any of them are NOT ancestors to that tip, then it can't
							** supercede revTag */ 
							descendentOf(
									tip,
									parent,
									(tipDescendsFromParent) -> {
									  if(!tipDescendsFromParent) {
										  gg.finish(false);
										  return;
									  }
									},
									(exc)->g.exception(exc));
						}).result( (ggg, tipIsDescendedFromAllParents) -> {
						  if(tipIsDescendedFromAllParents) {
							  gg.finish(true);
						  }
						}).run();
					}).result((gg, tipContainsSupermerge) -> {
					  // Is there at least one branch tip descended from each of our parents?
					  callback.supercededResult(tipContainsSupermerge);
					}).run();
				},
				()->{
					g.exception(new SearchFailedException(revTag));
				});
		}).passExceptions(exceptionCallback)
		  .run();
	}
	
	/** Given a revision set, eliminate any revisions that are ancestral to other revisions in
	 * the set.
	 * @throws IOException 
	 */
	public void minimalSet(
			Collection<RevisionTag>             revTags,
			RevisionSetCallback                 callback,
			OpportunisticExceptionHandler       exceptionCallback
	) {
		HashSet<RevisionTag> minimal = new HashSet<> (revTags);
		MutableInt           pending = new MutableInt(0);
		MutableBoolean         ready = new MutableBoolean();
		int                   queued = 0;
		
		for(RevisionTag tag : revTags) {
			int localQueued = 0;
			queued++;
			
			for(RevisionTag other : revTags) {
				localQueued++;
				ready.setValue(queued == localQueued && queued == revTags.size());
				
				if(other == tag)                         continue;
				if(other.getHeight() <= tag.getHeight()) continue;
				pending.increment();
				
				descendentOf(
						other,
						tag,
						(isDescendent)->{
							if(isDescendent) {
								minimal.remove(tag);
							}
							
							pending.decrement();
							if(ready.getValue() && pending.decrementAndGet() == 0) {
								callback.revisionSet(minimal);
							}
						},
						exceptionCallback);
			}
		}
		
		Util.handleExceptions(exceptionCallback, ()->{
			if(pending.getValue() == 0) {
				callback.revisionSet(minimal);
			}
		});
	}
	
	/** Return a set of bases that are suitable for listing as parents to a merge.
	 * A tag is not suitable to be listed as a parent in a merge if it has multiple parents
	 * (i.e. it is itself a merge). This is because it is possible for many-way merges to
	 * take place in which merges of various subsets of the mergeset are created first. If
	 * this happens, and each merge was allowed to act as a parent, then not only would each
	 * subset create a unique revtag, but each merge of each subset would also create new
	 * revtags, and merges of each merge of each subset.
	 * 
	 * Thus, merges list only "canonical bases" as parents. If a parent has multiple parents,
	 * then those parents are considered for inclusion and the algorithm recurses into their
	 * parents if needed.
	 * 
	 * In other words, suppose the following graph exists:
	 *   O    <- A
	 *   O    <- B
	 *   O    <- C
	 *   A, B <- D
	 *   C, D <- R
	 * (where "X <- Y" denotes X is a parent of Y)
	 * then the canonical bases of D are [A, B], and the canonical bases of R are [A, B, C]. 
	 * @throws IOException */
	public void canonicalBases(
			Collection<RevisionTag>         revTags,
			RevisionSetCallback             callback,
			OpportunisticExceptionHandler   exceptionCallback
		)
	{
		MutableInt         pending = new MutableInt(revTags.size());
		HashSet<RevisionTag> bases = new HashSet<>();
		
		commonAncestor(
				revTags,
				(ancestor)->{
					// We will accept the common ancestor of the entire set as a base.
					for(RevisionTag tag : revTags) {
						canonicalBasesRecursion(
								tag,
								pending,
								bases,
								callback,
								exceptionCallback);
					}
				},
				exceptionCallback);
	}
	
	protected void canonicalBasesRecursion(
			RevisionTag                     tag,
			MutableInt                      pending,
			Collection<RevisionTag>         bases,
			RevisionSetCallback             callback,
			OpportunisticExceptionHandler   exceptionCallback
		)
	{
		Util.handleExceptions(exceptionCallback, ()->{
			parentsForTag(
					tag,
					(parents)->{
						int remainingLookups = pending.decrementAndGet();
						if(parents.size() == 1) {
							bases.add(tag);
							if(remainingLookups == 0) {
								callback.revisionSet(bases);
							}
							return;
						}
						
						pending.add(parents.size());
						for(RevisionTag parent : parents) {
							canonicalBasesRecursion(
									parent,
									pending,
									bases,
									callback,
									exceptionCallback);
						}
					},
					()->{
						exceptionCallback.exception(new SearchFailedException(tag));
					});
		});
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
	
	public long defaultLookupTimeout() {
		return config
			.getMaster()
			.getGlobalConfig()
			.getLong("net.swarm.revisionlookuptimeoutms");
	}
}
