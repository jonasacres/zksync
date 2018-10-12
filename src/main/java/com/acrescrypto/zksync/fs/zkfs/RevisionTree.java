package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
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
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.SearchFailedException;
import com.acrescrypto.zksync.fs.swarmfs.SwarmFS;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.Util;

public class RevisionTree {
	public final static int DEFAULT_TREE_SEARCH_TIMEOUT_MS = 30000; // when finding ancestors, how long to wait before giving up on a lookup?
	public static int treeSearchTimeoutMs = DEFAULT_TREE_SEARCH_TIMEOUT_MS;
	
	class TreeSearchItem {
		ArrayList<RevisionTag> revTags = new ArrayList<>();
		HashMap<Long,ArrayList<RevisionTag>> deferred = new HashMap<>();
		long height;
		
		TreeSearchItem(RevisionTag tag) {
			this.height = tag.height;
			revTags.add(tag);			
		}
		
		boolean hasAncestor(RevisionTag tag) throws SearchFailedException {
			while(height >= 0) {
				if(revTags.contains(tag)) {
					return true;
				}
				
				for(ArrayList<RevisionTag> list : deferred.values()) {
					if(list.contains(tag)) return true;
				}
				
				if(height > 0) recurse();
				else return false;
			}
			
			return false;
		}
		
		void recurse() throws SearchFailedException {
			ArrayList<RevisionTag> newRevTags = new ArrayList<>();
			height--;
			
			if(deferred.containsKey(height)) {
				newRevTags.addAll(deferred.get(height));
				deferred.remove(height);
			}
			
			ArrayList<Future<?>> futures = new ArrayList<>();
			for(RevisionTag revTag : revTags) {
				Future<?> future = threadPool.submit(()->{
					Collection<RevisionTag> parents = parentsForTag(revTag, treeSearchTimeoutMs);
					if(parents == null) return;
					for(RevisionTag parent : parents) {
						if(parent.height == height) {
							synchronized(newRevTags) {
								newRevTags.add(parent);
							}
						} else {
							deferred.putIfAbsent(parent.height, new ArrayList<>());
							deferred.get(parent.height).add(parent);
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
					exc.printStackTrace();
					exc.getCause().printStackTrace();
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
	HashMap<RevisionTag, HashSet<RevisionTag>> map = new HashMap<>();
	boolean autowrite = true;
	protected GroupedThreadPool threadPool;
	protected final Logger logger = LoggerFactory.getLogger(RevisionTree.class); 
	
	public RevisionTree(ZKArchiveConfig config) {
		this.config = config;
		threadPool = GroupedThreadPool.newFixedThreadPool(config.getThreadGroup(), "RevisionTree lookup", 8);
		try {
			read();
		} catch(ENOENTException|SecurityException exc) {
			try {
				scan();
			} catch (IOException exc2) {
				logger.error("Caught exception scanning revision tree from known branches", exc2);
			}
		} catch (IOException exc) {
			logger.error("Caught exception reading revision tree", exc);
		}
	}
	
	public void clear() throws IOException {
		map.clear();
		if(autowrite) {
			write();
		}
	}
	
	public void addParentsForTag(RevisionTag revTag, Collection<RevisionTag> parents) {
		if(map.containsKey(revTag)) return;
		validateParentList(revTag, parents);
		
		synchronized(this) {
			HashSet<RevisionTag> parentSet = new HashSet<>(parents);
			map.put(revTag, parentSet);
			this.notifyAll();
		}
		
		if(autowrite) {
			try {
				write();
			} catch (IOException exc) {
				logger.error("Caught exception writing revision tree", exc);
			}
		}
	}
	
	/** Walk through all the revisions we know about and add them. */
	public synchronized void scan() throws IOException {
		boolean oldAutowrite = autowrite;
		autowrite = false;
		if(config.accessor.isSeedOnly()) return;
		for(RevisionTag tip : config.revisionList.branchTips()) {
			scanRevTag(tip);
		}
		autowrite = oldAutowrite;
	}
	
	protected void scanRevTag(RevisionTag revTag) throws IOException {
		if(revTag.refTag.isBlank()) return;
		
		// we're not willing to download pages from the swarm for this, so skip anything we don't have
		if(!config.archive.hasInode(revTag, InodeTable.INODE_ID_INODE_TABLE)) return;
		
		// don't use readOnlyFS here since that will flood our cache with old revisions
		Collection<RevisionTag> parents = revTag.getFS().inodeTable.revision.parents;
		
		/* this gets called automatically anyway on the tree in the ZKArchiveConfig -- we want to
		 * guarantee that the tags get added to this instance, so we'll call again to be safe.
		 */
		addParentsForTag(revTag, parents);
		for(RevisionTag parent : parents) {
			scanRevTag(parent);
		}
	}
	
	public boolean hasParentsForTag(RevisionTag revTag) {
		return map.containsKey(revTag);
	}
	
	public Collection<RevisionTag> parentsForTag(RevisionTag revTag, long timeoutMs) {
		Collection<RevisionTag> r = parentsForTagNonblocking(revTag);
		if(r != null) return r;
		fetchParentsForTag(revTag, timeoutMs);
		return parentsForTagNonblocking(revTag);
	}
	
	public Collection<RevisionTag> parentsForTagNonblocking(RevisionTag revTag) {
		return map.getOrDefault(revTag, null);		
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
	
	public int numRevisions() {
		return map.size();
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
		
		if(parentHash != revTag.parentHash) {
			throw new SecurityException("parent hash for " + Util.bytesToHex(revTag.getBytes(), 4) + " does not match; expected " + String.format("%016x", revTag.parentHash) + " got " + String.format("%016x", parentHash));
		}
	}
	
	protected synchronized boolean fetchParentsForTag(RevisionTag revTag, long timeoutMs) {
		// priority just a bit superior to the default for file lookups since these should go fast
		config.swarm.requestRevisionDetails(SwarmFS.REQUEST_PRIORITY+1, revTag);
		long endTime = timeoutMs < 0 ? Long.MAX_VALUE : System.currentTimeMillis() + timeoutMs;
		
		while(parentsForTagNonblocking(revTag) == null && !config.isClosed() && System.currentTimeMillis() < endTime) {
			try {
				this.wait(Math.min(100, endTime - System.currentTimeMillis()));
			} catch(InterruptedException exc) {}
		}
		
		return parentsForTagNonblocking(revTag) != null;
	}
	
	public synchronized void write() throws IOException {
		MutableSecureFile
		  .atPath(config.localStorage, getPath(), key())
		  .write(serialize(), 65536);
	}
	
	public void read() throws IOException {
		deserialize(MutableSecureFile
				  .atPath(config.localStorage, getPath(), key())
				  .read());
	}
	
	public String getPath() {
		return Paths.get(ZKArchive.REVISION_DIR, "revision-tree").toString();
	}
	
	protected Key key() {
		return config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_REVISION_TREE);
	}
	
	protected synchronized byte[] serialize() {
		int bytesNeeded = 8;
		for(RevisionTag revTag : map.keySet()) {
			bytesNeeded += 4 + (1+map.get(revTag).size())*RevisionTag.sizeForConfig(config);
		}
		
		ByteBuffer buf = ByteBuffer.allocate(bytesNeeded);
		buf.putLong(map.size());
		for(RevisionTag revTag : map.keySet()) {
			HashSet<RevisionTag> parents = map.get(revTag);
			buf.putInt(parents.size());
			buf.put(revTag.getBytes());
			for(RevisionTag parent : parents) {
				buf.put(parent.getBytes());
			}
		}
		
		assert(buf.remaining() == 0);
		return buf.array();
	}
	
	protected synchronized void deserialize(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		long numRevTags = buf.getLong();
		byte[] revTagBytes = new byte[RevisionTag.sizeForConfig(config)];
		for(int i = 0; i < numRevTags; i++) {
			int numParents = buf.getInt();
			buf.get(revTagBytes);
			RevisionTag revTag = new RevisionTag(config, revTagBytes);
			LinkedList<RevisionTag> parents = new LinkedList<>();
			
			for(int j = 0; j < numParents; j++) {
				byte[] parentTagBytes = new byte[RevisionTag.sizeForConfig(config)];
				buf.get(parentTagBytes);
				parents.add(new RevisionTag(config, parentTagBytes));
			}
			
			addParentsForTag(revTag, parents);
		}
	}
}
