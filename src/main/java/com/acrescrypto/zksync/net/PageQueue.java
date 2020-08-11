package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.DirectoryTraverser;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.InodeTable;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.StorageTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.utility.Shuffler;
import com.acrescrypto.zksync.utility.Util;

public class PageQueue {
	public final static int DEFAULT_EVERYTHING_PRIORITY = -20;
	public final static int DEFAULT_EVERY_REVISION_STRUCTURE_PRIORITY = -10;
	public final static int CANCEL_PRIORITY = Integer.MIN_VALUE;
	
	public class ChunkReference {
		FS fs;
		StorageTag tag;
		int index;
		
		protected ChunkReference(FS fs, StorageTag tag, int index) {
			this.fs = fs;
			this.tag = tag;
			this.index = index;
		}
		
		public byte[] getData() throws IOException {
			int offset = index * PeerMessage.FILE_CHUNK_SIZE;
			int len = Math.min((int) (config.getSerializedPageSize() - offset), PeerMessage.FILE_CHUNK_SIZE);
			int timeoutMs = config.getMaster().getGlobalConfig().getInt("net.swarm.pageSendAvailabilityTimeoutMs");
			return config.readPageData(tag, offset, len, timeoutMs);
		}
	}
	
	abstract class QueueItem implements Comparable<QueueItem> {
		int priority; // higher comes first
		QueueItem lastChild;
		
		QueueItem(int priority) { this.priority = priority; }
		boolean   hasNextChild() throws IOException { return false; }
		QueueItem nextChildActual() { return null; }
		QueueItem nextChild() {
			return lastChild = nextChildActual();
		}
		
		ChunkReference reference() { return null; }
		abstract int classPriority(); // tiebreaker between equal priority; higher goes first.
		abstract long getHash();
		
		@Override
		public int compareTo(QueueItem other) {
			if(other.priority != this.priority) return -Integer.compare(priority, other.priority);
			return -Integer.compare(classPriority(), other.classPriority());
		}
		
		public void reprioritize(int newPriority) {
			if(newPriority == CANCEL_PRIORITY) {
				cancel();
				return;
			}
			
			itemsByPriority.remove(this);
			this.priority = newPriority;
			itemsByPriority.add(this);
			if(lastChild != null) {
				lastChild.reprioritize(newPriority);
			}
		}
		
		public void cancel() {
			itemsByPriority.remove(this);
			itemsByHash.remove(this.getHash());
			if(lastChild != null) {
				lastChild.cancel();
			}
		}
	}
	
	class ChunkQueueItem extends QueueItem {
		ChunkReference reference;
		
		ChunkQueueItem(int priority, ChunkReference reference) {
			super(priority);
			this.reference = reference;
		}
		
		@Override ChunkReference reference() { return reference; }
		@Override int classPriority() { return 0; }
		@Override long getHash() { return reference.tag.shortTagPreserialized() + reference.index + 1; } 
	}
	
	class PageQueueItem extends QueueItem {
		ZKArchive archive;
		StorageTag tag;
		Shuffler shuffler;
		
		PageQueueItem(int priority, ZKArchive archive, StorageTag tag) throws IOException {
			super(priority);
			this.archive = archive;
			this.tag = tag;
			
			if(tag == null || !archive.getStorage().exists(tag.path())) {
				shuffler = Shuffler.fixedShuffler(0);
			} else {
				int numChunks = (int) Math.ceil((double) archive.getConfig().getPageSize() / PeerMessage.FILE_CHUNK_SIZE);
				shuffler = Shuffler.fixedShuffler(numChunks);
			}
		}
		
		@Override
		QueueItem nextChildActual() {
			if(!shuffler.hasNext()) return null;
			return new ChunkQueueItem(priority, new ChunkReference(archive.getStorage(), tag, shuffler.next()));
		}
		
		@Override public boolean hasNextChild() { return shuffler.hasNext(); }
		@Override int classPriority() { return -10; }
		@Override long getHash() { return tag != null ? tag.shortTagPreserialized() : -1; }
	}
	
	class InodeContentsQueueItem extends QueueItem {
		PageTree tree;
		Shuffler shuffler;
		
		InodeContentsQueueItem(int priority, RevisionTag revTag, long inodeId) {
			super(priority);
			try {
				PageTree inodeTableTree = new PageTree(revTag.getRefTag());
				inodeTableTree.assertExists();
				
				try(ZKFS fs = revTag.getFS()) {
					Inode inode = fs.getInodeTable().inodeWithId(inodeId);
					if(inode.isDeleted()) throw new EINVALException("inode " + inodeId + " not issued in requested revtag");
					tree = new PageTree(inode);
					tree.assertExists();
					if(tree.numPages() > Integer.MAX_VALUE) {
						throw new EINVALException("inode contents has too many pages"); // forces abort of this request
					}
					
					int count = (int) tree.numPages();
					if(tree.getRefTag().getRefType() == RefTag.REF_TYPE_2INDIRECT) count += tree.numChunks();
					shuffler = Shuffler.fixedShuffler(count);
				}
			} catch(IOException exc) {
				shuffler = Shuffler.fixedShuffler(0);
			}
		}
		
		@Override
		QueueItem nextChildActual() {
			if(!shuffler.hasNext()) return null;
			try {
				int next = shuffler.next();
				StorageTag tag;
				if(next < tree.numPages()) {
					tag = tree.getPageTag(next);
				} else {
					tag = tree.tagForChunk(next - tree.numPages());
				}
				
				return new PageQueueItem(priority, tree.getArchive(), tag);
			} catch(IOException exc) {
				return null;
			}
		}
		
		@Override public boolean hasNextChild() { return shuffler.hasNext(); }
		@Override int classPriority() { return -20; }
		@Override long getHash() {
			return tree != null
				? tree.getRefTag().getStorageTag().shortTagPreserialized()
				: -1;
		}
	}
	
	class RevisionQueueItem extends QueueItem {
		InodeTable inodeTable;
		Shuffler shuffler;
		RevisionTag revTag;
		
		RevisionQueueItem(int priority, RevisionTag revTag) {
			super(priority);
			this.revTag = revTag;
			if(revTag.getRefTag().getRefType() == RefTag.REF_TYPE_IMMEDIATE) {
				this.shuffler = Shuffler.fixedShuffler(0);
				return;
			}
			
			try(ZKFS fs = revTag.makeCacheOnlyCopy().readOnlyFS()) {
				this.inodeTable = fs.getInodeTable();
				assert(inodeTable.nextInodeId() <= Integer.MAX_VALUE);
				this.shuffler = Shuffler.fixedShuffler((int) inodeTable.nextInodeId());
			} catch(IOException|SecurityException exc) {
				this.shuffler = Shuffler.fixedShuffler(0);
			}
		}
		
		@Override
		QueueItem nextChildActual() {
			try {
				while(inodeTable != null && shuffler.hasNext()) {
					int inodeId = shuffler.next();
					RefTag refTag = inodeTable.inodeWithId(inodeId).getRefTag();
					if(refTag.getRefType() != RefTag.REF_TYPE_IMMEDIATE) {
						return new InodeContentsQueueItem(priority, revTag, inodeId);
					}
				}
				
				return null;
			} catch (IOException exc) {
				logger.error("Caught exception queuing revision tag {}", revTag, exc);
				if(exc.getCause() != null) {
					exc.getCause().printStackTrace();
				}
				return null;
			}
		}
		
		@Override public boolean hasNextChild() { return inodeTable != null && shuffler.hasNext(); }
		@Override int classPriority() { return -40; }
		@Override long getHash() { return Util.shortTag(revTag.getBytes()); }
	}
	
	/* send pages for the inode table and each directory in a revision */
	class RevisionStructureQueueItem extends QueueItem {
		InodeTable inodeTable;
		Shuffler shuffler;
		RevisionTag revTag;
		
		RevisionStructureQueueItem(int priority, RevisionTag revTag) {
			super(priority);
			this.revTag = revTag;
			if(revTag.getRefTag().getRefType() == RefTag.REF_TYPE_IMMEDIATE) {
				this.shuffler = Shuffler.fixedShuffler(0);
				return;
			}
			
			try(ZKFS fs = revTag.makeCacheOnlyCopy().readOnlyFS()) {
				this.inodeTable = fs.getInodeTable();
				assert(inodeTable.nextInodeId() <= Integer.MAX_VALUE);
				this.shuffler = Shuffler.fixedShuffler((int) inodeTable.nextInodeId());
			} catch(IOException|SecurityException exc) {
				this.shuffler = Shuffler.fixedShuffler(0);
			}
		}
		
		@Override
		QueueItem nextChildActual() {
			try {
				while(inodeTable != null && shuffler.hasNext()) {
					int inodeId = shuffler.next();
					Inode inode = inodeTable.inodeWithId(inodeId);
					if(inodeId != InodeTable.INODE_ID_INODE_TABLE && !inode.getStat().isDirectory()) {
						continue;
					}
					
					RefTag refTag = inodeTable.inodeWithId(inodeId).getRefTag();
					if(refTag.getRefType() != RefTag.REF_TYPE_IMMEDIATE) {
						return new InodeContentsQueueItem(priority, revTag, inodeId);
					}
				}
				
				return null;
			} catch (IOException exc) {
				logger.error("Caught exception queuing revision tag {}", revTag, exc);
				return null;
			}
		}
		
		@Override public boolean hasNextChild() { return inodeTable != null && shuffler.hasNext(); }
		@Override int classPriority() { return -30; }
		@Override long getHash() { return Util.shortTag(revTag.getBytes()); }
	}
	
	class EverythingQueueItem extends QueueItem {
		DirectoryTraverser traverser;
		ZKArchive archive;
		boolean done;
		
		EverythingQueueItem(int priority, ZKArchive archive) {
			super(priority);
			this.archive = archive;
			Directory dir = null;
			try {
				dir = this.archive.getStorage().opendir("/");
				traverser = new DirectoryTraverser(this.archive.getStorage(),
						this.archive.getStorage().opendir("/"));
			} catch(IOException exc) {
				logger.error("Caught exception establishing EverythingQueueItem", exc);
			} finally {
				if(dir != null) {
					try {
						dir.close();
					} catch (IOException exc2) {
						logger.error("Caught exception closing directory {}", dir.getPath(), exc2);
					}
				}
			}
		}
		
		@Override
		QueueItem nextChildActual() {
			try {
				if(traverser == null || !traverser.hasNext()) {
					done = true;
					return null;
				}

				String path;
				path = traverser.next().getPath();
				logger.trace("Enqueuing path {}", path);
				StorageTag tag = new StorageTag(archive.getCrypto(), path);
				return new PageQueueItem(priority, archive, tag);
			} catch (IOException exc) {
				logger.error("Caught exception queuing next tag in EverythingQueueItem", exc);
				return null;
			}
		}
		
		@Override public boolean hasNextChild() throws IOException { return traverser != null && traverser.hasNext(); }
		@Override int classPriority() { return -100; }
		@Override long getHash() { return 0; }
	}
	
	private Logger logger = LoggerFactory.getLogger(PageQueue.class);
	protected PriorityQueue<QueueItem> itemsByPriority = new PriorityQueue<QueueItem>();
	protected Map<Long,QueueItem> itemsByHash = new HashMap<>();
	protected ZKArchiveConfig config;
	protected EverythingQueueItem everythingItem;
	protected boolean closed;
	
	public PageQueue(ZKArchiveConfig config) {
		this.config = config;
	}
	
	public void addChunkReference(int priority, ChunkReference reference) {
		addItem(new ChunkQueueItem(priority, reference));
	}
	
	public void addPageTag(int priority, long shortTag) {
		try {
			StorageTag tag = config.getArchive().expandShortTag(shortTag);
			if(tag == null) {
				logger.warn("Cannot enqueue non-existent short tag {}", shortTag);
				return;
			}
			
			addPageTag(priority, tag);
		} catch (Exception exc) {
			logger.warn("Caught exception queuing short tag {}", String.format("%016x", shortTag), exc);
		}
	}
	
	public void addPageTag(int priority, StorageTag pageTag) throws IOException {
		logger.debug("Enqueuing page tag {}", pageTag);
		addItem(new PageQueueItem(priority, config.getArchive(), pageTag));
	}
	
	public void addInodeContents(int priority, RevisionTag revTag, long inodeId) {
		logger.debug("Enqueuing inode {} of {}", inodeId, Util.formatRevisionTag(revTag));
		addItem(new InodeContentsQueueItem(priority, revTag, inodeId));
	}
	
	public void addRevisionTag(int priority, RevisionTag revTag) {
		logger.debug("Enqueuing {}", Util.formatRevisionTag(revTag));
		addItem(new RevisionQueueItem(priority, revTag));
	}
	
	public void addRevisionTagForStructure(int priority, RevisionTag revTag) {
		logger.debug("Enqueuing {} (structure only)", Util.formatRevisionTag(revTag));
		addItem(new RevisionStructureQueueItem(priority, revTag));
	}
	
	public synchronized void startSendingEverything() {
		if(everythingItem != null && !everythingItem.done) return;
		everythingItem = new EverythingQueueItem(DEFAULT_EVERYTHING_PRIORITY, config.getArchive());
		addItem(everythingItem);
	}
	
	public synchronized void stopSendingEverything() {
		if(everythingItem == null) return;
		everythingItem.cancel();
	}
	
	public synchronized void stopAll() {
		itemsByPriority.clear();
		itemsByHash.clear();
	}
	
	public synchronized void close() {
		closed = true;
		this.notifyAll();
	}
	
	public boolean hasNextChunk() throws IOException {
		unpackNextReference();
		return !itemsByPriority.isEmpty();
	}
	
	public boolean expectTagNext(StorageTag tag) throws IOException {
		unpackNextReference();
		if(itemsByPriority.isEmpty()) return false;
		QueueItem head = itemsByPriority.peek();
		if(head == null) return false;
		try {
			return head.reference() != null && tag.equals(head.reference().tag);
		} catch(NullPointerException exc) {
			System.out.println("wtf");
			exc.printStackTrace();
			throw exc;
		}
	}
	
	public synchronized ChunkReference nextChunk() throws IOException {
		while(!hasNextChunk() && !closed) {
			try {
				this.wait();
			} catch(InterruptedException exc) {}
		}
		
		if(closed) return null;
		
		QueueItem item = itemsByPriority.remove();
		itemsByHash.remove(item.getHash());
		return item.reference();
	}
	
	protected synchronized void addItem(QueueItem item) {
		QueueItem existing = itemsByHash.get(item.getHash());
		if(existing != null) {
			existing.reprioritize(item.priority);
			return;
		}
		
		itemsByPriority.add(item);
		itemsByHash.put(item.getHash(), item);
		this.notifyAll();
	}
	
	protected synchronized void unpackNextReference() throws IOException {
		unpackToDepth(1);
	}
	
	protected synchronized void unpackToDepth(int depth) throws IOException {
	    // ensure that the first `depth` elements of the queue are ChunkQueueItems
	    while(!unpackedToDepth(depth)) {
	        unpackFirstPackedReference();
	    }
	}
	
	protected synchronized boolean unpackedToDepth(int depth) throws IOException {
	    int i = 0;
	    
	    for(QueueItem item : itemsByPriority) {
	        if(i >= depth)               return true;
	        if(item.hasNextChild())      return false;
	        if(item.reference() == null) return false;
	        i++;
	    }
	    
	    // we have fewer than 'depth' items (potentially none), but nothing is packed
	    return true;
	}
	
	protected synchronized void unpackFirstPackedReference() throws IOException {
	    QueueItem retiredItem = null;
	    
	    for(QueueItem item : itemsByPriority) {
	        QueueItem child = item.nextChild();
	        if(child == null) {
	            // nothing to unpack
	            
	            if(item.reference() != null) continue; // has data, so keep it
	            
	            // remove this from the queue, then cycle back through
	            retiredItem = item;
	            break;
	        }
	        
	        // add the child in
            itemsByPriority.add(child);
            itemsByHash    .put(child.getHash(), child);
            
            break;
	    }
	    
        if(    retiredItem != null
           &&  retiredItem.reference() == null
           && !retiredItem.hasNextChild())
        {
	        itemsByPriority.remove(retiredItem);
            itemsByHash.remove(retiredItem.getHash());
        }
	}
}
