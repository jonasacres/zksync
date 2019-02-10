package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.fs.DirectoryTraverser;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.InodeTable;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.utility.Shuffler;
import com.acrescrypto.zksync.utility.Util;

public class PageQueue {
	public final static int DEFAULT_EVERYTHING_PRIORITY = -10;
	public final static int CANCEL_PRIORITY = Integer.MIN_VALUE;
	
	public class ChunkReference {
		FS fs;
		byte[] tag;
		int index;
		
		protected ChunkReference(FS fs, byte[] tag, int index) {
			this.fs = fs;
			this.tag = tag;
			this.index = index;
		}
		
		public byte[] getData() throws IOException {
			try(File file = fs.open(Page.pathForTag(tag), File.O_RDONLY)) {
				long offset = index * PeerMessage.FILE_CHUNK_SIZE;
				int len = Math.min((int) (file.getStat().getSize() - offset), PeerMessage.FILE_CHUNK_SIZE);
				if(len < 0) throw new RuntimeException("attempted to read offset " + offset + " beyond end of file for page " + Util.bytesToHex(tag));
				
				byte[] data = new byte[len];
				file.seek(offset, File.SEEK_SET);
				file.read(data, 0, len);
				return data;
			}
		}
	}
	
	abstract class QueueItem implements Comparable<QueueItem> {
		int priority; // higher comes first
		QueueItem lastChild;
		
		QueueItem(int priority) { this.priority = priority; }
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
		@Override long getHash() { return Util.shortTag(reference.tag) + reference.index + 1; } 
	}
	
	class PageQueueItem extends QueueItem {
		ZKArchive archive;
		byte[] tag;
		Shuffler shuffler;
		
		PageQueueItem(int priority, ZKArchive archive, byte[] tag) {
			super(priority);
			this.archive = archive;
			this.tag = tag;
			
			if(tag == null || !archive.getStorage().exists(Page.pathForTag(tag))) {
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
		
		@Override int classPriority() { return -10; }
		@Override long getHash() { return tag != null ? Util.shortTag(tag) : -1; }
	}
	
	class InodeContentsQueueItem extends QueueItem {
		PageTree tree;
		Shuffler shuffler;
		
		InodeContentsQueueItem(int priority, RevisionTag revTag, long inodeId) {
			super(priority);
			try {
				PageTree inodeTableTree = new PageTree(revTag.getRefTag());
				inodeTableTree.assertExists();
				
				Inode inode = revTag.getFS().getInodeTable().inodeWithId(inodeId);
				if(inode.isDeleted()) throw new EINVALException("inode " + inodeId + " not issued in requested revtag");
				tree = new PageTree(inode);
				tree.assertExists();
				if(tree.numPages() > Integer.MAX_VALUE) {
					throw new EINVALException("inode contents has too many pages"); // forces abort of this request
				}
				
				int count = (int) tree.numPages();
				if(tree.getRefTag().getRefType() == RefTag.REF_TYPE_2INDIRECT) count += tree.numChunks();
				shuffler = Shuffler.fixedShuffler(count);
			} catch(IOException exc) {
				shuffler = Shuffler.fixedShuffler(0);
			}
		}
		
		@Override
		QueueItem nextChildActual() {
			if(!shuffler.hasNext()) return null;
			try {
				int next = shuffler.next();
				byte[] tag;
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
		
		@Override int classPriority() { return -20; }
		@Override long getHash() { return tree != null ? Util.shortTag(tree.getRefTag().getHash()) : -1; }
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
			
			try {
				this.inodeTable = revTag.makeCacheOnly().readOnlyFS().getInodeTable();
				assert(inodeTable.nextInodeId() <= Integer.MAX_VALUE);
				this.shuffler = Shuffler.fixedShuffler((int) inodeTable.nextInodeId());
			} catch(IOException|SecurityException exc) {
				this.shuffler = Shuffler.fixedShuffler(0);
			}
		}
		
		@Override
		QueueItem nextChildActual() {
			try {
				while(shuffler.hasNext()) {
					int inodeId = shuffler.next();
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
			try {
				traverser = new DirectoryTraverser(this.archive.getStorage(), this.archive.getStorage().opendir("/"));
			} catch(IOException exc) {
				logger.error("Caught exception establishing EverythingQueueItem", exc);
			}
		}
		
		@Override
		QueueItem nextChildActual() {
			if(traverser == null || !traverser.hasNext()) {
				done = true;
				return null;
			}
			
			try {
				String path = traverser.next();
				return new PageQueueItem(priority, archive, Page.tagForPath(path));
			} catch (IOException exc) {
				logger.error("Caught exception queuing next tag in EverythingQueueItem", exc);
				return null;
			}
		}
		
		@Override int classPriority() { return -40; }
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
			addPageTag(priority, config.getArchive().expandShortTag(shortTag));
		} catch (IOException exc) {
			logger.error("Caught exception queuing short tag {}", String.format("%16x", shortTag), exc);
		}
	}
	
	public void addPageTag(int priority, byte[] pageTag) {
		addItem(new PageQueueItem(priority, config.getArchive(), pageTag));
	}
	
	public void addInodeContents(int priority, RevisionTag revTag, long inodeId) {
		addItem(new InodeContentsQueueItem(priority, revTag, inodeId));
	}
	
	public void addRevisionTag(int priority, RevisionTag revTag) {
		addItem(new RevisionQueueItem(priority, revTag));
	}
	
	public void startSendingEverything() {
		if(everythingItem != null && !everythingItem.done) return;
		everythingItem = new EverythingQueueItem(DEFAULT_EVERYTHING_PRIORITY, config.getArchive());
		addItem(everythingItem);
	}
	
	public void stopSendingEverything() {
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
	
	public boolean hasNextChunk() {
		unpackNextReference();
		return !itemsByPriority.isEmpty();
	}
	
	public boolean expectTagNext(byte[] tag) {
		unpackNextReference();
		if(itemsByPriority.isEmpty()) return false;
		QueueItem head = itemsByPriority.peek();
		return head.reference() != null && Arrays.equals(tag, head.reference().tag);
	}
	
	public synchronized ChunkReference nextChunk() {
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
	
	protected synchronized void unpackNextReference() {
		while(!itemsByPriority.isEmpty()) {
			QueueItem head = itemsByPriority.peek();
			QueueItem child = head.nextChild();
			if(child != null) {
				itemsByPriority.add(child);
				itemsByHash.put(child.getHash(), child);
			} else if(head.reference() == null) {
				QueueItem item = itemsByPriority.poll();
				itemsByHash.remove(item.getHash());
			} else {
				return;
			}
		}
	}
}
