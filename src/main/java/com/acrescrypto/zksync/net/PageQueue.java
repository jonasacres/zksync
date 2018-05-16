package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.fs.DirectoryTraverser;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.InodeTable;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.PageMerkle;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.utility.Shuffler;

public class PageQueue {
	public final static int DEFAULT_EVERYTHING_PRIORITY = -10;
	
	public class ChunkReference {
		FS fs;
		byte[] tag;
		int index;
		
		protected ChunkReference(FS fs, byte[] tag, int index) {
			this.fs = fs;
			this.tag = tag;
			this.index = index;
		}
		
		public byte[] getData() {
			return null;
		}
	}
	
	abstract class QueueItem implements Comparable<QueueItem> {
		int priority;
		QueueItem(int priority) { this.priority = priority; }
		QueueItem nextChild() { return null; }
		ChunkReference reference() { return null; }
		abstract int classPriority();
		
		@Override
		public int compareTo(QueueItem other) {
			if(other.priority != this.priority) return Integer.compare(priority, other.priority);
			return Integer.compare(classPriority(), other.classPriority());
		}
	}
	
	class ChunkQueueItem extends QueueItem {
		ChunkReference reference;
		
		ChunkQueueItem(int priority, ChunkReference reference) {
			super(priority);
			this.reference = reference;
		}
		
		@Override
		ChunkReference reference() {
			return reference;
		}
		
		@Override
		int classPriority() { return 0; }
	}
	
	class PageQueueItem extends QueueItem {
		ZKArchive archive;
		byte[] tag;
		Shuffler shuffler;
		
		PageQueueItem(int priority, ZKArchive archive, long shortTag) {
			this(priority, archive, archive.expandShortTag(shortTag));
		}
		
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
		QueueItem nextChild() {
			if(!shuffler.hasNext()) return null;
			return new ChunkQueueItem(priority, new ChunkReference(archive.getStorage(), tag, shuffler.next()));
		}
		
		@Override
		int classPriority() { return -10; }
	}
	
	class RefTagContentsQueueItem extends QueueItem {
		PageMerkle merkle;
		Shuffler shuffler;
		
		RefTagContentsQueueItem(int priority, RefTag refTag) {
			super(priority);
			if(refTag.getRefType() == RefTag.REF_TYPE_IMMEDIATE) {
				shuffler = Shuffler.fixedShuffler(0);
			} else {
				try {
					this.merkle = new PageMerkle(refTag);
					shuffler = Shuffler.fixedShuffler(merkle.numPages());
				} catch(IOException exc) {
					shuffler = Shuffler.fixedShuffler(0);
				}
			}
		}
		
		@Override
		QueueItem nextChild() {
			if(!shuffler.hasNext()) return null;
			return new PageQueueItem(priority, merkle.getArchive(), merkle.getPageTag(shuffler.next()));
		}
		
		@Override
		int classPriority() { return -20; }
	}
	
	class RevisionQueueItem extends QueueItem {
		InodeTable inodeTable;
		Shuffler shuffler;
		RefTag revTag;
		
		RevisionQueueItem(int priority, RefTag revTag) {
			super(priority);
			this.revTag = revTag;
			try {
				this.inodeTable = revTag.getArchive().openRevision(revTag).getInodeTable();
				assert(inodeTable.nextInodeId <= Integer.MAX_VALUE);
				this.shuffler = Shuffler.fixedShuffler((int) inodeTable.nextInodeId);
			} catch(IOException exc) {
				this.shuffler = Shuffler.fixedShuffler(0);
			}
		}
		
		@Override
		QueueItem nextChild() {
			try {
				RefTag refTag;			

				do {
					if(!shuffler.hasNext()) return null;
					int inodeId = shuffler.next();
					refTag = inodeTable.inodeWithId(inodeId).getRefTag();
				} while(refTag.getRefType() == RefTag.REF_TYPE_IMMEDIATE);
				
				return new RefTagContentsQueueItem(priority, refTag);
			} catch (IOException exc) {
				logger.error("Caught exception queuing revision tag {}", revTag, exc);
				return null;
			}
		}
		
		@Override
		int classPriority() { return -30; }
	}
	
	class EverythingQueueItem extends QueueItem {
		DirectoryTraverser traverser;
		ZKArchive archive;
		
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
		QueueItem nextChild() {
			if(traverser == null || !traverser.hasNext()) return null;
			try {
				String path = traverser.next();
				return new PageQueueItem(priority, archive, Page.tagForPath(path));
			} catch (IOException exc) {
				logger.error("Caught exception queuing next tag in EverythingQueueItem", exc);
				return null;
			}
		}
		
		@Override
		int classPriority() { return -40; }
	}
	
	private Logger logger = LoggerFactory.getLogger(PageQueue.class);
	protected PriorityQueue<QueueItem> itemsByPriority = new PriorityQueue<QueueItem>();
	protected ZKArchive archive;
	
	public PageQueue(ZKArchive archive) {
		archive = this.archive;
	}
	
	public void addPageTag(int priority, long shortTag) {
		addItem(new PageQueueItem(priority, archive, shortTag));
	}
	
	public void addRefTagContents(int priority, RefTag refTag) {
		addItem(new RefTagContentsQueueItem(priority, refTag));
	}
	
	public void addRevisionTag(int priority, RefTag revTag) {
		addItem(new RevisionQueueItem(priority, revTag));
	}
	
	public void startSendingEverything() {
		addItem(new EverythingQueueItem(DEFAULT_EVERYTHING_PRIORITY, archive));
	}
	
	public void stopSendingEverything() {
	}
	
	public synchronized void stopAll() {
		itemsByPriority.clear();
	}
	
	public boolean hasNextChunk() {
		return !itemsByPriority.isEmpty();
	}
	
	public synchronized ChunkReference nextChunk() {
		while(!hasNextChunk()) {
			try {
				this.wait();
			} catch(InterruptedException exc) {}
		}

		QueueItem head, child;
		do {
			head = itemsByPriority.peek();
			child = head.nextChild();
			if(child != null) {
				itemsByPriority.add(child);
			}
		} while(child != null);
		
		itemsByPriority.poll();
		return head.reference();
	}
	
	protected synchronized void addItem(QueueItem item) {
		itemsByPriority.add(item);
		this.notifyAll();
	}
}
