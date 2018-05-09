package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.zkfs.InodeTable;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.PageMerkle;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.utility.HashCache;
import com.acrescrypto.zksync.utility.Shuffler;

/** Enqueue page requests by priority. To conserve memory, bulk requests for reftag contents or revisions or page files
 * are mapped into individual chunks on a just-in-time basis. A best-effort is made to randomize the order of chunks
 * within a given priority level, to allow greater parallelism in acquiring files from the swarm.
 */
public class PageQueue {
	private Logger logger = LoggerFactory.getLogger(PageQueue.class);
	protected static HashCache<RefTag,InodeTable> tableCache = new HashCache<RefTag,InodeTable>(8, (refTag)->refTag.readOnlyFS().getInodeTable(), (tag, table)->table.close());
	protected static HashCache<RefTag,PageMerkle> merkleCache = new HashCache<RefTag,PageMerkle>(8, (refTag)->new PageMerkle(refTag), (tag, merkle)->{});
	
	protected abstract class PageQueueEntry {
		protected int priority, step;
		protected Shuffler shuffler;
		
		public PageQueueEntry(int priority) throws IOException {
			this.priority = priority;
			int size = size();
			if(size > 0) shuffler = Shuffler.fixedShuffler(size);
		}
		
		public Object getNext() {
			Object obj = null;
			while(hasNext() && obj == null) {
				try {
					obj = element(shuffler.next());
				} catch(ENOENTException exc) {
					logger.info("Unable to queue file for transmission to peer", exc);
				} catch(IOException exc) {
					logger.error("Unable to queue file for transmission to peer", exc);
				}
			}
			
			return obj;
		}
		
		public boolean hasNext() {
			if(shuffler == null) return false;
			return shuffler.hasNext();
		}
		
		public abstract Object element(int index) throws IOException;
		public abstract int size() throws IOException;
	}
	
	protected class EverythingPageQueueEntry extends PageQueueEntry {
		ArrayList<byte[]> tags;
		int tagsRead;
		
		public EverythingPageQueueEntry(int priority) throws IOException {
			super(priority);
			ArrayList<byte[]> allTags = connection.socket.swarm.config.getArchive().allTags();
			this.tags = new ArrayList<byte[]>(allTags.size());
			for(int i = 0; i < allTags.size(); i++) {
				int j = (int) ((i+1)*Math.random());
				if(i != j) {
					this.tags.set(i, this.tags.get(j));
				}
				this.tags.set(j, allTags.get(i));
			}
		}
		
		public PageTagPageQueueEntry getNext() {
			PageTagPageQueueEntry entry = null;
			while(hasNext() && entry == null) {
				byte[] tag;
				do {
					tag = tags.get(tagsRead++);
				} while(!connection.wantsFile(tag));
				
				try {
					entry = new PageTagPageQueueEntry(priority, tag);
				} catch(ENOENTException exc) {
					logger.info("Unable to queue file for transmission to peer", exc);
				} catch(IOException exc) {
					logger.error("Unable to queue file for transmission to peer", exc);
				}
			}

			return entry;
		}
		
		public boolean hasNext() {
			return tagsRead < tags.size();
		}
		
		public void add(byte[] tag) {
			int location = (int) (Math.random() * (tags.size() - tagsRead)) + tagsRead;
			tags.add(location, tag);
		}
		
		@Override
		public Object element(int index) throws IOException {
			return null;
		}

		@Override
		public int size() {
			return 0;
		}
	}
		
	protected class RevisionPageQueueEntry extends PageQueueEntry {
		RefTag revTag;
		
		public RevisionPageQueueEntry(int priority, RefTag revTag) throws IOException {
			super(priority);
			this.revTag = revTag;
		}

		@Override
		public ReftagPageQueueEntry element(int index) throws IOException {
			RefTag tag;
			synchronized(tableCache) {
				tag = inodeTable().inodeWithId(index).getRefTag();
			}
			
			if(tag.isBlank() || tag.getRefType() == RefTag.REF_TYPE_IMMEDIATE) return null;
			return new ReftagPageQueueEntry(priority, tag);
		}

		@Override
		public int size() throws IOException {
			synchronized(tableCache) { // eviction from tableCache means closing InodeTable, so ensure we're not evicted until done
				InodeTable table = inodeTable();
				return (int) (table.getStat().getSize()/table.inodeSize());
			}
		}
		
		protected InodeTable inodeTable() throws IOException {
			synchronized(tableCache) {
				return tableCache.get(revTag);
			}
		}
	}
	
	protected class ReftagPageQueueEntry extends PageQueueEntry {
		RefTag refTag;
		
		public ReftagPageQueueEntry(int priority, RefTag refTag) throws IOException {
			super(priority);
			this.refTag = refTag;
		}

		@Override
		public PageTagPageQueueEntry element(int index) throws IOException {
			PageMerkle merkle = merkle();
			if(index < merkle.numChunks()) {
				return new PageTagPageQueueEntry(priority, merkle.tagForChunk(index));
			} else {
				return new PageTagPageQueueEntry(priority, merkle.getPageTag(index-merkle.numChunks()));
			}
		}

		@Override
		public int size() throws IOException {
			return (int) refTag.getNumPages() + merkle().numChunks();
		}
		
		protected PageMerkle merkle() throws IOException {
			return merkleCache.get(refTag);
		}
	}
	
	protected class PageTagPageQueueEntry extends PageQueueEntry {
		byte[] tag;
		
		public PageTagPageQueueEntry(int priority, byte[] prefix) throws IOException {
			super(priority);
			this.tag = prefix;
		}
		
		@Override
		public ChunkPageQueueEntry element(int index) throws IOException {
			return new ChunkPageQueueEntry(priority, path(), index);
		}
	
		@Override
		public int size() {
			return (int) Math.ceil(connection.socket.swarm.config.getPageSize()/PeerMessage.FILE_CHUNK_SIZE);
		}
		
		protected String path() throws IOException {
			return Page.pathForTag(tag);
		}
	}

	protected class ChunkPageQueueEntry extends PageQueueEntry {
		String path;
		int chunkOffset;
		
		public ChunkPageQueueEntry(int priority, String path, int chunkOffset) throws IOException {
			super(priority);
			this.path = path;
			this.chunkOffset = chunkOffset;
		}
	
		@Override
		public byte[] getNext() {
			try {
				File file = connection.socket.swarm.config.getStorage().open(path, File.O_RDONLY);
				file.seek(chunkOffset * PeerMessage.FILE_CHUNK_SIZE, File.SEEK_SET);
				return file.read(PeerMessage.FILE_CHUNK_SIZE);
			} catch(IOException exc) {
				return null;
			}
		}
	
		@Override
		public boolean hasNext() {
			return false;
		}
	
		@Override
		public int size() {
			return 0;
		}

		@Override
		public Object element(int index) throws IOException {
			return null;
		}
	}

	protected PeerConnection connection;
	protected PriorityQueue<PageQueueEntry> entries;
	protected HashMap<Integer,LinkedList<RefTag>> pages;
	protected int currentPriority;
	protected EverythingPageQueueEntry everythingEntry;
	
	public PageQueue(PeerConnection connection) {
		this.connection = connection;
		Comparator<? super PageQueueEntry> comparator = (a, b)->{
			if(a.priority == b.priority) {
				if(a.getClass().equals(b.getClass())) return 0;
				if(ReftagPageQueueEntry.class.isInstance(a)) return -1;
				return 1; // reftags go before revisions
			} else {
				return Integer.compare(b.priority, a.priority); // higher priority goes first
			}
		};
		
		entries = new PriorityQueue<PageQueueEntry>(64, comparator);
	}
	
	public void startSendingEverything() throws IOException {
		if(everythingEntry != null) return;
		everythingEntry = new EverythingPageQueueEntry(0);
	}
	
	public void stopSendingEverything() {
		entries.remove(everythingEntry);
		everythingEntry = null;
	}
	
	public void stopAll() {
		entries.clear();
		everythingEntry = null;
	}
	
	public synchronized RefTag nextPageTag() {
		return popNextPage();
	}
	
	protected synchronized RefTag popNextPage() {
		if(!entries.isEmpty() && !pages.containsKey(currentPriority)) {
			assert(entries.peek().priority == currentPriority);
			expandNextEntry();
		}
		
		LinkedList<RefTag> list = pages.get(currentPriority);
		int idx = (int) (Math.random()*list.size());
		RefTag tag = list.remove(idx);
		if(list.isEmpty()) {
			list.remove(currentPriority);
			recalculateCurrentPriority();
		}
		
		return tag;
	}
	
	protected void expandNextEntry() {
		PageQueueEntry entry = entries.peek();
		Object next = entry.getNext();
		if(next instanceof RefTag) {
			pages.putIfAbsent(entry.priority, new LinkedList<RefTag>());
			pages.get(entry.priority).add((RefTag) next);
		} else if(next instanceof ReftagPageQueueEntry) {
			entries.add(entry);
			expandNextEntry();
		} else {
			logger.error("PageQueue generated unexpected class " + next.getClass().toString());
			throw new RuntimeException("got invalid class back from page queue");
		}
		
		if(!entry.hasNext()) {
			entries.remove(entry);
		}
	}
	
	protected void recalculateCurrentPriority() {
		Integer bestPage = null, bestEntry = null;
		for(int priority : pages.keySet()) {
			if(bestPage == null || bestPage < priority) bestPage = priority;
		}
		
		if(!entries.isEmpty()) {
			bestEntry = entries.peek().priority;
		}
		
		if(bestEntry == null && bestPage == null) {
			currentPriority = 0;
		} else if(bestEntry == null) {
			currentPriority = bestPage;
		} else if(bestPage == null) {
			currentPriority = bestEntry;
		} else {
			currentPriority = Math.max(bestPage, bestEntry);
		}
	}
	
	public synchronized void addPageTag(int priority, long shortTag) throws IOException {
		byte[] pageTag = connection.socket.swarm.config.getArchive().expandShortTag(shortTag);
		addEntry(new PageTagPageQueueEntry(priority, pageTag));
	}
	
	public synchronized void addRefTagContents(int priority, RefTag refTag) throws IOException {
		addEntry(new ReftagPageQueueEntry(priority, refTag));
	}
	
	public synchronized void addRevisionTag(int priority, RefTag refTag) throws IOException {
		addEntry(new RevisionPageQueueEntry(priority, refTag));
	}
	
	protected void addEntry(PageQueueEntry entry) {
		entries.add(entry);
	}
}
