package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.net.PeerConnection.PeerCapabilityException;
import com.acrescrypto.zksync.utility.Util;

public class RequestPool {
	public final static int DEFAULT_PRUNE_INTERVAL_MS = 30*1000;
	public static int pruneIntervalMs = DEFAULT_PRUNE_INTERVAL_MS;
	
	protected boolean dirty;
	protected ZKArchiveConfig config;
	protected HashList<InodeRef> requestedInodes = new HashList<InodeRef>(
			(r)->r.getRevTag().getShortHash() + r.getInodeId() + 1,
			(r)->{
				ByteBuffer buf = ByteBuffer.allocate(r.getRevTag().getBytes().length + 8);
				buf.put(r.getRevTag().getBytes());
				buf.putLong(r.getInodeId());
				return buf.array();
			},
			(buf)->{
				RevisionTag revTag = new RevisionTag(config, buf, false);
				long inodeId = buf.getLong();
				return new InodeRef(revTag, inodeId);
			});
	protected HashList<RevisionTag> requestedRevisions = new HashList<RevisionTag>(
			(t)->Util.shortTag(t.getBytes()),
			(t)->t.getBytes(),
			(buf)->new RevisionTag(config, buf, false));
	protected HashList<RevisionTag> requestedRevisionDetails = new HashList<RevisionTag>(
			(t)->Util.shortTag(t.getBytes()),
			(t)->t.getBytes(),
			(buf)->new RevisionTag(config, buf, false));
	protected HashList<Long> requestedPageTags = new HashList<Long>(
			(t)->t.longValue(),
			(t)->Util.serializeLong(t.longValue()),
			(buf)->buf.getLong());
	
	interface Hasher<T> { public long hash(T item); }
	interface Remover<T> { public boolean test(T item); }
	interface Serializer<T> { public byte[] serialize(T item); }
	interface Deserializer<T> { public T deserialize(ByteBuffer serialization); }
	
	public class InodeRef {
		private RevisionTag revTag;
		private long inodeId;
		
		public InodeRef(RevisionTag revTag, long inodeId) {
			this.setRevTag(revTag);
			this.setInodeId(inodeId);
		}
		
		// getters and setters for JSON serialization
		public long getInodeId() {
			return inodeId;
		}

		public void setInodeId(long inodeId) {
			this.inodeId = inodeId;
		}

		public RevisionTag getRevTag() {
			return revTag;
		}

		public void setRevTag(RevisionTag revTag) {
			this.revTag = revTag;
		}
	}
	
	class HashListEntry<T> {
		int priority;
		T item;
		
		public HashListEntry(int priority, T item) {
			this.item = item;
			this.priority = priority;
		}
	}
	
	public class HashList<T> implements Iterable<HashListEntry<T>> {
		LinkedList<HashListEntry<T>> list = new LinkedList<>();
		HashMap<Long, HashListEntry<T>> map = new HashMap<>();
		Hasher<T> hasher;
		Serializer<T> serializer;
		Deserializer<T> deserializer;
		
		public HashList(Hasher<T> hasher, Serializer<T> serializer, Deserializer<T> deserializer) {
			this.hasher = hasher;
			this.serializer = serializer;
			this.deserializer = deserializer;
		}
		
		public byte[] serialize() {
			int recordSize = 0;
			if(!list.isEmpty()) {
				recordSize = serializer.serialize(list.getFirst().item).length;
			}
			
			ByteBuffer buf = ByteBuffer.allocate(4 + list.size()*(4 + recordSize));
			buf.putInt(list.size());
			for(HashListEntry<T> entry : list) {
				buf.putInt(entry.priority);
				buf.put(serializer.serialize(entry.item));
			}
			
			return buf.array();
		}
		
		public void deserialize(ByteBuffer buf) {
			list.clear();
			map.clear();
			int numRecords = buf.getInt();
			for(int i = 0; i < numRecords; i++) {
				int priority = buf.getInt();
				T item = deserializer.deserialize(buf);
				HashListEntry<T> entry = new HashListEntry<T>(priority, item);
				list.add(entry);
				map.put(getHash(item), entry);
			}
		}
		
		public boolean add(int priority, T item) {
			long hash = getHash(item);
			HashListEntry<T> entry = map.get(hash);
			if(entry != null) {
				if(entry.priority == priority) return false;
				entry.priority = priority;
				return true;
			}
			
			entry = new HashListEntry<T>(priority, item);
			map.put(hash, entry);
			list.add(entry);
			return true;
		}
		
		public void remove(T entry) {
			long hash = getHash(entry);
			list.remove(entry);
			map.remove(hash);
		}
		
		public long getHash(T item) {
			return hasher.hash(item);
		}
		
		public int size() {
			return list.size();
		}
		
		public HashMap<Integer,LinkedList<T>> priorityMap() {
			HashMap<Integer,LinkedList<T>> pMap = new HashMap<>();
			for(HashListEntry<T> entry : list) {
				pMap.putIfAbsent(entry.priority, new LinkedList<>());
				pMap.get(entry.priority).add(entry.item);
			}
			return pMap;
		}

		@Override
		public Iterator<HashListEntry<T>> iterator() {
			return list.iterator();
		}

		public boolean contains(int priority, T item) {
			return map.containsKey(getHash(item)) && map.get(getHash(item)).priority == priority;
		}

		public void removeIf(Remover<T> test) {
			LinkedList<HashListEntry<T>> toRemove = new LinkedList<>();
			for(HashListEntry<T> entry : list) {
				if(test.test(entry.item)) {
					toRemove.add(entry);
				}
			}
			
			for(HashListEntry<T> entry : toRemove) {
				list.remove(entry);
				map.remove(getHash(entry.item));
			}
		}

		public boolean isEmpty() {
			return list.isEmpty();
		}

		public HashListEntry<T> lookup(T item) {
			return map.get(getHash(item));
		}
	}
	
	boolean requestingEverything, stopped, paused;
	Logger logger = LoggerFactory.getLogger(RequestPool.class);
	
	protected RequestPool() {}
	
	public RequestPool(ZKArchiveConfig config) {
		this.config = config;
		
		new Thread(config.getThreadGroup(), ()->pruneThread()).start();
	}
	
	public synchronized void stop() {
		stopped = true;
		this.notifyAll();
	}
	
	public void setRequestingEverything(boolean requestingEverything) {
		this.requestingEverything = requestingEverything;
		dirty = true;
		
		for(PeerConnection connection : config.getSwarm().getConnections()) {
			connection.requestAll();
		}
	}
	
	public void setPaused(boolean paused) {
		this.paused = paused;
	}
	
	public synchronized void addRequestsToConnection(PeerConnection conn) {
		conn.setPaused(paused);
		
		addDataRequestsToConnection(conn);
	}
	
	public synchronized void addInode(int priority, RevisionTag revTag, long inodeId) {
		requestedInodes.add(priority, new InodeRef(revTag, inodeId));
		dirty = true;

		for(PeerConnection connection : config.getSwarm().getConnections()) {
			ArrayList<Long> list = new ArrayList<>(1);
			list.add(inodeId);
			try {
				connection.requestInodes(priority, revTag, list);
			} catch (PeerCapabilityException e) {}
		}
	}
	
	public synchronized void cancelInode(RevisionTag revTag, long inodeId) {
		requestedInodes.remove(new InodeRef(revTag, inodeId));
		for(PeerConnection connection : config.getSwarm().getConnections()) {
			ArrayList<Long> list = new ArrayList<>(1);
			list.add(inodeId);
			try {
				connection.requestInodes(PageQueue.CANCEL_PRIORITY, revTag, list);
			} catch (PeerCapabilityException e) {}
		}
	}
	
	public int priorityForInode(RevisionTag revTag, long inodeId) {
		try {
			return requestedInodes.lookup(new InodeRef(revTag, inodeId)).priority;
		} catch(NullPointerException exc) {
			return PageQueue.CANCEL_PRIORITY;
		}
	}

	
	public synchronized void addRevision(int priority, RevisionTag revTag) {
		requestedRevisions.add(priority,  revTag);
		dirty = true;
		
		for(PeerConnection connection : config.getSwarm().getConnections()) {
			ArrayList<RevisionTag> list = new ArrayList<>(1);
			list.add(revTag);
			try {
				connection.requestRevisionContents(priority, list);
			} catch(PeerCapabilityException exc) {}
		}
	}
	
	public synchronized void cancelRevision(RevisionTag revTag) {
		requestedRevisions.remove(revTag);
		dirty = true;
		
		for(PeerConnection connection : config.getSwarm().getConnections()) {
			ArrayList<RevisionTag> list = new ArrayList<>(1);
			list.add(revTag);
			try {
				connection.requestRevisionContents(PageQueue.CANCEL_PRIORITY, list);
			} catch(PeerCapabilityException exc) {}
		}
	}
	
	public int priorityForRevision(RevisionTag revTag) {
		try {
			return requestedRevisions.lookup(revTag).priority;
		} catch(NullPointerException exc) {
			return PageQueue.CANCEL_PRIORITY;
		}
	}
	
	public synchronized void addPageTag(int priority, long shortTag) {
		requestedPageTags.add(priority, shortTag);
		dirty = true;
		
		for(PeerConnection connection : config.getSwarm().getConnections()) {
			connection.requestPageTag(priority, shortTag);
		}
	}
	
	public synchronized void cancelPageTag(long shortTag) {
		requestedPageTags.remove(shortTag);
		dirty = true;
		
		for(PeerConnection connection : config.getSwarm().getConnections()) {
			connection.requestPageTag(PageQueue.CANCEL_PRIORITY, shortTag);
		}
	}
	
	public int priorityForPageTag(long shortTag) {
		try {
			return requestedPageTags.lookup(shortTag).priority;
		} catch(NullPointerException exc) {
			return PageQueue.CANCEL_PRIORITY;
		}
	}
	
	public synchronized void addPageTag(int priority, byte[] pageTag) {
		addPageTag(priority, Util.shortTag(pageTag));
	}
	
	public synchronized void cancelPageTag(byte[] pageTag) {
		cancelPageTag(Util.shortTag(pageTag));
	}
	
	public int priorityForPageTag(byte[] pageTag) {
		try {
			return requestedPageTags.lookup(Util.shortTag(pageTag)).priority;
		} catch(NullPointerException exc) {
			return PageQueue.CANCEL_PRIORITY;
		}
	}
	
	public synchronized void addRevisionDetails(int priority, RevisionTag revTag) {
		requestedRevisionDetails.add(priority, revTag);
		
		for(PeerConnection connection : config.getSwarm().getConnections()) {
			try {
				connection.requestRevisionDetails(priority, revTag);
			} catch(PeerCapabilityException exc) {}
		}
	}
	
	public int priorityForRevisionDetails(RevisionTag revTag) {
		try {
			return requestedRevisionDetails.lookup(revTag).priority;
		} catch(NullPointerException exc) {
			return PageQueue.CANCEL_PRIORITY;
		}
	}
	
	public boolean hasPageTag(int priority, long shortTag) {
		return requestedPageTags.contains(priority, shortTag);
	}
	
	public boolean hasInode(int priority, RevisionTag refTag, long inodeId) {
		return requestedInodes.contains(priority, new InodeRef(refTag, inodeId));
	}
	
	public boolean hasRevision(int priority, RevisionTag revTag) {
		return requestedRevisions.contains(priority, revTag);
	}
	
	public boolean hasRevisionDetails(int priority, RevisionTag revTag) {
		return requestedRevisionDetails.contains(priority, revTag);
	}

	
	public synchronized void prune() {
		if(config.getArchive() == null) return; // archive not initialized yet yet
		try {
			prunePageTags();
			pruneRefTags();
			pruneRevisionTags();
			pruneRevisionDetails();
		} catch(IOException exc) {
			logger.error("Caught exception pruning request pool", exc);
		}
	}
	
	protected void addDataRequests() {
		// TODO API: (coverage) branch coverage
		if(config.getSwarm() != null) {
			for(PeerConnection conn : config.getSwarm().getConnections()) {
				addDataRequestsToConnection(conn);
			}
		}
	}
	
	protected void addDataRequestsToConnection(PeerConnection conn) {
		if(requestingEverything) {
			conn.requestAll();
		}
		
		HashMap<Integer,LinkedList<Long>> pageMap = requestedPageTags.priorityMap();
		for(int priority : pageMap.keySet()) {
			conn.requestPageTags(priority, pageMap.get(priority));
		}
		
		try {
			HashMap<Integer, HashMap<RevisionTag, LinkedList<Long>>> inodeMap = inodeMap();
			for(int priority : inodeMap.keySet()) {
				for(RevisionTag revTag : inodeMap.get(priority).keySet()) {
					conn.requestInodes(priority, revTag, inodeMap.get(priority).get(revTag));
				}
			}
			
			HashMap<Integer,LinkedList<RevisionTag>> map = requestedRevisions.priorityMap();
			for(int priority : map.keySet()) {
				conn.requestRevisionContents(priority, map.get(priority));
			}
			
			map = requestedRevisionDetails.priorityMap();
			for(int priority : map.keySet()) {
				conn.requestRevisionDetails(priority, map.get(priority));
			}
		} catch(PeerCapabilityException exc) {}
	}
	
	protected HashMap<Integer, HashMap<RevisionTag, LinkedList<Long>>> inodeMap() {
		HashMap<Integer, HashMap<RevisionTag, LinkedList<Long>>> map = new HashMap<>();
		for(HashListEntry<InodeRef> entry : requestedInodes) {
			map.putIfAbsent(entry.priority, new HashMap<RevisionTag, LinkedList<Long>>());
			HashMap<RevisionTag, LinkedList<Long>> tagMap = map.get(entry.priority);
			tagMap.putIfAbsent(entry.item.getRevTag(), new LinkedList<Long>());
			tagMap.get(entry.item.getRevTag()).add(entry.item.getInodeId());
		}
		
		return map;
	}
	
	protected void pruneThread() {
		Util.setThreadName("RequestPool prune thread");
		while(!stopped) {
			try {
				synchronized(this) { this.wait(pruneIntervalMs); }
				if(stopped) break;
				prune();
				if(dirty) {
					try {
						write();
					} catch(IOException exc) {
						if(!stopped) {
							logger.error("Caught exception writing request pool file", exc);
						}
					}
				}
			} catch(Exception exc) {
				logger.error("Prune thread caught exception", exc);
			}
		}
	}
	
	protected void prunePageTags() throws IOException {
		requestedPageTags.removeIf((shortTag)->{
			try {
				return config.getArchive().expandShortTag(shortTag) != null;
			} catch(IOException exc) {
				return false;
			}
		});
	}
	
	protected void pruneRefTags() throws IOException {
		requestedInodes.removeIf((r)->{
			try {
				return config.getArchive().hasInode(r.getRevTag(), r.getInodeId());
			} catch (IOException e) {
				return false;
			}
		});
	}

	protected void pruneRevisionTags() throws IOException {
		requestedRevisions.removeIf((revTag)->{
			try {
				return config.getArchive().hasRevision(revTag);
			} catch(IOException exc) {
				return false;
			}
		});
	}
	
	protected void pruneRevisionDetails() throws IOException {
		requestedRevisionDetails.removeIf((revTag)->config.getRevisionTree().hasParentsForTag(revTag));
	}
	
	protected Key key() {
		return config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, "easysafe-request-pool-key");
	}
	
	protected String path() {
		return "request-pool";
	}
	
	protected void write() throws IOException {
		MutableSecureFile
		  .atPath(config.getLocalStorage(), path(), key())
		  .write(serialize(), 1024*512);
		dirty = false;
	}
	
	protected void read() throws IOException {
		try {
			byte[] serialized = MutableSecureFile
			  .atPath(config.getLocalStorage(), path(), key())
			  .read();
		
			deserialize(ByteBuffer.wrap(serialized));
		} catch(ENOENTException exc) {}
	}
	
	protected byte[] serialize() {
		LinkedList<byte[]> pieces = new LinkedList<>();
		
		pieces.add(new byte[] { (byte) (requestingEverything ? 0x01 : 0x00) });		
		pieces.add(requestedPageTags.serialize());
		pieces.add(requestedInodes.serialize());
		pieces.add(requestedRevisions.serialize());
		pieces.add(requestedRevisionDetails.serialize());
		
		int totalBytes = 0;
		for(byte[] piece : pieces) totalBytes += piece.length;
		ByteBuffer buf = ByteBuffer.allocate(totalBytes);
		for(byte[] piece : pieces) buf.put(piece);
		return buf.array();
	}
	
	protected void deserialize(ByteBuffer buf) {
		requestingEverything = (buf.get() & 0x01) == 0x01;
		requestedPageTags.deserialize(buf);
		requestedInodes.deserialize(buf);
		requestedRevisions.deserialize(buf);
		requestedRevisionDetails.deserialize(buf);
		addDataRequests();
	}
	
	public HashMap<Integer,LinkedList<Long>> requestedPageTags() {
		return requestedPageTags.priorityMap();
	}
	
	public HashMap<Integer,LinkedList<InodeRef>> requestedInodes() {
		return requestedInodes.priorityMap();
	}
	
	public HashMap<Integer,LinkedList<RevisionTag>> requestedRevisions() {
		return requestedRevisions.priorityMap();
	}

	public HashMap<Integer,LinkedList<RevisionTag>> requestedRevisionDetails() {
		return requestedRevisionDetails.priorityMap();
	}

	public int numPagesRequested() {
		return requestedPageTags.size();
	}
	
	public int numInodesRequested() {
		return requestedInodes.size();
	}
	
	public int numRevisionsRequested() {
		return requestedRevisions.size();
	}
	
	public int numRevisionDetailsRequested() {
		return requestedRevisionDetails.size();
	}
	
	public boolean isRequestingEverything() {
		return requestingEverything;
	}
	
	public boolean isPaused() {
		return paused;
	}
}
