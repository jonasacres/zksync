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
	
	boolean dirty;
	ZKArchiveConfig config;
	HashList<InodeRef> requestedInodes = new HashList<InodeRef>(
			(r)->r.revTag.getShortHash() + r.inodeId + 1,
			(r)->{
				ByteBuffer buf = ByteBuffer.allocate(r.revTag.getBytes().length + 8);
				buf.put(r.revTag.getBytes());
				buf.putLong(r.inodeId);
				return buf.array();
			},
			(buf)->{
				RevisionTag revTag = new RevisionTag(config, buf, false);
				long inodeId = buf.getLong();
				return new InodeRef(revTag, inodeId);
			});
	HashList<RevisionTag> requestedRevisions = new HashList<RevisionTag>(
			(t)->Util.shortTag(t.getBytes()),
			(t)->t.getBytes(),
			(buf)->new RevisionTag(config, buf, false));
	HashList<RevisionTag> requestedRevisionDetails = new HashList<RevisionTag>(
			(t)->Util.shortTag(t.getBytes()),
			(t)->t.getBytes(),
			(buf)->new RevisionTag(config, buf, false));
	HashList<Long> requestedPageTags = new HashList<Long>(
			(t)->t.longValue(),
			(t)->Util.serializeLong(t.longValue()),
			(buf)->buf.getLong());
	
	interface Hasher<T> { public long hash(T item); }
	interface Remover<T> { public boolean test(T item); }
	interface Serializer<T> { public byte[] serialize(T item); }
	interface Deserializer<T> { public T deserialize(ByteBuffer serialization); }
	
	class InodeRef {
		RevisionTag revTag;
		long inodeId;
		
		public InodeRef(RevisionTag revTag, long inodeId) {
			this.revTag = revTag;
			this.inodeId = inodeId;
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
	
	class HashList<T> implements Iterable<HashListEntry<T>> {
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
	}
	
	boolean requestingEverything, stopped, paused, requestingConfigInfo;
	Logger logger = LoggerFactory.getLogger(RequestPool.class);
	
	public RequestPool(ZKArchiveConfig config) {
		this.config = config;
		
		setRequestingConfigInfo(!config.canReceive());
		new Thread(config.getThreadGroup(), ()->pruneThread()).start();
	}
	
	public synchronized void stop() {
		stopped = true;
		this.notifyAll();
	}
	
	public void setRequestingEverything(boolean requestingEverything) {
		this.requestingEverything = requestingEverything;
		dirty = true;
		
		if(config.canReceive()) {
			for(PeerConnection connection : config.getSwarm().getConnections()) {
				connection.requestAll();
			}
		}
	}
	
	public void setRequestingConfigInfo(boolean requestingConfigInfo) {
		this.requestingConfigInfo = requestingConfigInfo;
		dirty = true;
	}
	
	public void setPaused(boolean paused) {
		this.paused = paused;
	}
	
	public void receivedConfigInfo() {
		setRequestingConfigInfo(false);
		addDataRequests();
	}
	
	public synchronized void addRequestsToConnection(PeerConnection conn) {
		conn.setPaused(paused);
		
		if(requestingConfigInfo) {
			conn.requestConfigInfo();
		}
		
		if(config.canReceive()) {
			addDataRequestsToConnection(conn);
		}
	}
	
	public synchronized void addInode(int priority, RevisionTag revTag, long inodeId) {
		requestedInodes.add(priority, new InodeRef(revTag, inodeId));
		dirty = true;

		if(config.canReceive()) {
			for(PeerConnection connection : config.getSwarm().getConnections()) {
				ArrayList<Long> list = new ArrayList<>(1);
				list.add(inodeId);
				try {
					connection.requestInodes(priority, revTag, list);
				} catch (PeerCapabilityException e) {}
			}
		}
	}
	
	public synchronized void addRevision(int priority, RevisionTag revTag) {
		requestedRevisions.add(priority,  revTag);
		dirty = true;
		
		if(config.canReceive()) {
			for(PeerConnection connection : config.getSwarm().getConnections()) {
				ArrayList<RevisionTag> list = new ArrayList<>(1);
				list.add(revTag);
				try {
					connection.requestRevisionContents(priority, list);
				} catch(PeerCapabilityException exc) {}
			}
		}
	}
	
	public synchronized void addPageTag(int priority, long shortTag) {
		requestedPageTags.add(priority, shortTag);
		dirty = true;
		
		if(config.canReceive()) {
			for(PeerConnection connection : config.getSwarm().getConnections()) {
				connection.requestPageTag(priority, shortTag);
			}
		}
	}
	
	public synchronized void addPageTag(int priority, byte[] pageTag) {
		addPageTag(priority, Util.shortTag(pageTag));
	}
	
	public synchronized void addRevisionDetails(int priority, RevisionTag revTag) {
		requestedRevisionDetails.add(priority, revTag);
		
		if(config.canReceive()) {
			for(PeerConnection connection : config.getSwarm().getConnections()) {
				try {
					connection.requestRevisionDetails(priority, revTag);
				} catch(PeerCapabilityException exc) {}
			}
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
		for(PeerConnection conn : config.getSwarm().getConnections()) {
			addDataRequestsToConnection(conn);
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
			tagMap.putIfAbsent(entry.item.revTag, new LinkedList<Long>());
			tagMap.get(entry.item.revTag).add(entry.item.inodeId);
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
						logger.error("Caught exception writing request pool file", exc);
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
				return config.getArchive().hasInode(r.revTag, r.inodeId);
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
		return config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_REQUEST_POOL);
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
}
