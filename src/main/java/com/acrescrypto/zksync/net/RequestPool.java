package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
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
	HashMap<Integer,HashMap<RevisionTag,LinkedList<Long>>> requestedInodes = new HashMap<>();
	HashMap<Integer,LinkedList<RevisionTag>> requestedRevisions = new HashMap<>();
	HashMap<Integer,LinkedList<Long>> requestedPageTags = new HashMap<>();
	HashMap<Integer,LinkedList<RevisionTag>> requestedRevisionDetails = new HashMap<>();
	
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
		requestedInodes.putIfAbsent(priority, new HashMap<>());
		requestedInodes.get(priority).putIfAbsent(revTag, new LinkedList<>());
		requestedInodes.get(priority).get(revTag).add(inodeId);
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
		requestedRevisions.putIfAbsent(priority, new LinkedList<>());
		requestedRevisions.get(priority).add(revTag);
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
		requestedPageTags.putIfAbsent(priority, new LinkedList<>());
		requestedPageTags.get(priority).add(shortTag);
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
		requestedRevisionDetails.putIfAbsent(priority, new LinkedList<>());
		requestedRevisionDetails.get(priority).add(revTag);
		
		if(config.canReceive()) {
			for(PeerConnection connection : config.getSwarm().getConnections()) {
				try {
					connection.requestRevisionDetails(priority, revTag);
				} catch(PeerCapabilityException exc) {}
			}
		}
	}
	
	public boolean hasPageTag(int priority, long shortTag) {
		return requestedPageTags
				.getOrDefault(priority, new LinkedList<>())
				.contains(shortTag);
	}
	
	public boolean hasInode(int priority, RevisionTag refTag, long inodeId) {
		return requestedInodes
				.getOrDefault(priority, new HashMap<>())
				.getOrDefault(refTag, new LinkedList<>())
				.contains(inodeId);
	}
	
	public boolean hasRevision(int priority, RevisionTag revTag) {
		return requestedRevisions.getOrDefault(priority, new LinkedList<>()).contains(revTag);
	}
	
	public boolean hasRevisionDetails(int priority, RevisionTag revTag) {
		return requestedRevisionDetails.getOrDefault(priority, new LinkedList<>()).contains(revTag);
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
		for(PeerConnection conn : config.getSwarm().getConnections()) {
			addDataRequestsToConnection(conn);
		}
	}
	
	protected void addDataRequestsToConnection(PeerConnection conn) {
		if(requestingEverything) {
			conn.requestAll();
		}
		
		for(int priority : requestedPageTags.keySet()) {
			conn.requestPageTags(priority, requestedPageTags.get(priority));
		}
		
		try {
			for(int priority : requestedInodes.keySet()) {
				for(RevisionTag revTag : requestedInodes.get(priority).keySet()) {
					conn.requestInodes(priority, revTag, requestedInodes.get(priority).get(revTag));
				}
			}
			
			for(int priority : requestedRevisions.keySet()) {
				conn.requestRevisionContents(priority, requestedRevisions.get(priority));
			}
			
			for(int priority : requestedRevisionDetails.keySet()) {
				conn.requestRevisionDetails(priority, requestedRevisionDetails.get(priority));
			}
		} catch(PeerCapabilityException exc) {}
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
		for(int priority : requestedPageTags.keySet()) {
			LinkedList<Long> existing = requestedPageTags.get(priority);
			existing.removeIf((shortTag)->{
				try {
					return config.getArchive().expandShortTag(shortTag) != null;
				} catch(IOException exc) {
					return false;
				}
			});
		}
	}
	
	protected void pruneRefTags() throws IOException {
		for(int priority : requestedInodes.keySet()) {
			HashMap<RevisionTag,LinkedList<Long>> existing = requestedInodes.get(priority);
			LinkedList<RevisionTag> emptyTags = new LinkedList<>();
			for(RevisionTag revTag : existing.keySet()) {
				LinkedList<Long> inodeIds = existing.get(revTag);
				if(inodeIds.isEmpty()) {
					emptyTags.add(revTag);
				} else {
					inodeIds.removeIf((inodeId)->{
						try {
							return config.getArchive().hasInode(revTag, inodeId);
						} catch (IOException e) {
							return false;
						}
					});
				}
			}
		}
	}

	protected void pruneRevisionTags() throws IOException {
		for(int priority : requestedRevisions.keySet()) {
			LinkedList<RevisionTag> existing = requestedRevisions.get(priority);
			existing.removeIf((revTag)->{
				try {
					return config.getArchive().hasRevision(revTag);
				} catch(IOException exc) {
					return false;
				}
			});
		}
	}
	
	protected void pruneRevisionDetails() throws IOException {
		for(int priority : requestedRevisionDetails.keySet()) {
			LinkedList<RevisionTag> existing = requestedRevisionDetails.get(priority);
			existing.removeIf((revTag)->{
				return config.getRevisionTree().hasParentsForTag(revTag);
			});
		}
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
		
		pieces.add(Util.serializeInt(requestedPageTags.size()));
		for(int priority : requestedPageTags.keySet()) {
			LinkedList<Long> list = requestedPageTags.get(priority);
			ByteBuffer buf = ByteBuffer.allocate(2*4+8*list.size());
			buf.putInt(priority);
			buf.putInt(list.size());
			for(long shortTag : list) buf.putLong(shortTag);
			pieces.add(buf.array());
		}
		
		pieces.add(Util.serializeInt(requestedInodes.size()));
		for(int priority : requestedInodes.keySet()) {
			HashMap<RevisionTag, LinkedList<Long>> map = requestedInodes.get(priority);
			pieces.add(Util.serializeInt(priority));
			pieces.add(Util.serializeInt(map.size()));

			for(RevisionTag revTag : map.keySet()) {
				LinkedList<Long> inodeIds = map.get(revTag);
				ByteBuffer buf = ByteBuffer.allocate(RevisionTag.sizeForConfig(config) + 4 + 8*inodeIds.size());
				buf.put(revTag.getBytes());
				buf.putInt(inodeIds.size());
				for(long inodeId : inodeIds) buf.putLong(inodeId);
				pieces.add(buf.array());
			}
		}
		
		pieces.add(Util.serializeInt(requestedRevisions.size()));
		for(int priority : requestedRevisions.keySet()) {
			LinkedList<RevisionTag> list = requestedRevisions.get(priority);
			ByteBuffer buf = ByteBuffer.allocate(2*4+RevisionTag.sizeForConfig(config)*list.size());
			buf.putInt(priority);
			buf.putInt(list.size());
			for(RevisionTag tag : list) buf.put(tag.getBytes());
			pieces.add(buf.array());
		}
		
		int totalBytes = 0;
		for(byte[] piece : pieces) totalBytes += piece.length;
		ByteBuffer buf = ByteBuffer.allocate(totalBytes);
		for(byte[] piece : pieces) buf.put(piece);
		return buf.array();
	}
	
	protected void deserialize(ByteBuffer buf) {
		requestingEverything = (buf.get() & 0x01) == 0x01;
		byte[] tagBytes = new byte[RevisionTag.sizeForConfig(config)];
		
		int numPageTagPriorities = buf.getInt();
		for(int i = 0; i < numPageTagPriorities; i++) {
			int priority = buf.getInt();
			int numEntries = buf.getInt();
			for(int j = 0; j < numEntries; j++) {
				addPageTag(priority, buf.getLong());
			}
		}
		
		int numInodePriorities = buf.getInt();
		for(int i = 0; i < numInodePriorities; i++) {
			int priority = buf.getInt();
			int numRevTags = buf.getInt();
			for(int j = 0; j < numRevTags; j++) {
				buf.get(tagBytes);
				int numInodeIds = buf.getInt();
				RevisionTag revTag = new RevisionTag(config, tagBytes, false);
				
				for(int k = 0; k < numInodeIds; k++) {
					addInode(priority, revTag, buf.getLong());
				}
			}
		}
		
		int numRevisionPriorities = buf.getInt();
		for(int i = 0; i < numRevisionPriorities; i++) {
			int priority = buf.getInt();
			int numEntries = buf.getInt();
			for(int j = 0; j < numEntries; j++) {
				buf.get(tagBytes);
				RevisionTag tag = new RevisionTag(config, tagBytes, false);
				addRevision(priority, tag);
			}
		}
		
		dirty = false;
	}
}
