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
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.net.PeerConnection.PeerCapabilityException;
import com.acrescrypto.zksync.utility.Util;

public class RequestPool {
	public final static int DEFAULT_PRUNE_INTERVAL_MS = 30*1000;
	public static int pruneIntervalMs = DEFAULT_PRUNE_INTERVAL_MS;
	
	boolean dirty;
	ZKArchiveConfig config;
	HashMap<Integer,LinkedList<RefTag>> requestedRefTags = new HashMap<>();
	HashMap<Integer,LinkedList<RefTag>> requestedRevisions = new HashMap<>();
	HashMap<Integer,LinkedList<Long>> requestedPageTags = new HashMap<>();
	
	boolean requestingEverything, stopped, paused, requestingConfigInfo;
	Logger logger = LoggerFactory.getLogger(RequestPool.class);
	
	public RequestPool(ZKArchiveConfig config) {
		this.config = config;
		
		// TODO DHT: (test) automatically requests config info when canReceive is false
		// TODO DHT: (test) does not automatically request config info when canReceive is true
		// TODO DHT: (test) delays requests when config info not received
		// TODO DHT: (test) sends requests when config info received

		setRequestingConfigInfo(!config.canReceive());
		new Thread(()->pruneThread()).start();
	}
	
	public void stop() {
		stopped = true;
	}
	
	public void setRequestingEverything(boolean requestingEverything) {
		this.requestingEverything = requestingEverything;
		dirty = true;
	}
	
	public void setRequestingConfigInfo(boolean requestingConfigInfo) {
		this.requestingConfigInfo = requestingConfigInfo;
		dirty = true;
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
	
	public synchronized void addRefTag(int priority, RefTag refTag) {
		requestedRefTags.putIfAbsent(priority, new LinkedList<>());
		requestedRefTags.get(priority).add(refTag);
		dirty = true;

		if(config.canReceive()) {
			for(PeerConnection connection : config.getSwarm().connections) {
				ArrayList<RefTag> list = new ArrayList<>(1);
				list.add(refTag);
				try {
					connection.requestRefTags(priority, list);
				} catch (PeerCapabilityException e) {}
			}
		}
	}
	
	public synchronized void addRevision(int priority, RefTag revTag) {
		requestedRevisions.putIfAbsent(priority, new LinkedList<>());
		requestedRevisions.get(priority).add(revTag);
		dirty = true;
		
		if(config.canReceive()) {
			for(PeerConnection connection : config.getSwarm().connections) {
				ArrayList<RefTag> list = new ArrayList<>(1);
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
			for(PeerConnection connection : config.getSwarm().connections) {
				connection.requestPageTag(priority, shortTag);
			}
		}
	}
	
	public synchronized void addPageTag(int priority, byte[] pageTag) {
		addPageTag(priority, Util.shortTag(pageTag));
	}
	
	public boolean hasPageTag(int priority, long shortTag) {
		return requestedPageTags.getOrDefault(priority, new LinkedList<>()).contains(shortTag);
	}
	
	public boolean hasRefTag(int priority, RefTag refTag) {
		return requestedRefTags.getOrDefault(priority, new LinkedList<>()).contains(refTag);
	}
	
	public boolean hasRevision(int priority, RefTag revTag) {
		return requestedRevisions.getOrDefault(priority, new LinkedList<>()).contains(revTag);
	}

	
	public synchronized void prune() {
		if(config.getArchive() == null) return; // archive not initialized yet yet
		try {
			prunePageTags();
			pruneRefTags();
			pruneRevisionTags();
		} catch(IOException exc) {
			logger.error("Caught exception pruning request pool", exc);
		}
	}
	
	protected void addDataRequests() {
		for(PeerConnection conn : config.getSwarm().connections) {
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
			for(int priority : requestedRefTags.keySet()) {
				conn.requestRefTags(priority, requestedRefTags.get(priority));
			}
			
			for(int priority : requestedRevisions.keySet()) {
				conn.requestRevisionContents(priority, requestedRevisions.get(priority));
			}
		} catch(PeerCapabilityException exc) {}
	}
	
	protected void pruneThread() {
		while(!stopped) {
			try {
				Util.sleep(pruneIntervalMs);
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
				/** TODO DHT: (itf) Log spam happens, probably on DHTModuleTest::testPeerDiscovery
				 * 
				 * 16:51:07.675 [Thread-2843] ERROR c.acrescrypto.zksync.net.RequestPool - Prune thread caught exception
					java.lang.NegativeArraySizeException: null
					at com.acrescrypto.zksync.fs.File.read(File.java:34)
					at com.acrescrypto.zksync.fs.zkfs.InodeTable.inodesForPage(InodeTable.java:190)
					at com.acrescrypto.zksync.fs.zkfs.InodeTable.lambda$0(InodeTable.java:57)
					at com.acrescrypto.zksync.utility.HashCache.add(HashCache.java:40)
					at com.acrescrypto.zksync.utility.HashCache.get(HashCache.java:34)
					at com.acrescrypto.zksync.fs.zkfs.InodeTable.inodeWithId(InodeTable.java:145)
					at com.acrescrypto.zksync.fs.zkfs.InodeTable.readExisting(InodeTable.java:285)
					at com.acrescrypto.zksync.fs.zkfs.InodeTable.<init>(InodeTable.java:63)
					at com.acrescrypto.zksync.fs.zkfs.ZKFS.<init>(ZKFS.java:29)
					at com.acrescrypto.zksync.fs.zkfs.ZKFS.<init>(ZKFS.java:33)
					at com.acrescrypto.zksync.fs.zkfs.ZKArchive.openRevision(ZKArchive.java:68)
					at com.acrescrypto.zksync.fs.zkfs.ZKArchive.openRevision(ZKArchive.java:72)
					at com.acrescrypto.zksync.fs.zkfs.ZKArchive.hasRevision(ZKArchive.java:150)
					at com.acrescrypto.zksync.net.RequestPool.lambda$3(RequestPool.java:161)
					at java.util.Collection.removeIf(Collection.java:414)
					at com.acrescrypto.zksync.net.RequestPool.pruneRevisionTags(RequestPool.java:159)
					at com.acrescrypto.zksync.net.RequestPool.prune(RequestPool.java:106)
					at com.acrescrypto.zksync.net.RequestPool.pruneThread(RequestPool.java:116)
					at com.acrescrypto.zksync.net.RequestPool.lambda$0(RequestPool.java:35)
					at java.lang.Thread.run(Thread.java:748)
				 */
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
		for(int priority : requestedRefTags.keySet()) {
			LinkedList<RefTag> existing = requestedRefTags.get(priority);
			existing.removeIf((refTag)->{
				try {
					return config.getArchive().hasRefTag(refTag);
				} catch(IOException exc) {
					return false;
				}
			});
		}
	}

	protected void pruneRevisionTags() throws IOException {
		for(int priority : requestedRevisions.keySet()) {
			LinkedList<RefTag> existing = requestedRevisions.get(priority);
			existing.removeIf((revTag)->{
				try {
					return config.getArchive().hasRevision(revTag);
				} catch(IOException exc) {
					return false;
				}
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
		
		pieces.add(Util.serializeInt(requestedRefTags.size()));
		for(int priority : requestedRefTags.keySet()) {
			LinkedList<RefTag> list = requestedRefTags.get(priority);
			ByteBuffer buf = ByteBuffer.allocate(2*4+config.refTagSize()*list.size());
			buf.putInt(priority);
			buf.putInt(list.size());
			for(RefTag tag : list) buf.put(tag.getBytes());
			pieces.add(buf.array());
		}
		
		pieces.add(Util.serializeInt(requestedRevisions.size()));
		for(int priority : requestedRevisions.keySet()) {
			LinkedList<RefTag> list = requestedRevisions.get(priority);
			ByteBuffer buf = ByteBuffer.allocate(2*4+config.refTagSize()*list.size());
			buf.putInt(priority);
			buf.putInt(list.size());
			for(RefTag tag : list) buf.put(tag.getBytes());
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
		
		int numPageTagPriorities = buf.getInt();
		for(int i = 0; i < numPageTagPriorities; i++) {
			int priority = buf.getInt();
			int numEntries = buf.getInt();
			for(int j = 0; j < numEntries; j++) {
				addPageTag(priority, buf.getLong());
			}
		}
		
		int numRefTagPriorities = buf.getInt();
		for(int i = 0; i < numRefTagPriorities; i++) {
			int priority = buf.getInt();
			int numEntries = buf.getInt();
			for(int j = 0; j < numEntries; j++) {
				byte[] tagBytes = new byte[config.refTagSize()];
				buf.get(tagBytes);
				RefTag tag = new RefTag(config, tagBytes);
				addRefTag(priority, tag);
			}
		}
		
		int numRevisionPriorities = buf.getInt();
		for(int i = 0; i < numRevisionPriorities; i++) {
			int priority = buf.getInt();
			int numEntries = buf.getInt();
			for(int j = 0; j < numEntries; j++) {
				byte[] tagBytes = new byte[config.refTagSize()];
				buf.get(tagBytes);
				RefTag tag = new RefTag(config, tagBytes);
				addRevision(priority, tag);
			}
		}
		
		dirty = false;
	}
}
