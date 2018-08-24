package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ClosedException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.net.Blacklist.BlacklistCallback;
import com.acrescrypto.zksync.utility.Util;

public class PeerSwarm implements BlacklistCallback {
	public final static int EMBARGO_EXPIRE_TIME_MS = 1000*60*10; // wait 10 minutes before retrying consistently unconnectable ads
	public final static int EMBARGO_SOFT_EXPIRE_TIME_MS = 1000; // wait 1s before retrying an ad before it is classified as consistently unconnectable
	public final static int EMBARGO_FAIL_COUNT_THRESHOLD = 3; // how many times do we try an ad before deeming it consistently unconnectable?
	public final static int DEFAULT_WAIT_PAGE_RETRY_TIME_MS = 5000; // how often should waitForPage retry requests for the page it is waiting in?
	
	protected ArrayList<PeerConnection> connections = new ArrayList<PeerConnection>();
	protected HashSet<PeerAdvertisement> knownAds = new HashSet<PeerAdvertisement>();
	protected HashSet<PeerAdvertisement> connectedAds = new HashSet<PeerAdvertisement>();
	protected ZKArchiveConfig config;
	protected HashMap<Long,ChunkAccumulator> activeFiles = new HashMap<Long,ChunkAccumulator>();
	protected HashMap<Long,Condition> pageWaits = new HashMap<Long,Condition>();
	protected HashMap<PeerAdvertisement,Long> adEmbargoes = new HashMap<PeerAdvertisement,Long>();
	protected RequestPool pool;
	protected PrivateDHKey identityKey;
	protected ExecutorService threadPool = Executors.newCachedThreadPool();
	
	protected Lock pageWaitLock = new ReentrantLock();
	protected Logger logger = LoggerFactory.getLogger(PeerSwarm.class);
	
	protected boolean closed;
	protected int activeSockets;
	
	int maxSocketCount = 128;
	int maxPeerListSize = 1024;
	int waitPageRetryTimeMs = DEFAULT_WAIT_PAGE_RETRY_TIME_MS;
	
	public PeerSwarm(ZKArchiveConfig config) throws IOException {
		this.config = config;
		this.config.getAccessor().getMaster().getBlacklist().addCallback(this);
		connectionThread();
		initIdentity();
		pool = new RequestPool(config);
		pool.read();
	}
	
	public void close() {
		closed = true;
		if(pool != null) pool.stop();
		
		for(PeerConnection connection : getConnections()) {
			connection.close();
		}
		
		synchronized(this) {
			pageWaitLock.lock();
			try {
				for(Condition cond : pageWaits.values()) {
					cond.signalAll();
				}
			} finally {
				pageWaitLock.unlock();
			}
		}
		
		this.config.getAccessor().getMaster().getBlacklist().removeCallback(this);
	}
	
	public boolean isClosed() {
		return closed;
	}
	
	public ZKArchiveConfig getConfig() {
		return config;
	}
	
	@Override
	public synchronized void disconnectAddress(String address, int durationMs) {
		LinkedList<PeerConnection> toRemoveConnections = new LinkedList<PeerConnection>();
		LinkedList<PeerAdvertisement> toRemoveAds = new LinkedList<PeerAdvertisement>();
		
		for(PeerConnection connection : connections) {
			PeerAdvertisement ad = connection.socket.getAd();
			if(connection.socket.matchesAddress(address) || (ad != null && ad.matchesAddress(address))) {
				toRemoveConnections.add(connection);
			}
		}
		
		for(PeerConnection connection : toRemoveConnections) {
			connection.close();
		}
		
		for(PeerAdvertisement ad : knownAds) {
			if(ad.matchesAddress(address)) {
				toRemoveAds.add(ad);
			}
		}
		
		for(PeerAdvertisement ad : toRemoveAds) {
			knownAds.remove(ad);
		}
	}
	
	public void openedConnection(PeerConnection connection) {
		if(closed) {
			connection.close();
			return;
		}
		
		synchronized(this) {
			PeerAdvertisement ad = connection.socket.getAd();
			if(ad != null) {
				// should be in both of these already but let's make sure
				connectedAds.remove(ad);
				connectedAds.add(connection.socket.getAd());
				knownAds.remove(ad);
				knownAds.add(connection.socket.getAd());
	
				for(PeerConnection existing : connections) {
					existing.announcePeer(ad);
				}
			}
			
			for(PeerConnection existing : getConnections()) {
				if(existing == connection) continue;
				if(existing.socket.remoteIdentityKey == null) continue;
				if(!Arrays.equals(connection.socket.remoteIdentityKey.getBytes(), existing.socket.remoteIdentityKey.getBytes())) continue;
				boolean existingIsLowOrder = Util.compareArrays(existing.socket.getSharedSecret(), connection.socket.getSharedSecret()) < 0;
				
				if(existingIsLowOrder) {
					connection.close();
					return;
				}
				
				existing.close();
				connections.remove(existing);
			}
			
			connection.announcePeers(knownAds);
			connections.add(connection);
		}
		
		pool.addRequestsToConnection(connection);
	}
	
	public synchronized void closedConnection(PeerConnection connection) {
		activeSockets--;
		connections.remove(connection);
	}
	
	public void addPeerAdvertisement(PeerAdvertisement ad) {
		// This could be improved. Once we hit capacity, how can we prune ads for low-quality peers for higher-quality ones?
		if(ad instanceof TCPPeerAdvertisement) { // ignore our own ad
			TCPPeerAdvertisement tcpAd = (TCPPeerAdvertisement) ad;
			if(tcpAd.pubKey.equals(identityKey.publicKey())) return;
		}
		
		if(knownAds.size() >= maxPeerListSize) return;
		if(!PeerSocket.adSupported(ad)) return;
		
		boolean announce;

		synchronized(this) {
			if(knownAds.size() >= maxPeerListSize) return;
			announce = !knownAds.contains(ad);
			if(announce) {
				knownAds.add(ad);
			}
		}

		if(announce) {
			announcePeer(ad);
		}
	}
	
	public Collection<PeerAdvertisement> knownAds() {
		return knownAds;
	}
	
	public void advertiseSelf(PeerAdvertisement ad) {
		for(PeerConnection connection : getConnections()) {
			connection.announceSelf(ad);
		}
	}
	
	public synchronized Collection<PeerConnection> getConnections() {
		return new ArrayList<>(connections);
	}
	
	public PublicDHKey getPublicIdentityKey() {
		return identityKey.publicKey();
	}
	
	protected void connectionThread() {
		threadPool.submit(() -> {
			Thread.currentThread().setName("PeerSwarm connection thread");
			while(!closed) {
				PeerAdvertisement ad = selectConnectionAd();
				if(ad == null || activeSockets >= maxSocketCount) {
					Util.sleep(100);
					continue;
				}
							
				try {
					logger.trace("Connecting to ad {}", ad);
					openConnection(ad);
				} catch(Exception exc) {
					logger.error("Connection thread caught exception handling ad {}", ad, exc);
				}
			}
		});
	}
	
	protected synchronized PeerAdvertisement selectConnectionAd() {
		for(PeerAdvertisement ad : knownAds) {
			if(isConnectedToAd(ad)) continue;
			if(adEmbargoes.containsKey(ad)) {
				if(adEmbargoes.get(ad) <= Util.currentTimeMillis()) {
					adEmbargoes.remove(ad);
					if(ad.failCount >= EMBARGO_FAIL_COUNT_THRESHOLD) {
						ad.failCount = 0;
					}
				} else {
					continue;
				}
			}
			return ad;
		}
		
		return null;
	}
	
	protected boolean isConnectedToAd(PeerAdvertisement ad) {
		if(connectedAds.contains(ad)) return true;
		return connectionForKey(ad.pubKey) != null;
	}
	
	protected PeerConnection connectionForKey(PublicDHKey key) {
		for(PeerConnection connection : getConnections()) {
			if(connection.socket.remoteIdentityKey != null && java.util.Arrays.equals(connection.socket.remoteIdentityKey.getBytes(), key.getBytes())) {
				return connection;
			}
		}
		
		return null;
	}
	
	protected void openConnection(PeerAdvertisement ad) {
		if(closed) return;
		synchronized(this) {
			activeSockets++;
			connectedAds.add(ad);
		}
		
		threadPool.submit(()-> {
			Thread.currentThread().setName("PeerSwarm openConnection thread");
			PeerConnection conn = null;
			try {
				conn = ad.connect(this);
				openedConnection(conn);
			} catch (UnsupportedProtocolException exc) {
				logger.info("Ignoring unsupported ad type " + ad.getType());
			} catch (ProtocolViolationException exc) {
				logger.warn("Encountered protocol violation connecting to peer ad: {}", ad, exc);
				try {
					ad.blacklist(config.getAccessor().getMaster().getBlacklist());
				} catch(IOException exc2) {
					logger.error("Encountered IOException blacklisting peer ad: {}", ad, exc);
				}
			} catch (BlacklistedException exc) {
				logger.debug("Ignoring ad for blacklisted peer {}", ad);
			} catch(SocketException exc) {
				logger.info("Caught network exception connecting to peer {}", ad);
			} catch(Exception exc) {
				logger.error("Caught exception connecting to peer {}", ad, exc);
			} finally {
				if(conn == null) {
					synchronized(this) {
						activeSockets--;
						boolean unconnectable = ++ad.failCount >= EMBARGO_FAIL_COUNT_THRESHOLD;
						int delay = unconnectable ? EMBARGO_EXPIRE_TIME_MS : EMBARGO_SOFT_EXPIRE_TIME_MS;
						
						adEmbargoes.put(ad, Util.currentTimeMillis() + delay);
						connectedAds.remove(ad);
					}
				}
			}
		});
	}
	
	public void waitForPage(int priority, byte[] tag) {
		long shortTag = Util.shortTag(tag);
		while(waitingForPage(tag)) {
			requestTag(priority, tag);
			pageWaitLock.lock();
			try {
				if(!pageWaits.containsKey(shortTag)) {
					pageWaits.put(shortTag, pageWaitLock.newCondition());
				}
				
				try {
					if(waitingForPage(tag)) {
						pageWaits.get(shortTag).await(waitPageRetryTimeMs, TimeUnit.MILLISECONDS);
					}
				} catch (InterruptedException e) {}
			} finally {		
				pageWaitLock.unlock();
			}
		}
	}
	
	protected boolean waitingForPage(byte[] tag) {
		if(Arrays.equals(tag, config.tag())) {
			return !config.getCacheStorage().exists(Page.pathForTag(tag));
		} else {
			try {
				return config.getArchive() == null || !config.getArchive().hasPageTag(tag);
			} catch (ClosedException e) {
				return false;
			}  
		}
	}
	
	public synchronized ChunkAccumulator accumulatorForTag(byte[] tag) throws IOException {
		long shortTag = Util.shortTag(tag);
		if(!activeFiles.containsKey(shortTag)) {
			int numChunksExpected = (int) Math.ceil((double) config.getPageSize() / PeerMessage.FILE_CHUNK_SIZE);
			ChunkAccumulator fileHandle = new ChunkAccumulator(this, tag, numChunksExpected);
			
			activeFiles.put(shortTag, fileHandle);
			return fileHandle;
		}
		
		return activeFiles.get(shortTag);
	}
	
	protected void receivedConfigInfo() {
		pool.receivedConfigInfo();
	}
	
	protected synchronized void receivedPage(byte[] tag) {
		long shortTag = Util.shortTag(tag);
		activeFiles.remove(shortTag);
		
		if(config.getArchive() != null) {
			config.getArchive().addPageTag(tag);
		}

		pageWaitLock.lock();
		try {
			if(pageWaits.containsKey(shortTag)) {
				pageWaits.get(shortTag).signalAll();
			}
		} finally {
			pageWaitLock.unlock();
		}
		
		announceTag(tag);
	}
	
	public void announceTag(byte[] tag) {
		long shortTag = Util.shortTag(tag);
		for(PeerConnection connection : getConnections()) {
			connection.announceTag(shortTag);
		}
	}
	
	public void announceTip(RevisionTag tip) {
		for(PeerConnection connection : getConnections()) {
			connection.announceTip(tip);
		}
	}
	
	public void requestTag(int priority, byte[] pageTag) {
		requestTag(priority, Util.shortTag(pageTag));
	}
	
	public void requestTag(int priority, long shortTag) {
		pool.addPageTag(priority, shortTag);
	}
	
	public void requestInode(int priority, RevisionTag revTag, long inodeId) {
		pool.addInode(priority, revTag, inodeId);
	}
	
	public void requestRevision(int priority, RevisionTag revTag) {
		pool.addRevision(priority, revTag);
	}
	
	// TODO DHT: test requestRevisionDetails
	public void requestRevisionDetails(int priority, RevisionTag revTag) {
		pool.addRevisionDetails(priority, revTag);
	}
	
	public void requestAll() {
		pool.setRequestingEverything(true);
	}
	
	public void stopRequestingAll() {
		pool.setRequestingEverything(false);
		for(PeerConnection connection : getConnections()) {
			connection.requestAllCancel();
		}
	}
	
	public void setPaused(boolean paused) {
		pool.setPaused(paused);
		for(PeerConnection connection : getConnections()) {
			connection.setPaused(paused);
		}
	}

	public void announceTips() throws IOException {
		for(PeerConnection connection : getConnections()) {
			connection.announceTips();
		}
	}
	
	public void announcePeer(PeerAdvertisement ad) {
		for(PeerConnection connection : getConnections()) {
			connection.announcePeer(ad);
		}
	}

	protected MutableSecureFile storedFile() throws IOException {
		FS fs = config.getMaster().localStorageFsForArchiveId(config.getArchiveId());
		return MutableSecureFile.atPath(fs, "identity", config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_AD_IDENTITY));
	}
	
	protected void initIdentity() {
		try {
			deserialize(storedFile().read());
		} catch(ENOENTException exc) {
			// let it slide without log message
		} catch(SecurityException exc) {
			logger.warn("Caught security exception decrypting stored advertisement for archive {}; creating new ad.", Util.bytesToHex(config.getArchiveId()), exc);
		} catch (IOException exc) {
			logger.error("Caught exception opening stored advertisement for archive {}", Util.bytesToHex(config.getArchiveId()), exc);
		} finally {
			if(this.identityKey != null) return;
			try {
				this.identityKey = config.getCrypto().makePrivateDHKey();
				storedFile().write(serialize(), 0);
			} catch (IOException exc) {
				logger.error("Caught exception writing advertisement for archive {}", Util.bytesToHex(config.getArchiveId()), exc);
			}
		}
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(config.getCrypto().asymPrivateDHKeySize() + config.getCrypto().asymPublicDHKeySize());
		buf.put(identityKey.getBytes());
		buf.put(identityKey.publicKey().getBytes());
		assert(!buf.hasRemaining());
		return buf.array();
	}
	
	protected void deserialize(byte[] serialized) {
		byte[] privateKeyRaw = new byte[config.getCrypto().asymPrivateDHKeySize()];
		byte[] publicKeyRaw = new byte[config.getCrypto().asymPublicDHKeySize()];
		assert(serialized.length == privateKeyRaw.length + publicKeyRaw.length);
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		buf.get(privateKeyRaw);
		buf.get(publicKeyRaw);
		assert(!buf.hasRemaining());
		
		identityKey = config.getCrypto().makePrivateDHKeyPair(privateKeyRaw, publicKeyRaw);
	}
	
	public void dumpConnections() {
		System.out.println("PeerSwarm: archive ID " + Util.bytesToHex(config.getArchiveId()));
		System.out.println("\tIdentity: " + Util.bytesToHex(identityKey.publicKey().getBytes()));
		System.out.println("\tConnections: " + connections.size());
		int i = 0;
		for(PeerConnection connection : connections) {
			System.out.println("\t\tConnection " + (i++) + ": " + Util.bytesToHex(connection.socket.remoteIdentityKey.getBytes()) + " " + (connection.socket.isLocalRoleClient() ? "client" : "server"));
		}
	}
}
