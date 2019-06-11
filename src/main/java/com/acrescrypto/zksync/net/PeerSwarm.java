
package com.acrescrypto.zksync.net;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ClosedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.SocketClosedException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.net.Blacklist.BlacklistCallback;
import com.acrescrypto.zksync.utility.BandwidthAllocator;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.Util;

public class PeerSwarm implements BlacklistCallback {
	public final static int EMBARGO_EXPIRE_TIME_MS = 1000*60*10; // wait 10 minutes before retrying consistently unconnectable ads
	public final static int EMBARGO_SOFT_EXPIRE_TIME_MS = 1000; // wait 1s before retrying an ad before it is classified as consistently unconnectable
	public final static int EMBARGO_FAIL_COUNT_THRESHOLD = 3; // how many times do we try an ad before deeming it consistently unconnectable?
	public final static int DEFAULT_WAIT_PAGE_RETRY_TIME_MS = 5000; // how often should waitForPage retry requests for the page it is waiting in?
	public final static int DEFAULT_MAX_SOCKET_COUNT = 128;
	public final static int DEFAULT_MAX_PEER_LIST_SIZE = 1024;
	
	protected ArrayList<PeerConnection> connections = new ArrayList<PeerConnection>();
	protected HashSet<PeerAdvertisement> knownAds = new HashSet<PeerAdvertisement>();
	protected HashSet<PeerAdvertisement> connectedAds = new HashSet<PeerAdvertisement>();
	protected ZKArchiveConfig config;
	protected HashMap<Long,ChunkAccumulator> activeFiles = new HashMap<Long,ChunkAccumulator>();
	protected HashMap<Long,Condition> pageWaits = new HashMap<Long,Condition>();
	protected HashMap<PeerAdvertisement,Long> adEmbargoes = new HashMap<PeerAdvertisement,Long>();
	protected RequestPool pool;
	protected GroupedThreadPool threadPool;
	protected BandwidthMonitor bandwidthMonitorTx, bandwidthMonitorRx;
	
	protected Lock pageWaitLock = new ReentrantLock();
	protected Lock connectionWaitLock = new ReentrantLock();
	protected Condition connectionWaitCondition = connectionWaitLock.newCondition();
	protected Logger logger = LoggerFactory.getLogger(PeerSwarm.class);
	
	protected boolean closed;
	protected int activeSockets;
	
	private int maxSocketCount = DEFAULT_MAX_SOCKET_COUNT;
	int maxPeerListSize = DEFAULT_MAX_PEER_LIST_SIZE;
	int waitPageRetryTimeMs = DEFAULT_WAIT_PAGE_RETRY_TIME_MS;
	
	protected PeerSwarm() {}
	
	public PeerSwarm(ZKArchiveConfig config) throws IOException {
		this.config = config;
		this.config.getAccessor().getMaster().getBlacklist().addCallback(this);
		this.threadPool = GroupedThreadPool.newCachedThreadPool(config.getThreadGroup(), "PeerSwarm " + Util.bytesToHex(config.getArchiveId()));
		this.bandwidthMonitorTx = new BandwidthMonitor(100, 3000);
		this.bandwidthMonitorRx = new BandwidthMonitor(100, 3000);
		this.bandwidthMonitorTx.addParent(config.getMaster().getBandwidthMonitorTx());
		this.bandwidthMonitorRx.addParent(config.getMaster().getBandwidthMonitorRx());
		connectionThread();
		pool = new RequestPool(config);
		pool.read();
	}
	
	public void close() {
		logger.info("Swarm {} -: Closing PeerSwarm",
				Util.formatArchiveId(config.getArchiveId()));
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
	public synchronized void disconnectAddress(String address, long durationMs) {
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
				
				logger.info("Swarm {} {}:{}: Closing duplicate connection",
						Util.formatArchiveId(config.getArchiveId()),
						connection.socket.getAddress(),
						connection.socket.getPort());
				if(existingIsLowOrder) {
					connection.close();
					return;
				}
				
				existing.close();
				connections.remove(existing);
			}
			
			connection.announcePeers(knownAds);
			connections.add(connection);
			logger.info("Swarm {} {}:{}: Opened connection {} peers total",
					Util.formatArchiveId(config.getArchiveId()),
					connection.socket.getAddress(),
					connection.socket.getPort(),
					connections.size());
		}
		
		pool.addRequestsToConnection(connection);
		
		connectionWaitLock.lock();
		try {
			connectionWaitCondition.signal();
		} finally {
			connectionWaitLock.unlock();
		}
	}
	
	public synchronized void closedConnection(PeerConnection connection) {
		activeSockets--;
		connections.remove(connection);
		logger.info("Swarm {} {}:{}: Closed connection, {} peers remaining, retry={}",
				Util.formatArchiveId(config.getArchiveId()),
				connection.socket.address,
				connection.socket.getPort(),
				connections.size(),
				connection.retryOnClose());
		if(connection.retryOnClose()) {
			connectedAds.remove(connection.socket.getAd());
		}
	}
	
	public void addPeerAdvertisement(PeerAdvertisement ad) {
		// This could be improved. Once we hit capacity, how can we prune ads for low-quality peers for higher-quality ones?
		if(ad instanceof TCPPeerAdvertisement) { // ignore our own ad
			TCPPeerAdvertisement tcpAd = (TCPPeerAdvertisement) ad;
			if(tcpAd.pubKey.equals(getPublicIdentityKey())) return;
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
			logger.info("Swarm {} {}: Received ad with public key {}",
					Util.formatArchiveId(config.getArchiveId()),
					ad.routingInfo(),
					Util.formatPubKey(ad.getPubKey()));
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
		return config.getMaster().getTCPListener().getIdentityKey().publicKey();
	}
	
	protected void connectionThread() {
		threadPool.submit(() -> {
			Util.setThreadName("PeerSwarm connection thread");
			while(!closed) {
				PeerAdvertisement ad = selectConnectionAd();
				if(ad == null || activeSockets >= getMaxSocketCount()) {
					Util.sleep(100);
					continue;
				}
							
				try {
					logger.info("Swarm {} {}: Connecting to ad",
							Util.formatArchiveId(config.getArchiveId()),
							ad.routingInfo());
					openConnection(ad);
				} catch(Exception exc) {
					logger.error("Swarm {} {}: Connection thread caught exception handling ad from {} for archive {}",
							Util.formatArchiveId(config.getArchiveId()),
							ad.routingInfo(),
							exc);
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
			Util.setThreadName("PeerSwarm openConnection thread");
			PeerConnection conn = null;
			try {
				conn = ad.connect(this);
				openedConnection(conn);
			} catch (UnsupportedProtocolException exc) {
				logger.info("Swarm {} {}: Ignoring unsupported ad type {}",
						Util.formatArchiveId(config.getArchiveId()),
						ad.routingInfo(),
						ad.getType());
			} catch (ProtocolViolationException exc) {
				logger.warn("Swarm {} {}: Encountered protocol violation connecting to peer ad",
						Util.formatArchiveId(config.getArchiveId()),
						ad.routingInfo(),
						exc);
				try {
					ad.blacklist(config.getAccessor().getMaster().getBlacklist());
				} catch(IOException exc2) {
					logger.error("Swarm {} {}: Encountered IOException blacklisting peer ad",
							Util.formatArchiveId(config.getArchiveId()),
							ad.routingInfo(),
							exc);
				}
			} catch (BlacklistedException exc) {
				logger.debug("Swarm {} {}: Ignoring ad for blacklisted peer",
						Util.formatArchiveId(config.getArchiveId()),
						ad.routingInfo());
			} catch(SocketException|SocketClosedException|EOFException exc) {
				logger.info("Swarm {} {}: Caught network exception connecting to peer, key = {}",
						Util.formatArchiveId(config.getArchiveId()),
						ad.routingInfo(),
						Util.formatPubKey(ad.getPubKey()),
						exc);
			} catch(Exception exc) {
				if(!isClosed()) {
					logger.error("Swarm {} {}: Caught exception connecting to peer",
							Util.formatArchiveId(config.getArchiveId()),
							ad.routingInfo(),
							exc);
				}
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
	
	public void waitForPeers(long timeoutMs) {
		// TODO API: (test) waitForPeers
		if(timeoutMs <= 0) timeoutMs = Long.MAX_VALUE;
		
		long timeStart = System.currentTimeMillis(), deadline = timeStart + timeoutMs;
		
		if(connections.size() > 0) return;
		connectionWaitLock.lock();
		try {
			while(connections.size() == 0 && !closed && System.currentTimeMillis() < deadline) {
				connectionWaitCondition.await(1, TimeUnit.MILLISECONDS);
			}
		} catch (InterruptedException e) {
		} finally {
			connectionWaitLock.unlock();
		}
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
		logger.info("Swarm {} -: Announcing revtag {} to {} peers",
				Util.formatArchiveId(config.getArchiveId()),
				Util.formatRevisionTag(tip),
				connections.size());
		for(PeerConnection connection : getConnections()) {
			connection.announceTip(tip);
		}
	}
	
	public void requestTag(int priority, byte[] pageTag) {
		requestTag(priority, Util.shortTag(pageTag));
	}
	
	public void cancelTag(byte[] pageTag) {
		// TODO API: (test) cover cancelTag bytes
		cancelTag(Util.shortTag(pageTag));
	}
	
	public int priorityForTag(byte[] pageTag) {
		// TODO API: (test) cover priorityForTag
		return pool.priorityForPageTag(pageTag);
	}
	
	public void requestTag(int priority, long shortTag) {
		pool.addPageTag(priority, shortTag);
	}
	
	public int priorityForTag(long pageTag) {
		// TODO API: (test) cover priorityForTag
		return pool.priorityForPageTag(pageTag);
	}
	
	public void cancelTag(long shortTag) {
		// TODO API: (test) cover cancelTag long
		pool.cancelPageTag(shortTag);
	}
	
	public void requestInode(int priority, RevisionTag revTag, long inodeId) {
		pool.addInode(priority, revTag, inodeId);
	}
	
	public void cancelInode(RevisionTag revTag, long inodeId) {
		// TODO API: (test) cover cancelInode
		pool.cancelInode(revTag, inodeId);
	}
	
	public int priorityForInode(RevisionTag revTag, long inodeId) {
		// TODO API: (test) cover priorityForInode
		return pool.priorityForInode(revTag, inodeId);
	}
	
	public void requestRevision(int priority, RevisionTag revTag) {
		pool.addRevision(priority, revTag);
	}
	
	public void cancelRevision(RevisionTag revTag) {
		// TODO API: (test) cover cancelRevision
		pool.cancelRevision(revTag);
	}
	
	public int priorityForRevision(RevisionTag revTag) {
		// TODO API: (test) cover priorityForRevision
		return pool.priorityForRevision(revTag);
	}
	
	public void requestRevisionDetails(int priority, RevisionTag revTag) {
		pool.addRevisionDetails(priority, revTag);
	}
	
	public int priorityForRevisionDetails(RevisionTag revTag) {
		// TODO API: (test) cover priorityForRevisionDetails
		return pool.priorityForRevisionDetails(revTag);
	}
	
	public void requestAll() {
		logger.info("Swarm {} -: Requesting all pages",
				Util.formatArchiveId(config.getArchiveId()));
		pool.setRequestingEverything(true);
	}
	
	public void stopRequestingAll() {
		logger.info("Swarm {} -: Canceling request for all pages",
				Util.formatArchiveId(config.getArchiveId()));
		pool.setRequestingEverything(false);
		for(PeerConnection connection : getConnections()) {
			connection.requestAllCancel();
		}
	}
	
	public boolean isRequestingAll() {
		// TODO API: (test) isRequestingAll
		return pool.requestingEverything; 
	}

	public void setPaused(boolean paused) {
		logger.info("Swarm {} -: Setting paused={}",
				Util.formatArchiveId(config.getArchiveId()),
				paused);
		pool.setPaused(paused);
		for(PeerConnection connection : getConnections()) {
			connection.setPaused(paused);
		}
	}

	public void announceTips() throws IOException {
		// TODO API: (coverage) method... seems weird it's not called
		for(PeerConnection connection : getConnections()) {
			connection.announceTips();
		}
	}
	
	public void announcePeer(PeerAdvertisement ad) {
		// TODO API: (coverage) method... seems weird it's not called
		for(PeerConnection connection : getConnections()) {
			connection.announcePeer(ad);
		}
	}

	protected MutableSecureFile storedFile() throws IOException {
		FS fs = config.getMaster().localStorageFsForArchiveId(config.getArchiveId());
		return MutableSecureFile.atPath(fs, "identity", config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, "easysafe-swarm-identity"));
	}
	
	public void dumpConnections() {
		System.out.println("PeerSwarm: archive ID " + Util.bytesToHex(config.getArchiveId()));
		System.out.println("\tConnections: " + connections.size());
		int i = 0;
		for(PeerConnection connection : connections) {
			Util.debugLog(String.format("\t\tConnection %2d: %s %s\n",
					i++,
					Util.bytesToHex(connection.socket.remoteIdentityKey.getBytes()),
					(connection.socket.isLocalRoleClient() ? "client" : "server")));
		}
	}

	public int getMaxSocketCount() {
		return maxSocketCount;
	}

	public void setMaxSocketCount(int maxSocketCount) {
		// TODO API: (implement) Drop peers to enforce new max socket count if necesssary
		this.maxSocketCount = maxSocketCount;
	}

	public BandwidthMonitor getBandwidthMonitorTx() {
		return bandwidthMonitorTx;
	}

	public void setBandwidthMonitorTx(BandwidthMonitor bandwidthMonitor) {
		this.bandwidthMonitorTx = bandwidthMonitor;
	}

	public BandwidthAllocator getBandwidthAllocatorRx() {
		return config.getMaster().getBandwidthAllocatorRx();
	}

	public BandwidthAllocator getBandwidthAllocatorTx() {
		return config.getMaster().getBandwidthAllocatorTx();
	}

	public BandwidthMonitor getBandwidthMonitorRx() {
		return bandwidthMonitorRx;
	}

	public void setBandwidthMonitorRx(BandwidthMonitor bandwidthMonitorRx) {
		this.bandwidthMonitorRx = bandwidthMonitorRx;
	}

	public int numConnections() {
		return connections.size();
	}

	public int numKnownAds() {
		return knownAds.size();
	}
	
	public int numEmbargoedAds() {
		return adEmbargoes.size();
	}

	public int numConnectedAds() {
		return connectedAds.size();
	}

	public RequestPool getRequestPool() {
		return pool;
	}
}
