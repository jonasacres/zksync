package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.DirectoryTraverser;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.net.Blacklist.BlacklistCallback;
import com.acrescrypto.zksync.utility.Util;

public class PeerSwarm implements BlacklistCallback {
	public final static int EMBARGO_EXPIRE_TIME_MILLIS = 1000*60*10; // wait 10 minutes before retrying unconnectable ads
	
	protected ArrayList<PeerConnection> connections = new ArrayList<PeerConnection>();
	protected HashSet<PeerAdvertisement> knownAds = new HashSet<PeerAdvertisement>();
	protected HashSet<PeerAdvertisement> connectedAds = new HashSet<PeerAdvertisement>();
	protected ZKArchiveConfig config;
	protected HashSet<Long> currentTags = new HashSet<Long>();
	protected HashMap<Long,ChunkAccumulator> activeFiles = new HashMap<Long,ChunkAccumulator>();
	protected HashMap<Long,Condition> pageWaits = new HashMap<Long,Condition>();
	protected HashMap<PeerAdvertisement,Long> adEmbargoes = new HashMap<PeerAdvertisement,Long>();
	
	protected Lock pageWaitLock = new ReentrantLock();
	protected Logger logger = LoggerFactory.getLogger(PeerSwarm.class);
	
	protected boolean closed;
	protected int activeSockets;
	
	int maxSocketCount = 128;
	int maxPeerListSize = 1024;
	
	public PeerSwarm(ZKArchiveConfig config) throws IOException {
		this.config = config;
		this.config.getAccessor().getMaster().getBlacklist().addCallback(this);
		buildCurrentTags();
		connectionThread();
	}
	
	public synchronized void close() {
		closed = true;
		for(PeerConnection connection : connections) {
			connection.close();
		}
		
		pageWaitLock.lock();
		for(Condition cond : pageWaits.values()) {
			cond.signalAll();
		}
		pageWaitLock.unlock();
		
		this.config.getAccessor().getMaster().getBlacklist().removeCallback(this);
	}
	
	public ZKArchiveConfig getConfig() {
		return config;
	}
	
	public void buildCurrentTags() throws IOException {
		FS fs = config.getAccessor().getMaster().storageFsForArchiveId(config.getArchiveId());
		DirectoryTraverser traverser = new DirectoryTraverser(fs, fs.opendir("/"));
		while(traverser.hasNext()) {
			byte[] tag = Page.tagForPath(traverser.next());
			currentTags.add(Util.shortTag(tag));
		}
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
	
	public synchronized void openedConnection(PeerConnection connection) {
		activeSockets++;
		if(closed) {
			connection.close();
			return;
		}
		
		PeerAdvertisement ad = connection.socket.getAd();
		if(ad != null) { // should be in both of these already but let's make sure
			connectedAds.remove(ad);
			connectedAds.add(connection.socket.getAd());
			knownAds.remove(ad);
			knownAds.add(connection.socket.getAd());
		}
		connections.add(connection);
	}
	
	public synchronized void closedConnection(PeerConnection connection) {
		activeSockets--;
		connections.remove(connection);
	}
	
	public synchronized void addPeerAdvertisement(PeerAdvertisement ad) {
		// This could be improved. Once we hit capacity, how can we prune ads for low-quality peers for higher-quality ones?
		if(knownAds.size() >= maxPeerListSize) return;
		if(!PeerSocket.adSupported(ad)) return;
		
		knownAds.remove(ad);
		knownAds.add(ad);
	}
	
	public synchronized void advertiseSelf(PeerAdvertisement ad) {
		for(PeerConnection connection : connections) {
			connection.announceSelf(ad);
		}
	}
	
	public void connectionThread() {
		new Thread(() -> {
			while(!closed) {
				PeerAdvertisement ad = selectConnectionAd();
				if(ad == null || activeSockets >= maxSocketCount) {
					try {
						TimeUnit.MILLISECONDS.sleep(100);
					} catch(InterruptedException exc) {}
					continue;
				}
				
				try {
					logger.trace("Connecting to ad {}", ad);
					openConnection(ad);
				} catch(Exception exc) {
					logger.error("Connection thread caught exception handling ad {}", ad, exc);
				}
			}
		}).start();
	}
	
	protected synchronized PeerAdvertisement selectConnectionAd() {
		for(PeerAdvertisement ad : knownAds) {
			if(connectedAds.contains(ad)) continue;
			if(adEmbargoes.containsKey(ad)) {
				long expireTime = Util.currentTimeMillis() - EMBARGO_EXPIRE_TIME_MILLIS;
				if(adEmbargoes.get(ad) <= expireTime) {
					adEmbargoes.remove(ad);
				} else {
					continue;
				}
			}
			return ad;
		}
		
		return null;
	}
	
	protected void openConnection(PeerAdvertisement ad) {
		if(closed) return;
		synchronized(this) {
			activeSockets++;
			connectedAds.add(ad);
		}
		
		new Thread(()-> {
			PeerConnection conn = null;
			try {
				conn = ad.connect(this);
				synchronized(this) { connections.add(conn); }
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
			} catch(Exception exc) {
				logger.error("Caught exception connecting to peer {}", ad, exc);
			} finally {
				if(conn == null) {
					synchronized(this) {
						activeSockets--;
						adEmbargoes.put(ad, Util.currentTimeMillis());
						connectedAds.remove(ad);
					}
				}
			}
		}).start();
	}
	
	public void waitForPage(byte[] tag) {
		long shortTag = Util.shortTag(tag);
		if(currentTags.contains(shortTag)) return;
		
		pageWaitLock.lock();
		if(!pageWaits.containsKey(shortTag)) {
			pageWaits.put(shortTag, pageWaitLock.newCondition());
		}
		
		pageWaits.get(shortTag).awaitUninterruptibly();
		pageWaitLock.unlock();
	}
	
	public ChunkAccumulator accumulatorForTag(byte[] tag) throws IOException {
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
		currentTags.add(shortTag);
		pageWaitLock.lock();
		if(pageWaits.containsKey(shortTag)) {
			pageWaits.get(shortTag).signalAll();
		}
		pageWaitLock.unlock();
		
		announceTag(tag);
	}
	
	protected void announceTag(byte[] tag) {
		long shortTag = Util.shortTag(tag);
		for(PeerConnection connection : connections) {
			connection.announceTag(shortTag);
		}
	}
	
	public void requestTag(byte[] pageTag) {
		requestTag(Util.shortTag(pageTag));
	}
	
	public void requestTag(long shortTag) {
		for(PeerConnection connection : connections) {
			connection.requestPageTag(shortTag);
		}
	}
}
