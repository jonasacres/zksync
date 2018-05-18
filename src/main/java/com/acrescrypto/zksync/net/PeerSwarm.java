package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.net.Blacklist.BlacklistCallback;
import com.acrescrypto.zksync.utility.Util;

public class PeerSwarm implements BlacklistCallback {
	protected ArrayList<PeerConnection> connections = new ArrayList<PeerConnection>();
	protected HashSet<PeerAdvertisement> knownAds = new HashSet<PeerAdvertisement>();
	protected HashSet<PeerAdvertisement> connectedAds = new HashSet<PeerAdvertisement>();
	protected ZKArchiveConfig config;
	protected HashSet<Long> currentTags = new HashSet<Long>();
	protected HashMap<Long,ChunkAccumulator> activeFiles = new HashMap<Long,ChunkAccumulator>();
	protected HashMap<Long,Condition> pageWaits = new HashMap<Long,Condition>();
	protected Lock pageWaitLock = new ReentrantLock();
	protected Logger logger = LoggerFactory.getLogger(PeerSwarm.class);
	protected boolean closed;
	
	int maxSocketCount = 128;
	int maxPeerListSize = 1024;
	
	public PeerSwarm(ZKArchiveConfig config) {
		this.config = config;
		this.config.getAccessor().getMaster().getBlacklist().addCallback(this);
	}
	
	public synchronized void close() {
		closed = true;
		for(PeerConnection connection : connections) {
			connection.close();
		}
		
		pageWaitLock.lock();
		for(Condition cond : pageWaits.values()) {
			cond.notifyAll();
		}
		pageWaitLock.unlock();
		
		this.config.getAccessor().getMaster().getBlacklist().removeCallback(this);
	}
	
	public ZKArchiveConfig getConfig() {
		return config;
	}
	
	@Override
	public synchronized void disconnectAddress(String address, int durationMs) {
		for(PeerConnection connection : connections) {
			PeerAdvertisement ad = connection.socket.getAd();
			if(connection.socket.matchesAddress(address) || (ad != null && ad.matchesAddress(address))) {
				connection.close();
			}
		}
		
		for(PeerAdvertisement ad : knownAds) {
			if(ad.matchesAddress(address)) {
				knownAds.remove(ad);
			}
		}
	}
	
	public synchronized void openedConnection(PeerConnection connection) {
		if(closed) {
			connection.close();
			return;
		}
		
		PeerAdvertisement ad = connection.socket.getAd();
		if(ad != null) {
			connectedAds.remove(ad);
			connectedAds.add(connection.socket.getAd());
			knownAds.remove(ad);
			knownAds.add(connection.socket.getAd());
		}
		connections.add(connection);
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
				if(ad == null || connections.size() >= maxSocketCount) {
					try {
						TimeUnit.MILLISECONDS.sleep(100);
					} catch(InterruptedException exc) {}
					continue;
				}
				
				try {
					logger.trace("Connecting to address ", ad);
					openConnection(ad);
				} catch(UnsupportedProtocolException exc) {
					logger.info("Skipping unsupported address: " + ad);
				} catch(Exception exc) {
					logger.error("Connection thread caught exception handling address {}", ad, exc);
				}
			}
		}).start();
	}
	
	protected synchronized PeerAdvertisement selectConnectionAd() {
		// TODO P2P: (refactor) maybe keep an unconnectedAds list to make this faster?
		for(PeerAdvertisement ad : knownAds) {
			if(connectedAds.contains(ad)) continue;
			return ad;
		}
		
		return null;
	}
	
	protected synchronized void openConnection(PeerAdvertisement ad) throws UnsupportedProtocolException, IOException, ProtocolViolationException, BlacklistedException {
		// TODO P2P: (refactor) too many exceptions. How do we get a ProtocolViolation just on instantiation?
		if(closed) return;
		connections.add(new PeerConnection(this, ad));
	}
	
	public synchronized void waitForPage(byte[] tag) {
		long shortTag = Util.shortTag(tag);
		if(currentTags.contains(shortTag)) return;
		
		pageWaitLock.lock();
		if(!pageWaits.containsKey(shortTag)) {
			pageWaits.put(shortTag, pageWaitLock.newCondition());
		}
		
		pageWaits.get(shortTag).awaitUninterruptibly();
		pageWaitLock.unlock();
	}
	
	public ChunkAccumulator accumulatorForTag(RefTag tag) throws IOException {
		long shortTag = tag.getShortHash();
		if(!activeFiles.containsKey(shortTag)) {
			int numChunksExpected = (int) Math.ceil((double) config.getPageSize() / PeerMessage.FILE_CHUNK_SIZE);
			ChunkAccumulator fileHandle = new ChunkAccumulator(this, tag.getHash(), numChunksExpected);
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
			pageWaits.get(shortTag).notifyAll();
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
