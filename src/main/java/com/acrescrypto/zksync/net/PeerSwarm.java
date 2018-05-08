package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.ChunkableFileHandle;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.utility.Util;

public class PeerSwarm {
	protected ArrayList<PeerConnection> connections = new ArrayList<PeerConnection>();
	protected HashSet<String> knownPeers = new HashSet<String>();
	protected HashSet<String> connectedAddresses = new HashSet<String>();
	protected ArrayList<PeerDiscoveryApparatus> discoveryApparatuses = new ArrayList<PeerDiscoveryApparatus>(); // It's "apparatuses." I looked it up.
	protected ZKArchiveConfig config;
	protected HashSet<Long> currentTags = new HashSet<Long>();
	protected HashMap<Long,ChunkableFileHandle> activeFiles = new HashMap<Long,ChunkableFileHandle>();
	protected HashMap<Long,Condition> pageWaits = new HashMap<Long,Condition>();
	protected Lock pageWaitLock = new ReentrantLock();
	
	int maxSocketCount = 128;
	int maxPeerListSize = 1024;
	
	public PeerSwarm(ZKArchiveConfig config) {
		this.config = config;
	}
	
	public ZKArchiveConfig getConfig() {
		return config;
	}
	
	public synchronized void openedConnection(PeerConnection connection) {
		connectedAddresses.add(connection.socket.getAddress());
		knownPeers.add(connection.socket.getAddress());
		connections.add(connection);
	}
	
	public synchronized void addPeer(String address) {
		if(knownPeers.size() >= maxPeerListSize) return;
		if(!PeerSocket.addressSupported(address)) return;
		
		knownPeers.add(address);
	}
	
	public synchronized void addApparatus(PeerDiscoveryApparatus apparatus) {
		discoveryApparatuses.add(apparatus);
	}
	
	public void connectionThread() {
		new Thread(() -> {
			while(true) {
				String addr = selectConnectionAddress();
				try {
					if(addr == null || connections.size() >= maxSocketCount) {
						TimeUnit.MILLISECONDS.sleep(100);
						continue;
					}
				} catch(InterruptedException exc) {}
				
				try {
					openConnection(addr);
				} catch(UnsupportedProtocolException exc) {
					connectionFailed(addr);
				}
			}
		}).start();
	}
	
	public void discoveryThread() {
		new Thread(() -> {
			while(true) {
				for(PeerDiscoveryApparatus apparatus : discoveryApparatuses) {
					for(String address : apparatus.discoveredPeers(config.getArchive())) {
						addPeer(address);
					}
				}
				
				try {
					TimeUnit.MILLISECONDS.sleep(100);
				} catch (InterruptedException e) {}
			}
		}).start();
	}
	
	public synchronized String selectConnectionAddress() {
		for(String peer : knownPeers) {
			if(connectedAddresses.contains(peer)) continue;
			return peer;
		}
		
		return null;
	}
	
	public synchronized void openConnection(String address) throws UnsupportedProtocolException {
		connections.add(new PeerConnection(this, address));
	}
	
	public synchronized void connectionFailed(String address) {
		connectedAddresses.remove(address);
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
	
	public ChunkableFileHandle fileHandleForTag(RefTag tag) throws IOException {
		long shortTag = tag.getShortHash();
		if(!activeFiles.containsKey(shortTag)) {
			String path = Page.pathForTag(tag.getHash());
			ChunkableFileHandle fileHandle = new ChunkableFileHandle(config.getStorage(), path, config.getPageSize(), PeerMessage.FILE_CHUNK_SIZE);
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
	
	public void announceTag(byte[] tag) {
		long shortTag = Util.shortTag(tag);
		for(PeerConnection connection : connections) {
			connection.announceTag(shortTag);
		}
	}
	
	protected void receivedTips(byte[] tipsFile) {
		// TODO P2P: (implement)
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
