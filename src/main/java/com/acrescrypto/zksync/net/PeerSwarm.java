package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.bouncycastle.util.Arrays;
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
import com.acrescrypto.zksync.fs.zkfs.ObfuscatedRefTag;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.net.Blacklist.BlacklistCallback;
import com.acrescrypto.zksync.utility.Util;

public class PeerSwarm implements BlacklistCallback {
	public final static int EMBARGO_EXPIRE_TIME_MILLIS = 1000*60*10; // wait 10 minutes before retrying consistently unconnectable ads
	public final static int EMBARGO_SOFT_EXPIRE_TIME_MILLIS = 1000; // wait 1s before retrying an ad before it is classified as consistently unconnectable
	public final static int EMBARGO_FAIL_COUNT_THRESHOLD = 3; // how many times do we try an ad before deeming it consistently unconnectable?
	
	protected ArrayList<PeerConnection> connections = new ArrayList<PeerConnection>();
	protected HashSet<PeerAdvertisement> knownAds = new HashSet<PeerAdvertisement>();
	protected HashSet<PeerAdvertisement> connectedAds = new HashSet<PeerAdvertisement>();
	protected ZKArchiveConfig config;
	protected HashMap<Long,ChunkAccumulator> activeFiles = new HashMap<Long,ChunkAccumulator>();
	protected HashMap<Long,Condition> pageWaits = new HashMap<Long,Condition>();
	protected HashMap<PeerAdvertisement,Long> adEmbargoes = new HashMap<PeerAdvertisement,Long>();
	protected RequestPool pool;
	protected PrivateDHKey identityKey;
	
	protected Lock pageWaitLock = new ReentrantLock();
	protected Logger logger = LoggerFactory.getLogger(PeerSwarm.class);
	
	protected boolean closed;
	protected int activeSockets;
	
	int maxSocketCount = 128;
	int maxPeerListSize = 1024;
	
	public PeerSwarm(ZKArchiveConfig config) throws IOException {
		this.config = config;
		this.config.getAccessor().getMaster().getBlacklist().addCallback(this);
		connectionThread();
		initIdentity(); // TODO DHT: (test) PeerSwarm identity key init, file read/write
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
			for(Condition cond : pageWaits.values()) {
				cond.signalAll();
			}
			pageWaitLock.unlock();
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
	
	public synchronized void openedConnection(PeerConnection connection) {
		if(closed) {
			connection.close();
			return;
		}
		
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
		
		PeerConnection existing;
		while((existing = connectionForKey(connection.socket.remoteIdentityKey)) != null && existing != connection) {
			if(Arrays.compareUnsigned(identityKey.publicKey().getBytes(), connection.socket.remoteIdentityKey.getBytes()) < 0) {
				break; // whoever has the higher public key closes the connection
			}
			existing.close();
			connections.remove(existing); // may have already been closed, so make sure it's removed
		}
		
		pool.addRequestsToConnection(connection);
		connection.announcePeers(knownAds);
		connections.add(connection);
	}
	
	public synchronized void closedConnection(PeerConnection connection) {
		activeSockets--;
		connections.remove(connection);
	}
	
	public synchronized void addPeerAdvertisement(PeerAdvertisement ad) {
		// This could be improved. Once we hit capacity, how can we prune ads for low-quality peers for higher-quality ones?
		if(ad instanceof TCPPeerAdvertisement) { // ignore our own ad
			TCPPeerAdvertisement tcpAd = (TCPPeerAdvertisement) ad;
			if(tcpAd.pubKey.equals(identityKey.publicKey())) return;
		}
		
		if(knownAds.size() >= maxPeerListSize) return;
		if(!PeerSocket.adSupported(ad)) return;
		
		if(!knownAds.contains(ad)) {
			announcePeer(ad);
		}
		
		knownAds.remove(ad);
		knownAds.add(ad);
	}
	
	public synchronized void advertiseSelf(PeerAdvertisement ad) {
		for(PeerConnection connection : getConnections()) {
			connection.announceSelf(ad);
		}
	}
	
	public synchronized Collection<PeerConnection> getConnections() {
		return new ArrayList<>(connections);
	}
	
	protected void connectionThread() {
		new Thread(() -> {
			Thread.currentThread().setName("PeerSwarm connection thread");
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
			if(connection.socket.remoteIdentityKey != null && Arrays.areEqual(connection.socket.remoteIdentityKey.getBytes(), key.getBytes())) {
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
		
		new Thread(()-> {
			Thread.currentThread().setName("PeerSwarm openConnection thread");
			PeerConnection conn = null;
			try {
				synchronized(this) {
					conn = ad.connect(this);
					openedConnection(conn);
				}
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
						int delay = unconnectable ? EMBARGO_EXPIRE_TIME_MILLIS : EMBARGO_SOFT_EXPIRE_TIME_MILLIS;
						
						adEmbargoes.put(ad, Util.currentTimeMillis() + delay);
						connectedAds.remove(ad);
					}
				}
			}
		}).start();
	}
	
	public void waitForPage(byte[] tag) throws ClosedException {
		long shortTag = Util.shortTag(tag);
		if(config.getArchive() != null && config.getArchive().hasPageTag(tag)) return;
		
		pageWaitLock.lock();
		if(!pageWaits.containsKey(shortTag)) {
			pageWaits.put(shortTag, pageWaitLock.newCondition());
		}
		
		pageWaits.get(shortTag).awaitUninterruptibly();
		pageWaitLock.unlock();
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
	
	protected synchronized void receivedConfigInfo() {
		pool.receivedConfigInfo();
	}
	
	protected synchronized void receivedPage(byte[] tag) {
		long shortTag = Util.shortTag(tag);
		activeFiles.remove(shortTag);
		pageWaitLock.lock();
		if(pageWaits.containsKey(shortTag)) {
			pageWaits.get(shortTag).signalAll();
		}
		pageWaitLock.unlock();
		if(config.getArchive() != null) {
			config.getArchive().addPageTag(tag);
		}
		
		announceTag(tag);
	}
	
	public void announceTag(byte[] tag) {
		long shortTag = Util.shortTag(tag);
		for(PeerConnection connection : getConnections()) {
			connection.announceTag(shortTag);
		}
	}
	
	public void announceTip(ObfuscatedRefTag tip) {
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
	
	public void requestInode(int priority, RefTag revTag, long inodeId) {
		pool.addInode(priority, revTag, inodeId);
	}
	
	public void requestRevision(int priority, RefTag revTag) {
		pool.addRevision(priority, revTag);
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
			try {
				this.identityKey = config.getCrypto().makePrivateDHKey();
				storedFile().write(serialize(), 0);
			} catch (IOException e) {
				logger.error("Caught exception writing advertisement for archive {}", Util.bytesToHex(config.getArchiveId()), exc);
			}
		} catch (IOException exc) {
			logger.error("Caught exception opening stored advertisement for archive {}", Util.bytesToHex(config.getArchiveId()), exc);
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
	
	protected void dumpConnections() {
		System.out.println("PeerSwarm: archive ID " + Util.bytesToHex(config.getArchiveId()));
		System.out.println("\tIdentity: " + Util.bytesToHex(identityKey.publicKey().getBytes()));
		System.out.println("\tConnections: " + connections.size());
		int i = 0;
		for(PeerConnection connection : connections) {
			System.out.println("\t\tConnection " + (i++) + ": " + Util.bytesToHex(connection.socket.remoteIdentityKey.getBytes()) + " " + (connection.socket.isLocalRoleClient() ? "client" : "server"));
		}
	}
}
