package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.utility.Util;

public class DHTRoutingTable {
	protected DHTClient client;
	protected ArrayList<DHTBucket> buckets = new ArrayList<>();
	protected PriorityQueue<RecentPeer> recent = new PriorityQueue<>();
	protected boolean closed;
	protected ArrayList<DHTPeer> allPeers = new ArrayList<>();
	private Logger logger = LoggerFactory.getLogger(DHTRoutingTable.class);
	
	public class RecentPeer implements Comparable<RecentPeer> {
		long timestamp;
		DHTPeer peer;
		
		public RecentPeer(DHTPeer peer) {
			this.peer = peer;
			refresh();
		}
		
		public void refresh() {
			this.timestamp = Util.currentTimeMillis();
		}
		
		@Override
		public boolean equals(Object o) {
			if(!(o instanceof RecentPeer)) return false;
			return peer.equals(((RecentPeer) o).peer);
		}
		
		@Override
		public int compareTo(RecentPeer o) {
			return Long.signum(timestamp - o.timestamp);
		}
	}

	public DHTRoutingTable(DHTClient client) {
		this.client = client;
		read();
		new Thread(client.threadGroup, ()->freshenThread()).start();
	}
	
	protected DHTRoutingTable() {}
	
	public void markFresh(DHTPeer peer) {
		for(DHTBucket bucket : buckets) {
			if(bucket.peers.contains(peer)) {
				bucket.markFresh();
				break;
			}
		}
	}
	
	public synchronized void close() {
		closed = true;
		this.notifyAll();
	}
	
	public synchronized void reset() {
		int len = client.idLength();
		
		buckets .clear();
		allPeers.clear();
		buckets .add(
				new DHTBucket(
						this,
						DHTID.zero(len),
						DHTID.max (len)));
	}
	
	public synchronized void freshen() {
		if(closed || !client.getSocketManager().isListening()) return;
		for(DHTBucket bucket : buckets) {
			bucket.prune();
			if(!bucket.needsFreshening()) continue;
			client.getProtocolManager().lookup(
					bucket.randomIdInRange(),
					new Key(client.crypto),
					(results)->{}               // can ignore results; just doing search freshens routing table
				);
		}
	}
	
	public synchronized Collection<DHTPeer> closestPeers(DHTID id, int numPeers) {
		DHTID greatestDistance = null;
		DHTPeer mostDistantPeer = null;
		ArrayList<DHTPeer> closest = new ArrayList<>(numPeers);
		
		for(DHTPeer peer : allPeers()) {
			DHTID distance = peer.id.xor(id);
			if(closest.size() < numPeers || distance.compareTo(greatestDistance) <= 0) {
				if(closest.size() >= numPeers) {
					closest.remove(mostDistantPeer);
				}
				
				closest.add(peer);
				greatestDistance = null;
				for(DHTPeer listed : closest) {
					DHTID listedDistance = listed.id.xor(id);
					if(greatestDistance == null || listedDistance.compareTo(greatestDistance) > 0) {
						greatestDistance = listedDistance;
						mostDistantPeer = listed;
					}
				}
			}
		}
		
		return closest;
	}
	
	public synchronized Collection<DHTPeer> allPeers() {
		return new ArrayList<>(allPeers);
	}
	
	public boolean suggestPeer(DHTPeer peer) {
		return suggestPeer(peer, Util.currentTimeMillis());
	}
	
	public synchronized boolean suggestPeer(DHTPeer peer, long lastSeen) {
		if(peer.id.equals(client.getId())) return false;      // we don't need an entry for ourselves!
		for(DHTPeer existing : allPeers) {
			if(   existing.id     .equals(peer.id)
			   && existing.address.equals(peer.address)
			   && existing.port        == peer.port)
			{
				return true;                                  // already have this peer
			}
		}
		
		DHTPeer insertablePeer;
		try {
			insertablePeer = new DHTPeer(peer);
		} catch(UnknownHostException exc) {
			return false;
		}
		
		DHTBucket bucket = bucketForId(insertablePeer.id);
		while(bucket.needsSplit()) {
			DHTBucket newBucket = bucket.split();
			buckets.add(newBucket);
			buckets.sort(null);
			
			if(newBucket.includes(insertablePeer.id)) {
				bucket = newBucket;
			}
		}
		
		if(bucket.hasCapacity()) {
			bucket.add(insertablePeer, lastSeen);
			allPeers.add(insertablePeer);
			logger.info("DHT {}:{}: Added peer to routing table, table has {} peers",
					peer.address,
					peer.port,
					allPeers.size());
			try {
				write();
			} catch (IOException exc) {
				logger.error("DHT {}:{} Encountered exception writing routing table after receiving new peer",
						peer.address,
						peer.port,
						exc);
			}
			
			return true;
		} else {
			logger.debug("DHT {}:{}: Relevant bucket too full for peer; ignoring",
					peer.address,
					peer.port);
			return false;
		}
	}
	
	protected DHTBucket bucketForId(DHTID id) {
		for(DHTBucket bucket : buckets) {
			if(bucket.includes(id)) return bucket;
		}
		
		logger.error("DHT -: Unable to find bucket for id {} (this should not be possible)",
				Util.bytesToHex(id.serialize()));
		throw new RuntimeException("Unable to locate bucket for ID");
	}
	
	public DHTPeer peerForMessage(String address, int port, PublicDHKey pubKey) throws UnknownHostException {
		for(RecentPeer rr : recent) {
			if(rr.peer.matches(address, port, pubKey)) {
				rr.refresh();
				return rr.peer;
			}
		}
		
		for(DHTPeer peer : allPeers) {
			if(peer.matches(address, port, pubKey)) {
				refreshRecent(peer);
				return peer;
			}
		}
		
		DHTPeer newPeer = new DHTPeer(client, address, port, pubKey.getBytes());
		suggestPeer(newPeer);
		refreshRecent(newPeer);
		
		return newPeer;
	}
	
	protected void refreshRecent(DHTPeer peer) {
		for(RecentPeer rr : recent) {
			if(!rr.peer.equals(peer)) continue;
			rr.refresh();
			return;
		}
		
		// need to add to list
		int maxCount = client.getMaster().getGlobalConfig().getInt("net.dht.maxRecentPeerQueueSize");
		while(recent.size() >= maxCount) {
			recent.poll();
		}
		
		RecentPeer rr = new RecentPeer(peer);
		recent.add(rr);
	}
	
	public synchronized void dump() {
		System.out.println("\tRouting table: " + allPeers.size() + " entries");
		for(DHTPeer peer : allPeers()) {
			System.out.println("\t\t " + Util.bytesToHex(peer.key.getBytes(), 4) + " " + peer.address + ":" + peer.port + " " + peer.lastSeen);
		}
	}
	
	protected void removedPeer(DHTPeer peer) {
		allPeers.remove(peer);
		logger.info("DHT {}:{}: Removed peer, key={}, table has {} peers",
				peer.address,
				peer.port,
				Util.bytesToHex(peer.key.getBytes()),
				allPeers.size());
	}
	
	protected void freshenThread() {
		Util.setThreadName("DHTRoutingTable freshen thread");
		while(!closed) {
			try {
				int freshenIntervalMs = client.getMaster().getGlobalConfig().getInt("net.dht.freshenIntervalMs");
				synchronized(this) { if(!closed) this.wait(freshenIntervalMs); }
				if(!closed) {
					freshen();
				}
			} catch(Exception exc) {
				if(closed) {
					logger.info("DHT -: routing table freshen thread encountered exception after close", exc);
				} else {
					logger.error("DHT -: routing table freshen thread encountered exception", exc);
				}
			}
		}
	}
	
	protected String path() {
		return "dht-routing-table";
	}
	
	protected void write() throws IOException {
		MutableSecureFile file = MutableSecureFile.atPath(client.getStorage(), path(), client.routingTableKey());
		file.write(serialize(), 0);
	}
	
	protected void read() {
		reset();
		
		MutableSecureFile file = MutableSecureFile.atPath(client.getStorage(), path(), client.routingTableKey());
		try {
			deserialize(ByteBuffer.wrap(file.read()));
		} catch(IOException|SecurityException exc) {
		}
	}
	
	protected synchronized byte[] serialize() {
		LinkedList<byte[]> pieces = new LinkedList<>();
		int totalLength = 0;
		
		for(DHTPeer peer : allPeers) {
			byte[] piece = peer.serialize();
			pieces.add(piece);
			pieces.add(Util.serializeLong(peer.lastSeen));
			totalLength += piece.length + 8;
		}
		
		ByteBuffer buf = ByteBuffer.allocate(4+totalLength);
		buf.putInt((short) pieces.size());
		for(byte[] piece : pieces) {
			buf.put(piece);
		}

		assert(buf.remaining() == 0);
		return buf.array();
	}
	
	protected void deserialize(ByteBuffer serialized) throws EINVALException {
		try {
			int numPeers = serialized.getInt();
			for(int i = 0; i < numPeers; i++) {
				DHTPeer peer = new DHTPeer(client, serialized);
				long lastSeen = serialized.getLong();
				suggestPeer(peer, lastSeen);
			}
		} catch(BufferUnderflowException exc) {
			throw new EINVALException(path());
		}
	}

	public void verifiedPeer(DHTPeer peer) {
		/* This peer has demonstrated that it can receive messages at its apparent address.
		 * Therefore, we can prune any other peers that have the same public key, since they
		 * are presumably the same peer at a previous IP and/or port. We can also discard
		 * any peers with different public keys at the indicated IP and port. */
		for(DHTBucket bucket : buckets) {
			bucket.pruneToVerifiedPeer(peer);
		}
	}

	public DHTPeer canonicalPeer(DHTPeer peer) {
		for(RecentPeer rr : recent) {
			if(peer.equals(rr.peer)) {
				rr.refresh();
				return rr.peer;
			}
		}
		
		for(DHTPeer existing : allPeers) {
			if(existing.equals(peer)) return existing;
		}
		
		refreshRecent(peer);
		suggestPeer(peer);
		return peer;
	}
	
	public long bucketFreshenInterval() {
		return client.getMaster().getGlobalConfig().getLong("net.dht.bucketFreshenIntervalMs");
	}

	public int maxBucketCapacity() {
		return client.getMaster().getGlobalConfig().getInt("net.dht.bucketMaxCapacity");
	}

	protected Collection<DHTBucket> buckets() {
		return buckets;
	}

	public DHTClient getClient() {
		return client;
	}
}
