package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

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
	protected boolean closed;
	protected ArrayList<DHTPeer> allPeers = new ArrayList<>();
	private Logger logger = LoggerFactory.getLogger(DHTRoutingTable.class);

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
		buckets.clear();
		allPeers.clear();
		for(int i = 0; i <= 8*client.idLength(); i++) {
			buckets.add(new DHTBucket(client, i-1));
		}
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
	
	public void suggestPeer(DHTPeer peer) {
		suggestPeer(peer, Util.currentTimeMillis());
	}
	
	public synchronized void suggestPeer(DHTPeer peer, long lastSeen) {
		if(peer.id.equals(client.getId())) return;            // we don't need an entry for ourselves!
		for(DHTPeer existing : allPeers) {
			if(existing.id.equals(peer.id) && existing.address.equals(peer.address) && existing.port == peer.port) {
				return;                                       // already have this peer
			}
		}
		
		DHTPeer insertablePeer = new DHTPeer(client, peer.address, peer.port, peer.key);
		insertablePeer.id      = new DHTID(peer.id.rawId.clone()); // some tests hijack this field, so we'll respect that
		insertablePeer.setPinned(peer.isPinned());
		
		int order        = insertablePeer.id.xor(client.id).order();
		DHTBucket bucket = buckets.get(order+1); // add 1 since an exact match has order -1
		
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
		} else {
			logger.debug("DHT {}:{}: Relevant bucket too full for peer; ignoring",
					peer.address,
					peer.port);
		}
	}
	
	public DHTPeer peerForMessage(String address, int port, PublicDHKey pubKey) {
		for(DHTPeer peer : allPeers) {
			if(peer.matches(address, port, pubKey)) return peer;
		}
		
		DHTPeer newPeer = new DHTPeer(client, address, port, pubKey.getBytes());
		suggestPeer(newPeer);
		return newPeer;
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
}
