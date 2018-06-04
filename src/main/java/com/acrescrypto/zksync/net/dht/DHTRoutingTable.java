package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.utility.Util;

public class DHTRoutingTable {
	public final static int DEFAULT_FRESHEN_INTERVAL_MS = 1000*60;
	public static int FRESHEN_INTERVAL_MS = DEFAULT_FRESHEN_INTERVAL_MS;
	
	protected DHTClient client;
	protected ArrayList<DHTBucket> buckets = new ArrayList<>();
	protected boolean closed;
	private ArrayList<DHTPeer> allPeers = new ArrayList<>();
	private Logger logger = LoggerFactory.getLogger(DHTRoutingTable.class);

	public DHTRoutingTable(DHTClient client) {
		this.client = client;
		read();
		new Thread(()->freshenThread()).start();
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
	
	public void close() {
		closed = true;
	}
	
	public void reset() {
		buckets.clear();
		allPeers.clear();
		for(int i = 0; i <= 8*client.idLength(); i++) {
			buckets.add(new DHTBucket(client, i-1));
		}
	}
	
	public void freshen() {
		for(DHTBucket bucket : buckets) {
			if(!bucket.needsFreshening()) continue;
			client.lookup(bucket.randomIdInRange(), (results)->{}); // can ignore results; just doing search freshens routing table
		}
	}
	
	public Collection<DHTPeer> closestPeers(DHTID id, int numPeers) {
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
	
	public Collection<DHTPeer> allPeers() {
		return allPeers;
	}
	
	public void suggestPeer(DHTPeer peer) {
		int order = peer.id.xor(client.id).order();
		DHTBucket bucket = buckets.get(order+1); // add 1 since an exact match has order -1
		if(bucket.hasCapacity()) {
			bucket.add(peer);
			allPeers.add(peer);
			try {
				write();
			} catch (IOException exc) {
				logger.error("Encountered exception writing routing table after receiving new peer", exc);
			}
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
	
	protected void removedPeer(DHTPeer peer) {
		allPeers.remove(peer);
	}
	
	protected void freshenThread() {
		while(!closed) {
			try {
				Util.sleep(FRESHEN_INTERVAL_MS);
				freshen();
			} catch(Exception exc) {
				logger.error("DHT routing table freshen thread encountered exception", exc);
			}
		}
	}
	
	protected String path() {
		return "dht-routing-table";
	}
	
	protected void write() throws IOException {
		MutableSecureFile file = MutableSecureFile.atPath(client.storage, path(), client.routingTableKey());
		file.write(serialize(), 0);
	}
	
	protected void read() {
		reset();
		
		MutableSecureFile file = MutableSecureFile.atPath(client.storage, path(), client.routingTableKey());
		try {
			deserialize(ByteBuffer.wrap(file.read()));
		} catch(IOException|SecurityException exc) {
		}
	}
	
	protected byte[] serialize() {
		LinkedList<byte[]> pieces = new LinkedList<>();
		int totalLength = 0;
		
		for(DHTPeer peer : allPeers) {
			byte[] piece = peer.serialize();
			pieces.add(piece);
			totalLength += piece.length;
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
				suggestPeer(peer);
			}
		} catch(BufferUnderflowException exc) {
			throw new EINVALException(path());
		}
	}
}
