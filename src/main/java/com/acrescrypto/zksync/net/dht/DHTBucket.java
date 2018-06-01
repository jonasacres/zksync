package com.acrescrypto.zksync.net.dht;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.acrescrypto.zksync.utility.Util;

public class DHTBucket {
	public final static int MAX_BUCKET_CAPACITY = 8;
	public final static int BUCKET_FRESHEN_INTERVAL_MS = 1000*60*15;
	
	DHTClient client;
	int order;
	ArrayList<DHTPeer> peers = new ArrayList<DHTPeer>(MAX_BUCKET_CAPACITY);
	long lastChanged;
	
	public DHTBucket(DHTClient client, int order) {
		this.client = client;
		this.order = order;
	}
	
	public boolean hasCapacity() {
		if(peers.size() < MAX_BUCKET_CAPACITY) return true;
		for(DHTPeer peer: peers) {
			if(peer.isBad()) return true;
		}
		
		return false;
	}
	
	public boolean includes(DHTID id) {
		return order == client.id.xor(id).order();
	}
	
	public void add(DHTPeer peer) {
		assert(hasCapacity());
		
		if(peers.size() >= MAX_BUCKET_CAPACITY) {
			prune();
		}
		
		peers.add(peer);
		markFresh();
	}
	
	// returns a random ID whose distance from the client ID is of the order of this bucket
	public DHTID randomIdInRange() {
		if(order == -1) return client.id;
		byte[] random = client.id.serialize();
		int pivotByteIndex = random.length - order/8 - 1;
		int pivotBitOffset = order % 8;
		
		byte pivotBitMask = (byte) (1 << pivotBitOffset);
		byte pivotLowerMask = (byte) (pivotBitMask-1);
		byte pivotUpperMask = (byte) ~(pivotBitMask | pivotLowerMask);
		
		byte randomBits = (byte) (client.crypto.rng(1)[0] & pivotLowerMask);
		byte flippedBit = (byte) ((pivotBitMask ^ random[pivotByteIndex]) & pivotBitMask);
		byte preservedBits = (byte) (random[pivotByteIndex] & pivotUpperMask);
		byte pivotByte = (byte) (preservedBits | flippedBit | randomBits);
		
		ByteBuffer buf = ByteBuffer.wrap(random);
		buf.position(pivotByteIndex);
		buf.put(pivotByte);
		buf.put(client.crypto.rng(buf.remaining()));
		
		return new DHTID(random);
	}
	
	public void markFresh() {
		lastChanged = Util.currentTimeMillis();
	}
	
	// TODO DHT: (design) Need to actually ping stale peers...
	
	public boolean needsFreshening() {
		// TODO DHT: (review) Consider only returning true if we've ever had contents in this bucket.
		// otherwise, we'll be doing 512 searches right out the gate at initialization!
		
		return Util.currentTimeMillis() - lastChanged >= BUCKET_FRESHEN_INTERVAL_MS;
	}
	
	protected void prune() {
		for(DHTPeer peer : peers) {
			if(peer.isBad()) {
				peers.remove(peer);
				client.routingTable.removedPeer(peer);
				return;
			}
		}
	}
}
