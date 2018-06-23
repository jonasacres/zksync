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
	long lastChanged = -1;
	
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
		add(peer, Util.currentTimeMillis());
	}
	
	public void add(DHTPeer peer, long lastSeen) {
		assert(hasCapacity());
		
		if(peers.size() >= MAX_BUCKET_CAPACITY) {
			prune();
		}
		
		peers.add(peer);
		markFresh(lastSeen);
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
		markFresh(Util.currentTimeMillis());
	}
	
	public void markFresh(long timestamp) {
		lastChanged = Math.max(lastChanged, timestamp);
	}
	
	public boolean needsFreshening() {
		return lastChanged >= 0 && Util.currentTimeMillis() - lastChanged >= BUCKET_FRESHEN_INTERVAL_MS;
	}
	
	protected void prune() {
		for(DHTPeer peer : peers) {
			if(peer.isBad()) {
				peers.remove(peer);
				client.routingTable.removedPeer(peer);
				return;
			}
		}
		
		DHTPeer stalest = Util.min(peers, (a,b)->Long.compare(a.lastSeen, b.lastSeen));
		if(stalest != null && stalest.isQuestionable()) {
			stalest.ping();
		}
	}
}
