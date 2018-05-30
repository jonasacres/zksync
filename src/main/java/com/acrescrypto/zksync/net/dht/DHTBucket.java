package com.acrescrypto.zksync.net.dht;

import java.util.ArrayList;

import com.acrescrypto.zksync.utility.Util;

public class DHTBucket {
	public final static int MAX_BUCKET_CAPACITY = 8;
	
	DHTClient client;
	DHTID min, max;
	ArrayList<DHTPeer> peers = new ArrayList<DHTPeer>(MAX_BUCKET_CAPACITY);
	long lastChanged;
	
	public DHTBucket(DHTClient client, DHTID min, DHTID max) {
		this.client = client;
		this.min = min;
		this.max = max;
	}
	
	public boolean hasCapacity() {
		if(peers.size() < MAX_BUCKET_CAPACITY || canSplit());
		for(DHTPeer peer: peers) {
			if(peer.isBad()) return true;
		}
		
		return false;
	}
	
	public boolean canSplit() {
		return includes(client.id);
	}
	
	public boolean includes(DHTID id) {
		return min.compareTo(id) <= 0 && id.compareTo(max) <= 0;
	}
	
	public void add(DHTPeer peer) {
		assert(hasCapacity());
		
		if(peers.size() >= MAX_BUCKET_CAPACITY) {
			prune();
		}
		
		peers.add(peer);
		refresh();
	}
	
	public boolean needSplit() {
		return peers.size() >= MAX_BUCKET_CAPACITY;
	}
	
	public DHTID randomIdInRange() {
		// TODO DHT: (implement) get a random id greater than min and less than max
		return null;
	}
	
	public void refresh() {
		lastChanged = Util.currentTimeMillis();
	}
	
	public DHTBucket split() {
		DHTID mid = max.calculateMidpoint(min);
		DHTBucket bucket = new DHTBucket(client, min, mid);
		min = mid;
		
		ArrayList<DHTPeer> newPeers = new ArrayList<DHTPeer>(MAX_BUCKET_CAPACITY);
		for(DHTPeer peer : peers) {
			if(bucket.includes(peer.id)) {
				bucket.add(peer);;
			} else {
				newPeers.add(peer);
			}
		}
		
		this.peers = newPeers;
		return bucket;
	}
	
	protected void prune() {
		for(DHTPeer peer : peers) {
			if(peer.isBad()) {
				peers.remove(peer);
				return;
			}
		}
	}
}
