package com.acrescrypto.zksync.net.dht;

import java.util.ArrayList;
import java.util.Collection;

public class DHTRoutingTable {
	protected ArrayList<DHTBucket> buckets = new ArrayList<>();
	private ArrayList<DHTPeer> allPeers = new ArrayList<>();
	
	public void freshen(DHTPeer peer) {
		for(DHTBucket bucket : buckets) {
			if(bucket.peers.contains(peer)) {
				bucket.refresh();
				break;
			}
		}
	}
	
	public Collection<DHTPeer> allPeers() {
		return allPeers;
	}
	
	public void suggestPeer(DHTPeer peer) {
		for(DHTBucket bucket : buckets) {
			if(bucket.peers.contains(peer)) {
				if(bucket.hasCapacity()) {
					bucket.add(peer);
					if(bucket.canSplit()) {
						splitSelfBucket(bucket);
					}
				}
				
				return;
			}
		}
	}
	
	protected void splitSelfBucket(DHTBucket selfBucket) {
		while(selfBucket.needSplit()) {
			DHTBucket newBucket = selfBucket.split();
			buckets.add(newBucket);
			if(newBucket.canSplit()) {
				selfBucket = newBucket;
			}
		}
	}
	
	protected void write() {
		// TODO DHT: (implement) write routing table (does it even need encryption? hmm...)
	}
	
	protected void read() {
		// TODO DHT: (implement) read routing table
	}
	
	protected byte[] serialize() {
		// TODO DHT: (implement) serialize routing table
		return null;
	}
	
	protected void deserialize(byte[] serialized) {
		// TODO DHT: (implement) deserialize routing table
	}
}
