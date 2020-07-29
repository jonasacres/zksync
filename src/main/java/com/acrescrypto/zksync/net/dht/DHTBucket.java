package com.acrescrypto.zksync.net.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import com.acrescrypto.zksync.utility.Util;

public class DHTBucket implements Comparable<DHTBucket> {
	protected DHTRoutingTable    routingTable;
	protected ArrayList<DHTPeer> peers;
	protected long               lastChanged = -1;
	protected DHTID              min,
	                             max;
	
	public DHTBucket(DHTRoutingTable routingTable, DHTID min, DHTID max) {
		this.routingTable = routingTable;
		this.min          = min;
		this.max          = max;
		this.peers        = new ArrayList<DHTPeer>();
	}
	
	public int maxCapacity() {
		return routingTable.maxBucketCapacity();
	}
	
	public boolean hasCapacity() {
		if(peers.size() < maxCapacity()) return true;
		for(DHTPeer peer: peers) {
			if(peer.isBad()) return true;
		}
		
		return false;
	}
	
	public DHTID min() {
		return min;
	}
	
	public DHTID max() {
		return max;
	}
	
	public boolean needsSplit() {
		if( hasCapacity())            return false;
		if(!includes(routingTable.getClient().getId())) return false;
		
		return true;
	}
	
	public DHTBucket split() {
		DHTID mid = max.midpoint(min);
		DHTID myMin, myMax, newMin, newMax;
		
		// guarantee that this bucket continues to include the client ID
		if(routingTable.getClient().getId().compareTo(mid) < 0) {
			myMin   = min;
			myMax   = mid;
			newMin  = mid;
			newMax  = max;
		} else {
			newMin  = min;
			newMax  = mid;
			myMin   = mid;
			myMax   = max;
		}
		
		ArrayList<DHTPeer> oldPeers    = peers;
		DHTBucket          newBucket   = new DHTBucket(routingTable,
				                                       newMin,
				                                       newMax);
		this.peers                     = new ArrayList<>(maxCapacity());
		this.min                       = myMin;
		this.max                       = myMax;
		
		for(DHTPeer peer : oldPeers) {
			if(includes(peer.getId())) {
				this     .add(peer);
			} else {
				newBucket.add(peer);
			}
		}
		
		return newBucket;
	}
	
	public boolean includes(DHTID id) {
		if(id.compareTo(min) <  0) return false; // id <  minimum
		if(id.compareTo(max) >= 0) return false; // id >= maximum
		return true;
	}
	
	public void add(DHTPeer peer) {
		add(peer, Util.currentTimeMillis());
	}
	
	public void add(DHTPeer peer, long lastSeen) {
		assert(hasCapacity());
		
		if(peers.size() >= maxCapacity()) {
			prune();
		}
		
		peers.add(peer);
		markFresh(lastSeen);
	}
	
	// returns a random ID that would be included in this bucket
	public DHTID randomIdInRange() {
		return max.randomLessThan(min);
	}
	
	protected void setFresh(long timestamp) {
		this.lastChanged = timestamp;
	}
	
	public void markFresh() {
		markFresh(Util.currentTimeMillis());
	}
	
	public void markFresh(long timestamp) {
		lastChanged = Math.max(lastChanged, timestamp);
	}
	
	public boolean needsFreshening() {
		long timeSinceLastChange = Util.currentTimeMillis() - lastChanged;
		long freshenInterval     = routingTable.bucketFreshenInterval();
		
		return peers.size() >  0
			&& timeSinceLastChange >= freshenInterval;
	}
	
	protected void prune() {
		for(DHTPeer peer : peers) {
			if(peer.isBad() && !peer.isPinned()) {
				peers.remove(peer);
				routingTable.removedPeer(peer);
				return;
			}
		}
		
		DHTPeer stalest = Util.min(peers, (a,b)->Long.compare(a.lastSeen, b.lastSeen));
		if(stalest != null && stalest.isQuestionable()) {
			stalest.ping();
		}
	}

	public void pruneToVerifiedPeer(DHTPeer peer) {
		LinkedList<DHTPeer> toPrune = new LinkedList<>();
		for(DHTPeer existing : peers) {
			if(peer == existing) continue;
			boolean samePort =  peer.getPort()    ==     existing.getPort(),
					sameAddr =  peer.getAddress().equals(existing.getAddress()),
					sameKey  =  peer.getKey()    .equals(existing.getKey()),
					sameNet  =  samePort && sameAddr,
					canPrune =  ( sameNet && !sameKey)
					         || (!sameNet &&  sameKey);
			if(canPrune) {
				toPrune.add(existing);
			}
		}
		
		peers.removeIf((p)->toPrune.contains(p));
		for(DHTPeer pruned : toPrune) {
			routingTable.removedPeer(pruned);
		}
	}
	
	public Collection<DHTPeer> peers() {
		return peers;
	}

	@Override
	public int compareTo(DHTBucket o) {
		if(o == this) return 0;
		
		int c = this.min.compareTo(o.min);
		if(c != 0) return c;
		
		// This is an unexpected state, but we'll use max as a secondary sort to resolve ambiguity.
		return this.max.compareTo(o.max);
	}
}
