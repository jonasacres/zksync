package com.acrescrypto.zksync.net.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

public class DHTSearchOperation {
	interface SearchOperationCallback {
		void searchOperationFinished(Collection<DHTPeer> closestPeers);
	}
	
	public final static int MAX_RESULTS = 8;
	public final static int SEARCH_QUERY_TIMEOUT_MS = 5000;
	
	int activeQueries = 0;
	
	DHTID searchId;
	DHTClient client;
	HashSet<DHTPeer> queried = new HashSet<DHTPeer>();
	ArrayList<DHTPeer> closestPeers = new ArrayList<DHTPeer>(MAX_RESULTS);
	SearchOperationCallback callback;
	
	DHTPeer weakestPeer;
	DHTID weakestDistance;
	
	public DHTSearchOperation(DHTClient client, DHTID searchId, SearchOperationCallback callback) {
		this.client = client;
		this.searchId = searchId;
		this.callback = callback;
	}
	
	public void run() {
		for(DHTPeer peer : client.routingTable.allPeers()) {
			addIfBetter(peer);
		}
		
		for(DHTPeer peer : closestPeers) {
			requestNodes(peer);
		}
	}
	
	protected void addIfBetter(DHTPeer peer) {
		DHTID distance = peer.id.xor(searchId);
		
		if(closestPeers.size() >= MAX_RESULTS) {
			if(distance.compareTo(weakestDistance) >= 0) return;
			closestPeers.remove(weakestPeer);
		}
		
		closestPeers.add(peer);
		recalculateWeakest();
	}
	
	protected void recalculateWeakest() {
		weakestDistance = null;
		weakestPeer = null;
		
		for(DHTPeer peer : closestPeers) {
			DHTID dist = peer.id.xor(searchId);
			if(weakestDistance == null || dist.compareTo(weakestDistance) > 0) {
				weakestDistance = dist;
				weakestPeer = peer;
			}
		}
	}
	
	protected synchronized void requestNodes(DHTPeer peer) {
		queried.add(peer);
		peer.findNode(searchId, (peers, isFinal)->handleFindNodeResults(peers, isFinal));
	}
	
	protected synchronized void handleFindNodeResults(Collection<DHTPeer> peers, boolean isFinal) {
		if(isFinal) finishedQuery();
		if(peers == null) return;
		
		for(DHTPeer peer : peers) {
			addIfBetter(peer);
		}
		
		for(DHTPeer peer : peers) {
			if(closestPeers.contains(peer) && !queried.contains(peer)) {
				requestNodes(peer);
			}
		}
	}
	
	protected synchronized void finishedQuery() {
		activeQueries--;
		if(activeQueries == 0) {
			callback.searchOperationFinished(closestPeers);
		}
	}
}
