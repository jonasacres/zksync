package com.acrescrypto.zksync.net.dht;

import java.util.Collection;
import java.util.HashSet;
import java.util.TreeSet;

import com.acrescrypto.zksync.utility.SnoozeThread;

public class DHTSearchOperation {
	interface SearchOperationCallback {
		void searchOperationFinished(Collection<DHTPeer> closestPeers);
	}
	
	public final static int DEFAULT_MAX_RESULTS = 8;
	public final static int DEFAULT_SEARCH_QUERY_TIMEOUT_MS = 3000;
	
	public static int MAX_RESULTS = DEFAULT_MAX_RESULTS;
	public static int SEARCH_QUERY_TIMEOUT_MS = DEFAULT_SEARCH_QUERY_TIMEOUT_MS;
	
	int activeQueries = 0;
	
	DHTID searchId;
	DHTClient client;
	HashSet<DHTPeer> queried = new HashSet<DHTPeer>();
	TreeSet<DHTPeer> closestPeers = new TreeSet<>((a,b)->a.id.xor(searchId).compareTo(b.id.xor(searchId)));
	SearchOperationCallback callback;
	SnoozeThread timeout;
	
	public DHTSearchOperation(DHTClient client, DHTID searchId, SearchOperationCallback callback) {
		this.client = client;
		this.searchId = searchId;
		this.callback = callback;
	}
	
	public synchronized void run() {
		this.timeout = new SnoozeThread(SEARCH_QUERY_TIMEOUT_MS, true, ()->callback.searchOperationFinished(closestPeers));
		
		for(DHTPeer peer : client.routingTable.allPeers()) {
			addIfBetter(peer);
		}
		
		if(closestPeers.isEmpty()) {
			callback.searchOperationFinished(closestPeers);
			return;
		}
		
		for(DHTPeer peer : closestPeers) {
			requestNodes(peer);
		}
	}
	
	protected synchronized void addIfBetter(DHTPeer peer) {
		if(closestPeers.contains(peer)) return;
		closestPeers.add(peer);
		
		while(closestPeers.size() > MAX_RESULTS) {
			closestPeers.pollLast();
		}
	}
	
	protected synchronized void requestNodes(DHTPeer peer) {
		queried.add(peer);
		activeQueries++;
		peer.findNode(searchId, (peers, isFinal)->{
			handleFindNodeResults(peers, isFinal);
		});
	}
	
	protected synchronized void handleFindNodeResults(Collection<DHTPeer> peers, boolean isFinal) {
		if(timeout.isCancelled()) return;
		
		if(peers != null) {
			for(DHTPeer peer : peers) {
				addIfBetter(peer);
			}
			
			for(DHTPeer peer : closestPeers) {
				if(!queried.contains(peer)) {
					requestNodes(peer);
				}
			}
		}
		
		if(isFinal) finishedQuery();
	}
	
	protected synchronized void finishedQuery() {
		activeQueries--;
		if(activeQueries == 0) {
			timeout.cancel(); // invokes callback.searchOperationFinished
		} else {
			timeout.snooze();
		}
	}
}
