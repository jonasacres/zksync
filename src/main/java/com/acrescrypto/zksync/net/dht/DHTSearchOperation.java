package com.acrescrypto.zksync.net.dht;

import java.util.Collection;
import java.util.HashSet;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;

public class DHTSearchOperation {
	interface SearchOperationPeerCallback {
		void searchOperationFinished(Collection<DHTPeer> closestPeers);
	}
	
	interface SearchOperationRecordCallback {
		void searchOperationDiscoveredRecord(DHTRecord record);
	}
	
	public final static int DEFAULT_MAX_RESULTS = 8;
	public final static int DEFAULT_SEARCH_QUERY_TIMEOUT_MS = 3000;
	
	public static int maxResults = DEFAULT_MAX_RESULTS;
	public static int searchQueryTimeoutMs = DEFAULT_SEARCH_QUERY_TIMEOUT_MS;
	
	int activeQueries = 0;
	
	DHTID searchId;
	DHTClient client;
	HashSet<DHTPeer> queried = new HashSet<DHTPeer>();
	TreeSet<DHTPeer> closestPeers = new TreeSet<>((a,b)->a.id.xor(searchId).compareTo(b.id.xor(searchId)));
	SearchOperationPeerCallback peerCallback;
	SearchOperationRecordCallback recordCallback;
	SnoozeThread timeout;
	Key lookupKey;
	
	private Logger logger = LoggerFactory.getLogger(DHTSearchOperation.class);
	
	public DHTSearchOperation(DHTClient client, DHTID searchId, Key lookupKey, SearchOperationPeerCallback peerCallback, SearchOperationRecordCallback recordCallback) {
		this.client = client;
		this.searchId = searchId;
		this.peerCallback = peerCallback;
		this.recordCallback = recordCallback;
		this.lookupKey = lookupKey;
	}
	
	public synchronized void run() {
		this.timeout = new SnoozeThread(searchQueryTimeoutMs, true, ()->peerCallback.searchOperationFinished(closestPeers));
		logger.debug("DHT: Searching for id {}, routing table has {} peers",
				Util.bytesToHex(searchId.rawId),
				client.routingTable.allPeers().size());
		
		for(DHTPeer peer : client.routingTable.allPeers()) {
			addIfBetter(peer);
		}
		
		if(closestPeers.isEmpty()) {
			peerCallback.searchOperationFinished(closestPeers);
			return;
		}
		
		for(DHTPeer peer : closestPeers) {
			requestNodes(peer);
		}
	}
	
	protected synchronized void addIfBetter(DHTPeer peer) {
		if(closestPeers.contains(peer)) return;
		closestPeers.add(peer);
		
		while(closestPeers.size() > maxResults) {
			closestPeers.pollLast();
		}
	}
	
	protected synchronized void requestNodes(DHTPeer peer) {
		queried.add(peer);
		activeQueries++;
		peer.findNode(searchId, lookupKey, (peers, isFinal)->{
			handleFindNodeResults(peers, isFinal);
		}, (record)->{
			recordCallback.searchOperationDiscoveredRecord(record);
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
