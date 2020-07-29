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
		void searchOperationFinished(DHTSearchOperation op, Collection<DHTPeer> closestPeers);
	}
	
	interface SearchOperationRecordCallback {
		void searchOperationDiscoveredRecord(DHTRecord record);
	}
	
	int activeQueries = 0;
	
	DHTID searchId;
	DHTClient client;
	HashSet<DHTPeer> queried = new HashSet<DHTPeer>();
	TreeSet<DHTPeer> closestPeers = new TreeSet<>((a,b)->a.id.xor(searchId).compareTo(b.id.xor(searchId)));
	SearchOperationPeerCallback peerCallback;
	SearchOperationRecordCallback recordCallback;
	SnoozeThread timeout;
	Key lookupKey;
	boolean cancelled;
	
	private Logger logger = LoggerFactory.getLogger(DHTSearchOperation.class);
	
	public DHTSearchOperation(DHTClient client, DHTID searchId, Key lookupKey, SearchOperationPeerCallback peerCallback, SearchOperationRecordCallback recordCallback) {
		this.client = client;
		this.searchId = searchId;
		this.peerCallback = peerCallback;
		this.recordCallback = recordCallback;
		this.lookupKey = lookupKey;
	}
	
	public void cancel() {
		cancelled = true;
	}
	
	public boolean isCancelled() {
		return cancelled;
	}
	
	public synchronized DHTSearchOperation run() {
		if(cancelled) return this;
		
		int searchQueryTimeoutMs, maxSearchQueryWaitTimeMs;
		try {
			searchQueryTimeoutMs     = client.getMaster().getGlobalConfig().getInt("net.dht.searchQueryTimeoutMs");
			maxSearchQueryWaitTimeMs = client.getMaster().getGlobalConfig().getInt("net.dht.maxSearchQueryWaitTimeMs");
		} catch(NullPointerException exc) {
			if(cancelled) return this;
			throw exc;
		}
		this.timeout = new SnoozeThread(searchQueryTimeoutMs, maxSearchQueryWaitTimeMs, true, ()->{
			if(cancelled) return;
			peerCallback.searchOperationFinished(this, closestPeers);
		});
		
		logger.debug("DHT -: Searching for id {}, routing table has {} peers",
				Util.bytesToHex(searchId.serialize()),
				client.routingTable.allPeers().size());
		
		for(DHTPeer peer : client.routingTable.allPeers()) {
			addIfBetter(peer);
		}
		
		if(closestPeers.isEmpty()) {
			peerCallback.searchOperationFinished(this, closestPeers);
			return this;
		}
		
		for(DHTPeer peer : closestPeers) {
			requestNodes(peer);
		}
		
		return this;
	}
	
	protected synchronized void addIfBetter(DHTPeer peer) {
		if(closestPeers.contains(peer)) return;
		closestPeers.add(peer);
		
		int maxResults = client.getMaster().getGlobalConfig().getInt("net.dht.maxResults");
		
		while(closestPeers.size() > maxResults) {
			closestPeers.pollLast();
		}
	}
	
	protected synchronized void requestNodes(DHTPeer peer) {
		if(cancelled) return;
		
		queried.add(peer);
		activeQueries++;
		peer.findNode(searchId, lookupKey, (peers, isFinal)->{
			handleFindNodeResults(peers, isFinal);
		}, (record)->{
			if(cancelled) return;
			recordCallback.searchOperationDiscoveredRecord(record);
		});
	}
	
	protected synchronized void handleFindNodeResults(Collection<DHTPeer> peers, boolean isFinal) {
		if(cancelled || timeout.isCancelled()) return;
		
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
		if(cancelled) return;
		
		activeQueries--;
		if(activeQueries == 0) {
			timeout.cancel(); // invokes callback.searchOperationFinished
		} else {
			timeout.snooze();
		}
	}
}
