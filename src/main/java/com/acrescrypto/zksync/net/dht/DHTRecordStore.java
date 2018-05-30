package com.acrescrypto.zksync.net.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;

import com.acrescrypto.zksync.utility.Util;

public class DHTRecordStore {
	public final static int MAX_RECORDS_PER_ID = 64;
	public final static int MAX_IDS = 64;
	public final static int EXPIRATION_TIME_MS = 1000*60*60*4; // entries cannot be pruned until they are at least 4 hours old
	
	class StoreEntry {
		DHTRecord record;
		long expirationTime;
		
		public StoreEntry(DHTRecord record) {
			this.record = record;
			this.expirationTime = Util.currentTimeMillis() + EXPIRATION_TIME_MS;
		}
		
		public boolean isExpired() {
			return Util.currentTimeMillis() >= this.expirationTime;
		}
	}
	
	protected HashMap<DHTID,ArrayList<StoreEntry>> entriesById;
	
	public void addRecordForId(DHTID id, DHTRecord record) {
		// TODO DHT: (implement) Consider spinning up a thread and requiring a TCP connect to this address to prove that it is accessible.
		if(!entriesById.containsKey(id)) {
			if(entriesById.size() >= MAX_IDS) prune();
			if(entriesById.size() >= MAX_IDS) return;
			entriesById.put(id, new ArrayList<StoreEntry>(MAX_RECORDS_PER_ID));
		}
		
		ArrayList<StoreEntry> entriesForId = entriesById.get(id);
		if(entriesForId.size() >= MAX_RECORDS_PER_ID) prune();
		if(entriesForId.size() >= MAX_RECORDS_PER_ID) return;
		entriesForId.add(new StoreEntry(record));
	}
	
	public Collection<DHTRecord> recordsForId(DHTID id) {
		LinkedList<DHTRecord> records = new LinkedList<>();
		Collection<StoreEntry> entries = entriesById.getOrDefault(id, null);
		
		for(StoreEntry entry : entries) {
			records.add(entry.record);
		}
		
		return records;
	}
	
	protected void write() {
		// TODO DHT: (implement) write record store to disk
	}
	
	protected void read() {
		// TODO DHT: (implement) read record store fro disk
		prune();
	}
	
	protected byte[] serialize() {
		// TODO DHT: (implement) serialize record store;
		return null;
	}
	
	protected void deserialize(byte[] serialized) {
		// TODO DHT: (implement) deserialize record store
	}
	
	protected void prune() {
		boolean dirty = false;
		LinkedList<DHTID> removeLists = new LinkedList<DHTID>();
		for(DHTID id : entriesById.keySet()) {
			ArrayList<StoreEntry> entriesForId = entriesById.get(id);
			dirty |= entriesForId.removeIf((entry)->entry.isExpired());
			if(entriesForId.size() == 0) {
				removeLists.add(id);
			}
		}
		
		dirty |= !removeLists.isEmpty();
		for(DHTID id : removeLists) {
			entriesById.remove(id);
		}
		
		if(dirty) {
			write();
		}
	}
}
