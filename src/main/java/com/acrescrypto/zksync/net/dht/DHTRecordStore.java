package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.utility.Util;

public class DHTRecordStore {
	public class StoreEntry {
		protected DHTRecord record;
		protected byte[]    token;
		protected long      receivedTime;
		
		public StoreEntry(DHTRecord record, byte[] token) {
			this.record         = record;
			this.token          = token;
			this.receivedTime   = Util.currentTimeMillis();
		}
		
		public StoreEntry(ByteBuffer serialized) throws UnsupportedProtocolException {
			deserialize(serialized);
		}
		
		public DHTRecord record() {
			return record;
		}
		
		public byte[] token() {
			return token;
		}
		
		public long receivedTime() {
			return receivedTime;
		}
		
		public long expirationTime() {
			return receivedTime()
				 + client.getMaster().getGlobalConfig().getLong("net.dht.store.expirationTimeMs");
		}
		
		public boolean isExpired() {
			return Util.currentTimeMillis() >= expirationTime();
		}
		
		public byte[] serialize() {
			byte[] recordSer = record.serialize();
			ByteBuffer buf = ByteBuffer.allocate(
					  8                               // expiration time
					+ 2                               // token length
					+ token.length                    // token
					+ 2                               // record length
					+ recordSer.length                // record
				);
			buf.putLong (        receivedTime    );
			buf.putShort((short) token.length    );
			buf.put     (        token           );
			buf.putShort((short) recordSer.length);
			buf.put     (        recordSer       );
			
			return buf.array();
		}
		
		public void deserialize(ByteBuffer serialized) throws UnsupportedProtocolException {
			this.receivedTime = serialized.getLong();
			int tokenLen = serialized.getShort();
			assert(tokenLen >= 0);
			this.token = new byte[tokenLen];
			serialized.get(token);
			
			int expectedPos = serialized.getShort() + serialized.position();
			
			try {
				record = client.getProtocolManager().deserializeRecord(null, serialized);
				if(serialized.position() != expectedPos) throw new UnsupportedProtocolException();
			} catch (UnsupportedProtocolException exc) {
				exc.printStackTrace();
				logger.error("Record store contained unsupported record", record);
				serialized.position(expectedPos);
				throw exc;
			}
		}
		
		public boolean equals(Object other) {
			if(other instanceof DHTRecord) {
				return record.equals(other);
			}
			
			// TODO API: (refactor) this branch never actually happens in tests, consider if we even need it
			if(other instanceof StoreEntry) {
				return record.equals(((StoreEntry) other).record);
			}
			
			return false;
		}
	}
	
	protected DHTClient client;
	protected HashMap<DHTID,ArrayList<StoreEntry>> entriesById = new HashMap<>();
	private Logger logger = LoggerFactory.getLogger(DHTRecordStore.class);
	
	protected DHTRecordStore() {}

	public DHTRecordStore(DHTClient client) {
		this.client = client;
		read();
	}
	
	public void addRecordForIdBlocking(DHTID id, byte[] token, DHTRecord record) throws IOException {
		if(!hasRoomForRecord(id, record)) return;
		try {
			client.threadPool.submit(()->addRecordIfReachable(id, token, record)).get();
		} catch (InterruptedException | ExecutionException exc) {
			logger.error("Exception waiting for record insertion", exc);
		}
	}
	
	public void addRecordForId(DHTID id, byte[] token, DHTRecord record) throws IOException {
		if(!hasRoomForRecord(id, record)) return;
		client.threadPool.submit(()->addRecordIfReachable(id, token, record));
	}
	
	public synchronized Map<DHTID, Collection<StoreEntry>> records() {
		HashMap<DHTID, Collection<StoreEntry>> map = new HashMap<>();
		for(DHTID id : entriesById.keySet()) {
			LinkedList<StoreEntry> list = new LinkedList<>();
			for(StoreEntry entry : entriesById.get(id)) {
				list.add(entry);
			}
			
			map.put(id, list);
		}
		
		return map;
	}
	
	public synchronized Collection<DHTRecord> recordsForId(DHTID id, byte[] token) {
		LinkedList<DHTRecord> records = new LinkedList<>();
		Collection<StoreEntry> entries = entriesById.getOrDefault(id, new ArrayList<>(0));
		
		for(StoreEntry entry : entries) {
			if(!Util.safeEquals(token, entry.token)) {
				continue;
			}
			
			records.add(entry.record);
		}
		
		return records;
	}
	
	public void dump() {
		System.out.println("\tRecord store: " + entriesById.size() + " keys");
		for(DHTID id : entriesById.keySet()) {
			System.out.println("\t\t" + Util.bytesToHex(id.rawId, 4) + " " + entriesById.get(id).size());
			for(StoreEntry entry : entriesById.get(id)) {
				System.out.println("\t\t\t" + entry.record);
			}
		}
	}

	@SuppressWarnings("unlikely-arg-type")
	protected synchronized boolean hasRoomForRecord(DHTID id, DHTRecord record) throws IOException {
		int maxIds          = client.getMaster().getGlobalConfig().getInt("net.dht.store.maxIds");
		int maxRecordsPerId = client.getMaster().getGlobalConfig().getInt("net.dht.store.maxRecordsPerId");
		
		if(!entriesById.containsKey(id)) {
			if(entriesById.size() >= maxIds) prune();
			if(entriesById.size() >= maxIds) return false;
			return true;
		}
		
		ArrayList<StoreEntry> entriesForId = entriesById.get(id);
		for(StoreEntry entry : entriesForId) {
			if(entry.equals(record)) return false;
		}
		if(entriesForId.size() >= maxRecordsPerId) prune();
		if(entriesForId.size() >= maxRecordsPerId) return false;
		return true;
	}
	
	protected void addRecordIfReachable(DHTID id, byte[] token, DHTRecord record) {
		int maxRecordsPerId = client.getMaster().getGlobalConfig().getInt("net.dht.store.maxRecordsPerId");
		Util.setThreadName("Add record worker");
		try {
			if(!record.isReachable()) {
				logger.info("Ignoring DHT record for non-reachable host {} for ID {}",
						record.routingInfo(),
						Util.bytesToHex(id.rawId));
				return;
			}
			
			synchronized(this) {
				if(!hasRoomForRecord(id, record)) return;
				
				if(!entriesById.containsKey(id)) {
					entriesById.put(id, new ArrayList<StoreEntry>(maxRecordsPerId));
				}
				
				ArrayList<StoreEntry> entriesForId = entriesById.get(id);
				if(entriesForId.size() >= maxRecordsPerId) prune();
				if(entriesForId.size() >= maxRecordsPerId) return;
				entriesForId.add(new StoreEntry(record, token));
				
				logger.info("Added record from {} for ID {}; {} records for ID, {} ids in store",
						record.routingInfo(),
						Util.bytesToHex(id.rawId, 8),
						entriesForId.size(),
						entriesById.size());

				write();
			}
		} catch(IOException exc) {
			logger.error("Caught exception adding record to record store", exc);
		}
		Util.setThreadName("Idle worker");
	}
	
	protected String path() {
		return "dht-record-store";
	}
	
	protected synchronized void write() throws IOException {
		MutableSecureFile file = MutableSecureFile.atPath(client.getStorage(), path(), client.recordStoreKey());
		file.write(serialize(), 0);
	}
	
	protected void read() {
		MutableSecureFile file = MutableSecureFile.atPath(client.getStorage(), path(), client.recordStoreKey());
		try {
			deserialize(ByteBuffer.wrap(file.read()));
		} catch(IOException|SecurityException exc) {
			entriesById.clear();
		}
		
		try {
			prune();
		} catch(IOException exc) {
		}
	}
	
	protected synchronized byte[] serialize() {
		LinkedList<byte[]> serializedPieces = new LinkedList<>();
		int totalLen = 0;
		
		for(DHTID id : entriesById.keySet()) {
			serializedPieces.add(id.serialize());
			serializedPieces.add(ByteBuffer.allocate(4).putInt(entriesById.get(id).size()).array());
			totalLen += id.serialize().length + 4;
			
			for(StoreEntry entry : entriesById.get(id)) {
				byte[] serialized = entry.serialize();
				serializedPieces.add(serialized);
				totalLen += serialized.length;
			}			
		}
		
		ByteBuffer buf = ByteBuffer.allocate(4+totalLen);
		buf.putInt(entriesById.keySet().size());
		for(byte[] piece : serializedPieces) {
			buf.put(piece);
		}

		return buf.array();
	}
	
	protected synchronized void deserialize(ByteBuffer serialized) throws EINVALException {
		entriesById.clear();
		try {
			int numIds = serialized.getInt();
			for(int i = 0; i < numIds; i++) {
				byte[] idRaw = new byte[client.idLength()];
				serialized.get(idRaw);
				DHTID id = new DHTID(idRaw);
				ArrayList<StoreEntry> entriesForId = new ArrayList<>();
				entriesById.put(id, entriesForId);
				
				int numEntriesForId = serialized.getInt();
				for(int j = 0; j < numEntriesForId; j++) {
					try {
						entriesForId.add(new StoreEntry(serialized));
					} catch(UnsupportedProtocolException exc) {
						throw new EINVALException(path());
					}
				}
			}
		} catch(BufferUnderflowException exc) {
			throw new EINVALException(path());
		}
	}
	
	protected synchronized void prune() throws IOException {
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
	
	public synchronized int numIds() {
		return entriesById.size();
	}

	public synchronized int numRecords() {
		int totalRecords = 0;
		for(ArrayList<StoreEntry> list : entriesById.values()) {
			totalRecords += list.size();
		}
		
		return totalRecords;
	}
}
