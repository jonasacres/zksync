package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
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
		
		public StoreEntry(ByteBuffer serialized) throws UnsupportedProtocolException {
			deserialize(serialized);
		}
		
		public boolean isExpired() {
			return Util.currentTimeMillis() >= this.expirationTime;
		}
		
		public byte[] serialize() {
			byte[] recordSer = record.serialize();
			ByteBuffer buf = ByteBuffer.allocate(8+2+recordSer.length);
			buf.putLong(expirationTime);
			buf.put(recordSer);
			return buf.array();
		}
		
		public void deserialize(ByteBuffer serialized) throws UnsupportedProtocolException {
			this.expirationTime = serialized.getLong();
			try {
				record = DHTRecord.deserializeRecord(client.crypto, serialized);
			} catch (UnsupportedProtocolException exc) {
				logger.error("Record store contained unsupported record", record);
				throw exc;
			}
		}
		
		public boolean equals(Object other) {
			if(other instanceof DHTRecord) {
				return record.equals(other);
			}
			
			if(other instanceof StoreEntry) {
				return record.equals(((StoreEntry) other).record);
			}
			
			return false;
		}
	}
	
	protected DHTClient client;
	protected HashMap<DHTID,ArrayList<StoreEntry>> entriesById;
	private Logger logger = LoggerFactory.getLogger(DHTRecordStore.class);

	public DHTRecordStore(DHTClient client) {
		this.client = client;
		read();
	}
	
	public void addRecordForId(DHTID id, DHTRecord record) throws IOException {
		if(!hasRoomForRecord(id, record)) return;
		new Thread(()->addRecordIfReachable(id, record)).start();
	}
	
	@SuppressWarnings("unlikely-arg-type")
	protected boolean hasRoomForRecord(DHTID id, DHTRecord record) throws IOException {
		if(!entriesById.containsKey(id)) {
			if(entriesById.size() >= MAX_IDS) prune();
			if(entriesById.size() >= MAX_IDS) return false;
			return true;
		}
		
		ArrayList<StoreEntry> entriesForId = entriesById.get(id);
		if(entriesForId.contains(record)) return false;
		if(entriesForId.size() >= MAX_RECORDS_PER_ID) prune();
		if(entriesForId.size() >= MAX_RECORDS_PER_ID) return false;
		return true;
	}
	
	protected void addRecordIfReachable(DHTID id, DHTRecord record) {
		try {
			if(!record.isReachable()) return;
			if(!entriesById.containsKey(id)) {
				if(entriesById.size() >= MAX_IDS) prune();
				if(entriesById.size() >= MAX_IDS) return;
				entriesById.put(id, new ArrayList<StoreEntry>(MAX_RECORDS_PER_ID));
			}
			
			ArrayList<StoreEntry> entriesForId = entriesById.get(id);
			if(entriesForId.size() >= MAX_RECORDS_PER_ID) prune();
			if(entriesForId.size() >= MAX_RECORDS_PER_ID) return;
			entriesForId.add(new StoreEntry(record));
			write();
		} catch(IOException exc) {
			logger.error("Caught exception adding record to record store", exc);
		}
	}
	
	public Collection<DHTRecord> recordsForId(DHTID id) {
		LinkedList<DHTRecord> records = new LinkedList<>();
		Collection<StoreEntry> entries = entriesById.getOrDefault(id, null);
		
		for(StoreEntry entry : entries) {
			records.add(entry.record);
		}
		
		return records;
	}
	
	protected String path() {
		return "dht-record-store";
	}
	
	protected void write() throws IOException {
		MutableSecureFile file = MutableSecureFile.atPath(client.storage, path(), client.recordStoreKey());
		file.write(serialize(), 0);
	}
	
	protected void read() {
		MutableSecureFile file = MutableSecureFile.atPath(client.storage, path(), client.recordStoreKey());
		try {
			deserialize(ByteBuffer.wrap(file.read()));
		} catch(IOException exc) {
			entriesById.clear();
		}
		
		try {
			prune();
		} catch(IOException exc) {
		}
	}
	
	protected byte[] serialize() {
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
	
	protected void deserialize(ByteBuffer serialized) throws EINVALException {
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
	
	protected void prune() throws IOException {
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
