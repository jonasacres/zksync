package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigDefaults;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksync.net.dht.DHTRecordStore.StoreEntry;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.Util;

public class DHTRecordStoreTest {
	class DummyMaster extends ZKMaster {
		public DummyMaster()
				throws IOException, InvalidBlacklistException
		{
			this.storage      = new RAMFS();
			this.globalConfig = new ConfigFile(storage, "config.json");
			
			globalConfig.apply(ConfigDefaults.getActiveDefaults());
		}
		
		@Override
		public void close() {}
	}
	class DummyClient extends DHTClient {
		RAMFS storage;
		
		public DummyClient() throws IOException, InvalidBlacklistException {
			this.master           = new DummyMaster();
			this.storage          = (RAMFS) master.getStorage();
			this.threadPool       = GroupedThreadPool.newCachedThreadPool(Thread.currentThread().getThreadGroup(), "DummyClient");
			this.crypto           = CryptoSupport.defaultCrypto();
			this.storageKey       = new Key(crypto);
			
			super.protocolManager = this.protocolManager = new DummyProtocolManager(this);
		}
		
		@Override
		public RAMFS getStorage() { return storage; }
	}
	
	class DummyProtocolManager extends DHTProtocolManager {
		DummyClient client;
		
		public DummyProtocolManager(DummyClient client) {
			super.client = this.client = client;
		}
		
		@Override
		protected DHTRecord deserializeRecord(DHTPeer peer, ByteBuffer serialized) throws UnsupportedProtocolException {
			return new DummyRecord(serialized);
		}
	}
	
	class DummyRecord extends DHTRecord {
		byte[] contents;
		boolean reachable = true, valid = true;
		
		public DummyRecord(int i) {
			ByteBuffer buf = ByteBuffer.allocate(32);
			buf.putInt(i);
			buf.put(client.crypto.prng(ByteBuffer.allocate(4).putInt(i).array()).getBytes(buf.remaining()));
			contents = buf.array();
		}
		
		public DummyRecord(ByteBuffer serialized) throws UnsupportedProtocolException {
			deserialize(serialized);
		}
		
		public void corrupt() {
			contents[0] |= 0x80;
		}

		@Override
		public byte[] serialize() {
			ByteBuffer serialized = ByteBuffer.allocate(2+contents.length);
			serialized.putShort((short) contents.length);
			serialized.put(contents);
			return serialized.array();
		}

		@Override
		public void deserialize(ByteBuffer serialized) throws UnsupportedProtocolException {
			int len = Util.unsignShort(serialized.getShort());
			contents = new byte[len];
			serialized.get(contents);
			if((contents[0] & 0x80) != 0) throw new UnsupportedProtocolException();
		}

		@Override public boolean isValid() { return valid; }
		@Override public boolean isReachable() { return reachable; }
		public boolean equals(Object o) { return Arrays.equals(contents, ((DummyRecord) o).contents); }
		@Override public String routingInfo() { return ""; }
	}
		
	DummyClient client;
	DHTRecordStore store;
	
	public DHTID makeId() {
		return new DHTID(client.crypto.rng(client.crypto.hashLength()));
	}
	
	public long expirationTimeMs() {
		return client.getMaster().getGlobalConfig().getLong("net.dht.store.expirationTimeMs");
	}
	
	public int maxIds() {
		return client.getMaster().getGlobalConfig().getInt("net.dht.store.maxIds");
	}
	
	public int maxRecordsPerId() {
		return client.getMaster().getGlobalConfig().getInt("net.dht.store.maxRecordsPerId");
	}
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException, InvalidBlacklistException {
		client = new DummyClient();
		store = new DHTRecordStore(client);
	}
	
	@After
	public void afterEach() {
		Util.setCurrentTimeNanos(-1);
	}
	
	@Test
	public void testAddRecordForIdAddsRecordIfReachableAndAvailable() throws IOException {
		DHTID id = makeId();
		DummyRecord record = new DummyRecord(0);
		byte[] token = client.crypto.hash(new byte[1]);
		store.addRecordForId(id, token, record);
		assertTrue(Util.waitUntil(50, ()->store.recordsForId(id, token).contains(record)));
	}
	
	@Test
	public void testAddRecordForIdIgnoresTheRecordIfNoRoomForNewIds() throws IOException {
		byte[] token = client.crypto.hash(new byte[1]);
		for(int i = 0; i < maxIds(); i++) {
			DHTID id = makeId();
			DummyRecord record = new DummyRecord(i);
			store.addRecordForId(id, token, record);
		}
		
		assertTrue(Util.waitUntil(50, ()->store.entriesById.size() == maxIds()));
		
		DHTID id = makeId();
		store.addRecordForId(id, token, new DummyRecord(maxIds()));
		assertFalse(Util.waitUntil(50, ()->store.recordsForId(id, token).size() != 0));
	}
	
	@Test
	public void testAddRecordForIdIgnoresTheRecordIfIdIsFull() throws IOException {
		DHTID id = makeId();
		byte[] token = client.crypto.hash(new byte[1]);
		
		for(int i = 0; i < maxRecordsPerId(); i++) {
			DummyRecord record = new DummyRecord(i);
			store.addRecordForId(id, token, record);
		}
		
		assertTrue(Util.waitUntil(50, ()->store.recordsForId(id, token).size() == maxRecordsPerId()));
		
		DummyRecord record = new DummyRecord(maxRecordsPerId());
		store.addRecordForId(id, token, record);
		assertFalse(Util.waitUntil(50, ()->store.recordsForId(id, token).contains(record)));
	}
	
	@Test
	public void testAddRecordForIdIgnoresTheRecordIfUnreachable() throws IOException {
		DHTID id = makeId();
		DummyRecord record = new DummyRecord(0);
		byte[] token = client.crypto.hash(new byte[1]);
		record.reachable = false;
		store.addRecordForId(id, token, record);
		assertFalse(Util.waitUntil(50, ()->store.recordsForId(id, token).contains(record)));
	}
	
	@Test
	public void testAddRecordForIdIgnoresRecordIfAlreadyPresent() throws IOException {
		DHTID id = makeId();
		byte[] token = client.crypto.hash(new byte[1]);
		store.addRecordForId(id, token, new DummyRecord(0));
		assertTrue(Util.waitUntil(50, ()->store.recordsForId(id, token).size() == 1));

		store.addRecordForId(id, token, new DummyRecord(0));
		assertFalse(Util.waitUntil(50, ()->store.recordsForId(id, token).size() != 1));
	}
	
	@Test
	public void testAddRecordForIdAllowsRecordAtMultipleIDs() throws IOException {
		DHTID id0 = makeId(), id1 = makeId();
		byte[] token = client.crypto.hash(new byte[1]);
		
		store.addRecordForId(id0, token, new DummyRecord(0));
		store.addRecordForId(id1, token, new DummyRecord(0));
		
		assertTrue(Util.waitUntil(50, ()->store.recordsForId(id0, token).contains(new DummyRecord(0))));
		assertTrue(Util.waitUntil(50, ()->store.recordsForId(id1, token).contains(new DummyRecord(0))));
	}
	
	@Test
	public void testRecordsReturnsAllRecordsInStore() throws IOException {
		int numIds          = 10,
			numRecordsPerId =  3;
		
		HashMap<DHTID, LinkedList<DummyRecord>>    map = new HashMap<>();
		HashMap<DummyRecord, byte[]>            tokens = new HashMap<>();
		
		for(int i = 0; i < numIds; i++) {
			DHTID id = makeId();
			LinkedList<DummyRecord> list = new LinkedList<>();
			map.put(id, list);
			
			for(int j = 0; j < numIds; j++) {
				int x              = i*numRecordsPerId + j;
				byte[] token       = client.crypto.hash(Util.serializeInt(x));
				DummyRecord record = new DummyRecord(x);
				
				tokens.put(record, token);
				store.addRecordForId(
						id,
						token,
						record);
				list.add(record);
			}
		}
		
		for(DHTID id : map.keySet()) {
			for(DummyRecord record : map.get(id)) {
				byte[] token = tokens.get(record);
				assertTrue(Util.waitUntil(50, ()->store.recordsForId(id, token).contains(record)));
			};
		}
		
		Map<DHTID, Collection<StoreEntry>> allRecords = store.records();
		assertEquals(map.size(), allRecords.size());
		for(DHTID id : map.keySet()) {
			assertTrue(allRecords.containsKey(id));
			assertEquals(map.get(id).size(), allRecords.get(id).size());
			
			for(StoreEntry entry : allRecords.get(id)) {
				boolean found = false;
				for(DummyRecord record : map.get(id)) {
					if(record.equals(entry.record())) {
						found = true;
						break;
					}
				}
				
				assertTrue(found);
			}
		}
	}
	
	@Test
	public void testPruningRemovesExpiredEntries() throws IOException {
		int numRecords = 64, numPrunable = 12;
		ArrayList<DHTID> ids = new ArrayList<>();
		DHTID id = null;
		byte[] token = client.crypto.hash(new byte[1]);
		
		Util.setCurrentTimeNanos(0);
		
		for(int i = 0; i < numRecords; i++) {
			if(i % 3 == 0) {
				id = makeId();
				ids.add(id);
			}
			
			if(i == numPrunable) {
				Util.sleep(5);
				Util.setCurrentTimeNanos(1000l*1000l*expirationTimeMs()/2);
			}
			
			DummyRecord record = new DummyRecord(i);
			store.addRecordForIdBlocking(id, token, record);
		}
		
		Util.sleep(5);
		Util.setCurrentTimeNanos(1000l*1000l*expirationTimeMs());
		store.prune();
		
		int numRemaining = 0;
		for(DHTID idd : ids) {
			numRemaining += store.recordsForId(idd, token).size();
		}
		
		assertEquals(numRecords-numPrunable, numRemaining);
	}
	
	@Test
	public void testRecordsForIdReturnsEmptyListIfIdNotPresent() {
		assertEquals(0, store.recordsForId(makeId(), client.crypto.hash(new byte[1])).size());
	}
	
	@Test
	public void testRecordsForIdReturnsAllRecordsForIdMatchingToken() throws IOException {
		// also tests that nonmatching tokens are excluded
		int numRecords = maxRecordsPerId()-1;
		ArrayList<DHTRecord> records = new ArrayList<>(numRecords);
		DHTID id = makeId();
		byte[] token = client.crypto.hash(new byte[1]);
		
		for(int i = 0; i < numRecords; i++) {
			DHTRecord record = new DummyRecord(i);
			records.add(record);
			store.addRecordForIdBlocking(id, token, record);
		}
		
		// also add in one with a mismatched token; it should not appear in the result set!
		store.addRecordForIdBlocking(id, client.crypto.hash(Util.serializeInt(0)), new DummyRecord(numRecords));
		
		assertTrue(Util.waitUntil(100, ()->numRecords == store.recordsForId(id, token).size()));
		assertTrue(records.containsAll(store.recordsForId(id, token)));
		assertEquals(records.size(), store.recordsForId(id, token).size());
	}
	
	@Test
	public void testInitWithExistingFile() throws IOException {
		// checks that we get all ids/records with expiration timestamps
		int numRecords = 64, numPrunable = 12;
		ArrayList<DHTID> ids = new ArrayList<>();
		DHTID id = null;
		byte[] token = client.crypto.hash(new byte[1]);
		
		Util.setCurrentTimeNanos(0);
		
		for(int i = 0; i < numRecords; i++) {
			if(i % 3 == 0) {
				id = makeId();
				ids.add(id);
			}
			
			if(i == numPrunable) {
				Util.sleep(5);
				Util.setCurrentTimeNanos(1000l*1000l*expirationTimeMs()/2);
			}
			
			DummyRecord record = new DummyRecord(i);
			store.addRecordForIdBlocking(id, token, record);
		}
		
		Util.sleep(10);
		
		DHTRecordStore store1 = new DHTRecordStore(client);
		for(DHTID idd : ids) {
			assertEquals(store.recordsForId(idd, token).size(), store1.recordsForId(idd, token).size());
			assertTrue(store.recordsForId(idd, token).containsAll(store1.recordsForId(idd, token)));
		}
		
		Util.setCurrentTimeNanos(1000l*1000l*expirationTimeMs());
		store1.prune();
		int numRemaining = 0;
		for(DHTID idd : ids) {
			numRemaining += store1.recordsForId(idd, token).size();
		}
		
		assertEquals(numRecords-numPrunable, numRemaining);
	}
	
	@Test
	public void testInitWithCorruptedFile() throws IOException {
		DHTID id = makeId();
		byte[] token = client.crypto.hash(new byte[1]);
	
		store.addRecordForId(id, token, new DummyRecord(0));
		assertTrue(Util.waitUntil(100, ()->client.storage.exists(store.path())));

		byte[] data = client.storage.read(store.path());
		data[9] ^= 0x20;
		client.storage.write(store.path(), data);
		
		DHTRecordStore store1 = new DHTRecordStore(client);
		assertEquals(0, store1.recordsForId(id, token).size());
	}
}
