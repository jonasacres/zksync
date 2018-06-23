package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.utility.Util;

public class DHTRecordStoreTest {
	class DummyClient extends DHTClient {
		public DummyClient() {
			this.storage = new RAMFS();
			this.crypto = new CryptoSupport();
			this.storageKey = new Key(crypto);
		}

		@Override
		protected DHTRecord deserializeRecord(ByteBuffer serialized) throws UnsupportedProtocolException {
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
	}
		
	DummyClient client;
	DHTRecordStore store;
	
	public DHTID makeId() {
		return new DHTID(client.crypto.rng(client.crypto.hashLength()));
	}
	
	@BeforeClass
	public static void beforeAll() {
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}
	
	@Before
	public void beforeEach() {
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
		store.addRecordForId(id, record);
		assertTrue(Util.waitUntil(50, ()->store.recordsForId(id).contains(record)));
	}
	
	@Test
	public void testAddRecordForIdIgnoresTheRecordIfNoRoomForNewIds() throws IOException {
		for(int i = 0; i < DHTRecordStore.MAX_IDS; i++) {
			DHTID id = makeId();
			DummyRecord record = new DummyRecord(i);
			store.addRecordForId(id, record);
		}
		
		assertTrue(Util.waitUntil(50, ()->store.entriesById.size() == DHTRecordStore.MAX_IDS));
		
		DHTID id = makeId();
		store.addRecordForId(id, new DummyRecord(DHTRecordStore.MAX_IDS));
		assertFalse(Util.waitUntil(50, ()->store.recordsForId(id).size() != 0));
	}
	
	@Test
	public void testAddRecordForIdIgnoresTheRecordIfIdIsFull() throws IOException {
		DHTID id = makeId();
		for(int i = 0; i < DHTRecordStore.MAX_RECORDS_PER_ID; i++) {
			DummyRecord record = new DummyRecord(i);
			store.addRecordForId(id, record);
		}
		
		assertTrue(Util.waitUntil(50, ()->store.recordsForId(id).size() == DHTRecordStore.MAX_RECORDS_PER_ID));
		
		DummyRecord record = new DummyRecord(DHTRecordStore.MAX_RECORDS_PER_ID);
		store.addRecordForId(id, record);
		assertFalse(Util.waitUntil(50, ()->store.recordsForId(id).contains(record)));
	}
	
	@Test
	public void testAddRecordForIdIgnoresTheRecordIfUnreachable() throws IOException {
		DHTID id = makeId();
		DummyRecord record = new DummyRecord(0);
		record.reachable = false;
		store.addRecordForId(id, record);
		assertFalse(Util.waitUntil(50, ()->store.recordsForId(id).contains(record)));
	}
	
	@Test
	public void testAddRecordForIdIgnoresRecordIfAlreadyPresent() throws IOException {
		DHTID id = makeId();
		store.addRecordForId(id, new DummyRecord(0));
		assertTrue(Util.waitUntil(50, ()->store.recordsForId(id).size() == 1));

		store.addRecordForId(id, new DummyRecord(0));
		assertFalse(Util.waitUntil(50, ()->store.recordsForId(id).size() != 1));
	}
	
	@Test
	public void testAddRecordForIdAllowsRecordAtMultipleIDs() throws IOException {
		DHTID id0 = makeId(), id1 = makeId();
		
		store.addRecordForId(id0, new DummyRecord(0));
		store.addRecordForId(id1, new DummyRecord(0));
		
		assertTrue(Util.waitUntil(50, ()->store.recordsForId(id0).contains(new DummyRecord(0))));
		assertTrue(Util.waitUntil(50, ()->store.recordsForId(id1).contains(new DummyRecord(0))));
	}
	
	@Test
	public void testPruningRemovesExpiredEntries() throws IOException {
		int numRecords = 64, numPrunable = 12;
		ArrayList<DHTID> ids = new ArrayList<>();
		DHTID id = null;
		
		Util.setCurrentTimeNanos(0);
		
		for(int i = 0; i < numRecords; i++) {
			if(i % 3 == 0) {
				id = makeId();
				ids.add(id);
			}
			
			if(i == numPrunable) {
				Util.sleep(5);
				Util.setCurrentTimeNanos(1000l*1000l*DHTRecordStore.EXPIRATION_TIME_MS/2);
			}
			
			DummyRecord record = new DummyRecord(i);
			store.addRecordForId(id, record);
		}
		
		Util.sleep(5);
		Util.setCurrentTimeNanos(1000l*1000l*DHTRecordStore.EXPIRATION_TIME_MS);
		store.prune();
		
		int numRemaining = 0;
		for(DHTID idd : ids) {
			numRemaining += store.recordsForId(idd).size();
		}
		
		assertEquals(numRecords-numPrunable, numRemaining);
	}
	
	@Test
	public void testRecordsForIdReturnsEmptyListIfIdNotPresent() {
		assertEquals(0, store.recordsForId(makeId()).size());
	}
	
	@Test
	public void testRecordsForIdReturnsAllRecordsForId() throws IOException {
		int numRecords = DHTRecordStore.MAX_RECORDS_PER_ID;
		ArrayList<DHTRecord> records = new ArrayList<>(numRecords);
		DHTID id = makeId();
		
		for(int i = 0; i < numRecords; i++) {
			DHTRecord record = new DummyRecord(i);
			records.add(record);
			store.addRecordForId(id, record);
		}
		
		assertTrue(Util.waitUntil(100, ()->numRecords == store.recordsForId(id).size()));
		assertTrue(records.containsAll(store.recordsForId(id)));
	}
	
	@Test
	public void testInitWithExistingFile() throws IOException {
		// checks that we get all ids/records with expiration timestamps
		int numRecords = 64, numPrunable = 12;
		ArrayList<DHTID> ids = new ArrayList<>();
		DHTID id = null;
		
		Util.setCurrentTimeNanos(0);
		
		for(int i = 0; i < numRecords; i++) {
			if(i % 3 == 0) {
				id = makeId();
				ids.add(id);
			}
			
			if(i == numPrunable) {
				Util.sleep(5);
				Util.setCurrentTimeNanos(1000l*1000l*DHTRecordStore.EXPIRATION_TIME_MS/2);
			}
			
			DummyRecord record = new DummyRecord(i);
			store.addRecordForId(id, record);
		}
		
		Util.sleep(5);
		
		DHTRecordStore store1 = new DHTRecordStore(client);
		for(DHTID idd : ids) {
			assertEquals(store.recordsForId(idd).size(), store1.recordsForId(idd).size());
			assertTrue(store.recordsForId(idd).containsAll(store1.recordsForId(idd)));
		}
		
		Util.setCurrentTimeNanos(1000l*1000l*DHTRecordStore.EXPIRATION_TIME_MS);
		store1.prune();
		int numRemaining = 0;
		for(DHTID idd : ids) {
			numRemaining += store1.recordsForId(idd).size();
		}
		
		assertEquals(numRecords-numPrunable, numRemaining);
	}
	
	@Test
	public void testInitWithCorruptedFile() throws IOException {
		DHTID id = makeId();
		store.addRecordForId(id, new DummyRecord(0));
		assertTrue(Util.waitUntil(100, ()->client.storage.exists(store.path())));

		byte[] data = client.storage.read(store.path());
		data[9] ^= 0x20;
		client.storage.write(store.path(), data);
		
		DHTRecordStore store1 = new DHTRecordStore(client);
		assertEquals(0, store1.recordsForId(id).size());
	}
}
