package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.utility.Shuffler;
import com.acrescrypto.zksync.utility.Util;

public class DHTSearchOperationTest {
	class DummyRecord extends DHTRecord {
		byte[] contents;
		boolean reachable = true, valid = true;
		
		public DummyRecord(int i) {
			ByteBuffer buf = ByteBuffer.allocate(32);
			buf.putInt(i);
			buf.put(crypto.prng(ByteBuffer.allocate(4).putInt(i).array()).getBytes(buf.remaining()));
			contents = buf.array();
		}
		
		public DummyRecord(ByteBuffer serialized) throws UnsupportedProtocolException {
			deserialize(serialized);
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
		public int hashCode() { return ByteBuffer.wrap(contents).getInt(); }
		@Override public String routingInfo() { return ""; }
	}

	class DummyClient extends DHTClient {
		ArrayList<DHTPeer> simPeers;

		public DummyClient() {
			this.crypto = CryptoSupport.defaultCrypto();
			this.simPeers = makeTestList(this, 2048);
			this.routingTable = new DummyRoutingTable(this);
		}
		
		public ArrayList<DHTPeer> peerSample(int size) {
			ArrayList<DHTPeer> sample = new ArrayList<>(size);
			Shuffler shuffler = new Shuffler(simPeers.size());
			for(int i = 0; i < size; i++) {
				sample.add(simPeers.get(shuffler.next()));
			}
			return sample;
		}
	}
	
	class DummyRoutingTable extends DHTRoutingTable {
		ArrayList<DHTPeer> allPeers;
		DummyClient client;
		
		public DummyRoutingTable(DummyClient client) {
			super.client = this.client = client;
			this.allPeers = client.peerSample(64);
		}

		@Override public Collection<DHTPeer> allPeers() { return allPeers; }
		@Override public void reset() { this.allPeers = new ArrayList<>(0); }
	}
	
	class DummyPeer extends DHTPeer {
		DummyClient client;
		ArrayList<DHTPeer> knownPeers;
		ArrayList<DHTRecord> records;
		
		int requestsReceived = 0;
		
		public DummyPeer(DummyClient client, String address, int port, byte[] pubKey) {
			super(client, address, port, pubKey);
			this.client = client;
			
			int numRecords = 8;
			this.records = new ArrayList<>(numRecords);
			for(int i = 0; i < numRecords; i++) {
				records.add(new DummyRecord(i));
			}
		}
		
		@Override
		public void findNode(DHTID id, Key lookupKey, DHTFindNodePeerCallback peerCallback, DHTFindNodeRecordCallback recordCallback) {
			if(knownPeers == null) {
				knownPeers = client.peerSample(512);
			}
			
			requestsReceived++;
			if(sendResponses) {
				// cut responses into two halves so that we have to reassemble multiple responses
				ArrayList<DHTPeer> closest = closestInList(searchId, knownPeers, DHTSearchOperation.maxResults);
				ArrayList<DHTPeer> lowerHalf = new ArrayList<>(), upperHalf = new ArrayList<>();
				
				for(DHTPeer peer : closest) {
					if(lowerHalf.size() <= upperHalf.size()) {
						lowerHalf.add(peer);
					} else {
						upperHalf.add(peer);
					}
				}
				
				Thread t1 = new Thread(()->peerCallback.response(lowerHalf, false));
				Thread t2 = new Thread(()->{
					try { t1.join(); } catch (InterruptedException e) { }
					for(DHTRecord record : records) {
						recordCallback.receivedRecord(record);
					}
					recordCallback.receivedRecord(null);
					peerCallback.response(upperHalf, true);
				});
				
				t1.start();
				t2.start();
			}
		}
	}
	
	DummyClient client;
	CryptoSupport crypto;
	DHTID searchId;
	DHTSearchOperation op;
	Key lookupKey;
	Collection<DHTPeer> results;
	ArrayList<DHTRecord> records;
	boolean sendResponses = true;
	
	public void waitForResult() {
		assertTrue(Util.waitUntil(100, ()->results != null));
	}
	
	public ArrayList<DHTPeer> makeTestList(DummyClient client, int size) {
		ArrayList<DHTPeer> list = new ArrayList<>(size);
		for(int i = 0; i < size; i++) {
			list.add(makeTestPeer(client, i));
		}
		
		return list;
	}
	
	public ArrayList<DHTPeer> closestInList(DHTID id, Collection<DHTPeer> peers, int numResults) {
		ArrayList<DHTPeer> closest = new ArrayList<>(numResults);
		ArrayList<DHTPeer> sorted = new ArrayList<>(peers);
		sorted.sort((a,b)->a.id.xor(id).compareTo(b.id.xor(id)));
		
		for(int i = 0; i < numResults; i++) {
			closest.add(sorted.get(i));
		}
		
		return closest;
	}
	
	public DHTPeer makeTestPeer(DummyClient client, int i) {
		byte[] seed = ByteBuffer.allocate(4).putInt(i).array();
		byte[] pubKey = crypto.prng(seed).getBytes(crypto.asymPublicDHKeySize());
		return new DummyPeer(client, "10.0.0."+i, 1000+i, pubKey);
	}
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() {
		crypto = CryptoSupport.defaultCrypto();
		client = new DummyClient();
		searchId = new DHTID(crypto.rng(crypto.hashLength()));
		results = null;
		records = new ArrayList<>();
		lookupKey = new Key(crypto);
		op = new DHTSearchOperation(client,
				searchId,
				lookupKey,
				(results)->this.results = results,
				(record)->this.records.add(record));
	}
	
	@After
	public void afterEach() {
		DHTSearchOperation.maxResults = DHTSearchOperation.DEFAULT_MAX_RESULTS;
		DHTSearchOperation.searchQueryTimeoutMs = DHTSearchOperation.DEFAULT_SEARCH_QUERY_TIMEOUT_MS;
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Test
	public void testConstructorSetsFields() {
		assertEquals(client, op.client);
		assertEquals(searchId, op.searchId);
		assertNotNull(op.peerCallback);
	}
	
	@Test
	public void testRunSendsRequestToClosestPeers() {
		sendResponses = false;
		ArrayList<DHTPeer> closest = closestInList(searchId, client.routingTable.allPeers(), DHTSearchOperation.maxResults);
		
		op.run();
		
		for(DHTPeer peer : client.routingTable.allPeers()) {
			if(((DummyPeer) peer).requestsReceived > 0) {
				assertTrue(closest.contains(peer));
			} else {
				assertFalse(closest.contains(peer));
			}
		}
	}
	
	@Test
	public void testRunSendsRequestRecursively() {
		int numSeen = 0;
		DHTID worstInitialDistance = null;
		for(DHTPeer peer : closestInList(searchId, client.routingTable.allPeers(), DHTSearchOperation.maxResults)) {
			if(worstInitialDistance == null || worstInitialDistance.compareTo(peer.id.xor(searchId)) < 0) {
				worstInitialDistance = peer.id.xor(searchId);
			}
		}
		
		op.run();
		waitForResult();
		
		for(DHTPeer peer : client.simPeers) {
			if(((DummyPeer) peer).requestsReceived > 0) {
				assertTrue(worstInitialDistance.compareTo(peer.id.xor(searchId)) >= 0);
				numSeen++;
			}
		}
		
		assertTrue(numSeen > DHTSearchOperation.maxResults);
	}
	
	@Test
	public void testRunInvokesCallbackWithBestResults() {
		int extraResults = 2;
		
		ArrayList<DHTPeer> expectedResults = closestInList(searchId, client.simPeers, extraResults+DHTSearchOperation.maxResults);
		op.run();
		waitForResult();
		
		assertEquals(expectedResults.size(), results.size()+extraResults);
		assertTrue(expectedResults.containsAll(results));
	}
	
	@Test
	public void testRunMakesImmediateEmptyCallbackIfNoPeers() {
		client.routingTable.reset();
		op.run();
		waitForResult();
		assertEquals(0, results.size());
	}
	
	@Test
	public void testInvokesCallbackOnTimeoutIfResponseNotReceived() {
		DHTSearchOperation.searchQueryTimeoutMs = 50;
		sendResponses = false;
		op.run();
		waitForResult();
		assertEquals(DHTSearchOperation.maxResults, results.size());
	}
}
