package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.utility.Util;

public class DHTRoutingTableTest {
	class DummyClient extends DHTClient {
		LinkedList<DHTID> lookupIds;
		
		public DummyClient() {
			this.storage = new RAMFS();
			this.crypto = CryptoSupport.defaultCrypto();
			this.storageKey = new Key(this.crypto);
			this.key = crypto.makePrivateDHKey();
			this.id = new DHTID(key.publicKey());
			this.lookupIds = new LinkedList<>();
		}
		
		@Override
		public boolean isListening() {
			return true;
		}
		
		@Override
		public void lookup(DHTID id, Key lookupKey, LookupCallback callback) {
			lookupIds.add(id);
		}
		
		@Override public void watchForResponse(DHTMessage msg, DatagramPacket packet) {}
		@Override public void sendDatagram(DatagramPacket msg) {}
	}
	
	DummyClient client;
	DHTRoutingTable table;
	
	public DHTPeer makeTestPeer(int i) {
		byte[] seed = ByteBuffer.allocate(4).putInt(i).array();
		byte[] pubKey = client.crypto.prng(seed).getBytes(client.crypto.asymPublicDHKeySize());
		return new DHTPeer(client, "10.0.0."+i, 1000+i, pubKey);
	}
	
	public DHTPeer makeTestPeer(DHTID id) {
		byte[] seed = id.serialize();
		byte[] pubKey = client.crypto.prng(seed).getBytes(client.crypto.asymPublicDHKeySize());
		DHTPeer peer = new DHTPeer(client,
				"10.0.0."+(client.crypto.prng(seed).getInt()%256),
				(client.crypto.prng(seed).getInt()%65536),
				pubKey);
		peer.id = id;
		return peer;
	}
	
	@Before
	public void beforeEach() {
		client = new DummyClient();
		table = new DHTRoutingTable(client);
	}
	
	@After
	public void afterEach() {
		Util.setCurrentTimeNanos(-1);
		DHTRoutingTable.freshenIntervalMs = DHTRoutingTable.DEFAULT_FRESHEN_INTERVAL_MS;
		table.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}
	
	@Test
	public void testInitWithoutStoredFile() {
		assertTrue(table.allPeers().isEmpty());
	}
	
	@Test
	public void testInitWithStoredFile() {
		for(int i = 0; i < DHTBucket.MAX_BUCKET_CAPACITY; i++) {
			table.suggestPeer(makeTestPeer(i));
		}
		
		DHTRoutingTable table2 = new DHTRoutingTable(client);
		assertEquals(table.allPeers().size(), table2.allPeers().size());
		for(DHTPeer peer : table.allPeers()) {
			assertTrue(table2.allPeers().contains(peer));
		}
		table2.close();
	}
	
	@Test
	public void testInitWithCorruptedFile() throws IOException {
		for(int i = 0; i < DHTBucket.MAX_BUCKET_CAPACITY; i++) {
			table.suggestPeer(makeTestPeer(i));
		}
		
		byte[] data = client.storage.read(table.path());
		data[12] ^= 0x40;
		client.storage.write(table.path(), data);
		
		DHTRoutingTable table2 = new DHTRoutingTable(client);
		assertEquals(0, table2.allPeers().size());
		table2.close();
	}
	
	@Test
	public void testMarkFreshMarksBucketContainingPeerAsFresh() {
		Util.setCurrentTimeMillis(0);
		DHTPeer peer = makeTestPeer(0);

		DHTBucket bucket = table.buckets.get(peer.id.xor(client.id).order()+1);
		table.suggestPeer(peer);

		Util.setCurrentTimeMillis(DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		assertTrue(bucket.needsFreshening());
		table.markFresh(peer);
		assertFalse(bucket.needsFreshening());
	}
	
	@Test
	public void testMarkFreshDoesNotMarkIrrelevantBucketsAsFresh() {
		Util.setCurrentTimeMillis(1);
		DHTPeer peer = makeTestPeer(0);
		int bucketIndex = peer.id.xor(client.id).order()+1;
		for(int i = 0; i <= 8*client.idLength(); i++) {
			table.buckets.get(i).markFresh();
		}

		table.suggestPeer(peer);
		Util.setCurrentTimeMillis(DHTBucket.BUCKET_FRESHEN_INTERVAL_MS+1);
		table.markFresh(peer);

		for(int i = 0; i <= 8*client.idLength(); i++) {
			if(i == bucketIndex) continue;
			assertTrue(table.buckets.get(i).needsFreshening());
		}
	}
	
	@Test
	public void testResetClearsAllBuckets() {
		for(int i = 0; i < 1000; i++) {
			table.suggestPeer(makeTestPeer(i));
		}
		
		boolean foundOccupiedBucket = false;
		for(int i = 0; i <= 8*client.idLength(); i++) {
			if(!table.buckets.get(i).peers.isEmpty()) {
				foundOccupiedBucket = true;
				break;
			}
		}
		
		assertTrue(foundOccupiedBucket);
		
		table.reset();
		
		for(int i = 0; i <= 8*client.idLength(); i++) {
			assertTrue(table.buckets.get(i).peers.isEmpty());
		}
	}
	
	@Test
	public void testClosestPeersReturnsPeersClosestToId() {
		for(int i = 0; i < 1000; i++) {
			table.suggestPeer(makeTestPeer(i));
		}
		
		int numPeers = DHTSearchOperation.maxResults;
		DHTID id = new DHTID(client.crypto.rng(client.idLength()));
		Collection<DHTPeer> closest = table.closestPeers(id, numPeers);
		
		assertEquals(numPeers, closest.size());
		
		DHTID mostDistant = null;
		for(DHTPeer peer : closest) {
			DHTID distance = peer.id.xor(id);
			if(mostDistant == null || distance.compareTo(mostDistant) > 0) {
				mostDistant = distance;
			}
		}
		
		for(DHTPeer peer : table.allPeers()) {
			if(closest.contains(peer)) continue;
			DHTID distance = peer.id.xor(id);
			assertTrue(distance.compareTo(mostDistant) >= 0);
		}
	}
	
	@Test
	public void testClosestPeersReturnsEmptyListIfTableEmpty() {
		DHTID id = new DHTID(client.crypto.rng(client.idLength()));
		assertEquals(0, table.closestPeers(id, 1).size());
	}
	
	@Test
	public void testClosestPeersReturnsPartialListIfTableIsSmallerThanRequestedSize() {
		int numPeers = 16;
		for(int i = 0; i < numPeers-1; i++) {
			// make sure to ignore bucket 0 since it always stays empty
			table.suggestPeer(makeTestPeer(table.buckets.get(i+1).randomIdInRange()));
		}
		
		DHTID id = new DHTID(client.crypto.rng(client.idLength()));
		assertEquals(numPeers-1, table.closestPeers(id, numPeers).size());
	}
	
	@Test
	public void testFreshenCallsLookupForRandomIdInEachStaleBucket() {
		// also covers case: freshen does not call lookup for ids in fresh buckets
		for(int i = 0; i <= 8*client.idLength(); i++) {
			if(i % 2 != 0) continue;
			table.buckets.get(i).markFresh();
		}
		
		table.freshen();
		int expectedOrder = 0;
		for(DHTID id : client.lookupIds) {
			assertEquals(expectedOrder, id.xor(client.id).order());
			expectedOrder += 2;
		}
	}
	
	@Test
	public void testFreshenCallsPruneForAllBuckets() {
		Util.setCurrentTimeMillis(0);
		for(int i = 0; i < client.idLength(); i++) {
			DHTBucket bucket = table.buckets.get(i);
			for(int j = 0; i < i % DHTBucket.MAX_BUCKET_CAPACITY; i++) {
				DHTPeer peer = makeTestPeer(i*DHTBucket.MAX_BUCKET_CAPACITY + j);
				bucket.add(peer);
				peer.missedMessage();
				peer.missedMessage();
			}
		}
		
		table.freshen();
		for(DHTBucket bucket : table.buckets) {
			assertTrue(bucket.peers.isEmpty());
		}
	}
	
	@Test
	public void testSuggestPeerAddsPeerToBucketIfBucketHasCapacity() {
		DHTPeer peer = makeTestPeer(0);
		table.suggestPeer(peer);
		int bucketIdx = peer.id.xor(client.id).order()+1;
		assertTrue(table.buckets.get(bucketIdx).peers.contains(peer));
	}
	
	@Test
	public void testSuggestPeerDoesNotAddsPeerToBucketIfBucketFull() {
		DHTPeer peer = makeTestPeer(0);
		int bucketIdx = peer.id.xor(client.id).order()+1;
		DHTBucket bucket = table.buckets.get(bucketIdx);
		
		for(int i = 0; i < DHTBucket.MAX_BUCKET_CAPACITY; i++) {
			bucket.add(makeTestPeer(i+1));
		}
		
		table.suggestPeer(peer);
		assertFalse(table.buckets.get(bucketIdx).peers.contains(peer));
	}
	
	
	@Test
	public void testSuggestPeerRejectsSelf() {
		DHTPeer peer = new DHTPeer(client, "127.0.0.1", 0, table.client.getPublicKey());
		table.suggestPeer(peer);
		int bucketIdx = peer.id.xor(client.id).order()+1;
		assertFalse(table.buckets.get(bucketIdx).peers.contains(peer));
	}
	
	@Test
	public void testAllPeersReturnsListOfPeersAcrossAllBuckets() {
		int numPeers = table.buckets.size() - 1;
		ArrayList<DHTPeer> peers = new ArrayList<>(numPeers);
		for(int i = 0; i < numPeers; i++) {
			DHTPeer peer = makeTestPeer(table.buckets.get(i+1).randomIdInRange());
			peers.add(peer);
			table.suggestPeer(peers.get(i));
		}
		
		assertEquals(numPeers, table.allPeers().size());
		assertEquals(peers, table.allPeers());
	}
	
	@Test
	public void testFreshenThreadCallsFreshenPeriodically() {
		DHTRoutingTable.freshenIntervalMs = 5;
		Util.setCurrentTimeMillis(0);
		table.suggestPeer(makeTestPeer(0));
		table.close();
		
		Util.setCurrentTimeMillis(DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		assert(client.lookupIds.isEmpty());
		table = new DHTRoutingTable(client);
		assertTrue(Util.waitUntil(50, ()->!client.lookupIds.isEmpty()));
	}
}
