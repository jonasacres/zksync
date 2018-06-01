package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.utility.Util;

public class DHTRoutingTableTest {
	class DummyClient extends DHTClient {
		LinkedList<DHTID> lookupIds;
		
		public DummyClient() {
			this.storage = new RAMFS();
			this.crypto = new CryptoSupport();
			this.storageKey = new Key(this.crypto);
			this.id = new DHTID(crypto.rng(crypto.hashLength()));
			this.lookupIds = new LinkedList<>();
		}
		
		@Override
		public void lookup(DHTID id, LookupCallback callback) {
			lookupIds.add(id);
		}
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
		DHTPeer peer = new DHTPeer(client, "10.0.0."+(client.crypto.prng(seed).getInt()%256), (client.crypto.prng(seed).getInt()%65536), pubKey);
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
		DHTRoutingTable.FRESHEN_INTERVAL_MS = DHTRoutingTable.DEFAULT_FRESHEN_INTERVAL_MS;
		table.close();
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
		Util.setCurrentTimeNanos(0);
		DHTPeer peer = makeTestPeer(0);

		DHTBucket bucket = table.buckets.get(peer.id.xor(client.id).order()+1);
		table.suggestPeer(peer);

		Util.setCurrentTimeNanos(1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		assertTrue(bucket.needsFreshening());
		table.markFresh(peer);
		assertFalse(bucket.needsFreshening());
	}
	
	@Test
	public void testMarkFreshDoesNotMarkIrrelevantBucketsAsFresh() {
		Util.setCurrentTimeNanos(0);
		DHTPeer peer = makeTestPeer(0);
		int bucketIndex = peer.id.xor(client.id).order()+1;

		table.suggestPeer(peer);
		Util.setCurrentTimeNanos(1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
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
	public void testAllPeersReturnsListOfPeersAcrossAllBuckets() {
		int numPeers = table.buckets.size();
		ArrayList<DHTPeer> peers = new ArrayList<>(numPeers);
		for(int i = 0; i < numPeers; i++) {
			DHTPeer peer = makeTestPeer(table.buckets.get(i).randomIdInRange());
			peers.add(peer);
			table.suggestPeer(peers.get(i));
		}
		
		assertEquals(numPeers, table.allPeers().size());
		assertEquals(peers, table.allPeers());
	}
	
	@Test
	public void testFreshenThreadCallsFreshenPeriodically() {
		DHTRoutingTable.FRESHEN_INTERVAL_MS = 5;
		table.close();
		assert(client.lookupIds.isEmpty());
		table = new DHTRoutingTable(client);
		assertTrue(Util.waitUntil(2*DHTRoutingTable.FRESHEN_INTERVAL_MS, ()->!client.lookupIds.isEmpty()));
	}
}
