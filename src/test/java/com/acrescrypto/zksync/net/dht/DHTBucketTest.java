package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.utility.Util;

public class DHTBucketTest {
	class DummyClient extends DHTClient {
		public DummyClient() {
			this.routingTable = new DummyRoutingTable(this);
			this.crypto = DHTBucketTest.crypto;
			this.id = new DHTID(crypto.rng(crypto.hashLength()));
		}
	}
	
	class DummyRoutingTable extends DHTRoutingTable {
		public DummyRoutingTable(DHTClient client) {
			super(client);
		}

		@Override public void read() {}
		@Override public void freshenThread() {}
	}
	
	class DummyPeer extends DHTPeer {
		boolean pinged;
		
		public DummyPeer(DHTClient client, String address, int port, byte[] keyBytes) {
			super(client, address, port, keyBytes);
		}
		
		@Override public void ping() { pinged = true; }
	}
	
	static CryptoSupport crypto;
	DummyClient client;
	
	DummyPeer makePeer(int i) {
		return new DummyPeer(client, "10.0.0."+i, 1000+i, crypto.makePrivateDHKey().publicKey().getBytes());
	}
	
	void makePeerBad(DHTPeer peer) {
		peer.missedMessage();
		peer.missedMessage();
	}
	
	DHTID makeIdOfDistanceOrder(int order) {
		byte m = (byte) (1 << (order % 8));
		byte r = (byte) (m | ((m-1) & crypto.rng(1)[0]));
		
		ByteBuffer id = ByteBuffer.allocate(crypto.hashLength());
		id.position(id.capacity() - order/8 - 1);
		id.put(r);
		id.put(crypto.rng(id.remaining()));
		
		DHTID origId = new DHTID(id.array());
		return client.id.xor(origId);
	}
	
	@BeforeClass
	public static void beforeAll() throws IOException, InvalidBlacklistException {
		crypto = CryptoSupport.defaultCrypto();
	}
	
	@Before
	public void beforeEach() {
		client = new DummyClient();
	}
	
	@After
	public void afterEach() {
		Util.setCurrentTimeNanos(-1);
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}

	@Test
	public void testHasCapacityReturnsTrueIfEmptySlotsAvailable() {
		DHTBucket bucket = new DHTBucket(client, 16);
		assertTrue(bucket.hasCapacity());
		
		for(int i = 0; i < DHTBucket.MAX_BUCKET_CAPACITY-1; i++) {
			bucket.add(makePeer(i));
			assertTrue(bucket.hasCapacity());
		}
	}

	@Test
	public void testHasCapacityReturnsTrueIfBadPeersPresent() {
		DHTBucket bucket = new DHTBucket(client, 16);
		assertTrue(bucket.hasCapacity());
		
		for(int i = 0; i < DHTBucket.MAX_BUCKET_CAPACITY; i++) {
			DHTPeer peer = makePeer(i);
			if(i == 3) {
				makePeerBad(peer);
			}
			
			bucket.add(peer);
			assertTrue(bucket.hasCapacity());
		}
	}
	
	@Test
	public void testHasCapacityReturnsFalseIfNoSlotsOrBadPeers() {
		DHTBucket bucket = new DHTBucket(client, 16);
		assertTrue(bucket.hasCapacity());
		
		for(int i = 0; i < DHTBucket.MAX_BUCKET_CAPACITY; i++) {
			bucket.add(makePeer(i));
		}
		
		assertFalse(bucket.hasCapacity());
	}
	
	@Test
	public void testIncludesReturnsTrueIfDistanceIsOfBucketOrder() {
		for(int i = 0; i < 8*crypto.hashLength(); i++) {
			DHTBucket bucket = new DHTBucket(client, i);
			assertTrue(bucket.includes(makeIdOfDistanceOrder(i)));
		}
		
		DHTBucket bucket = new DHTBucket(client, -1);
		assertTrue(bucket.includes(client.id));
	}
	
	@Test
	public void testIncludesReturnsFalseIfDistanceIsLessThanBucketOrder() {
		for(int i = 1; i < 8*crypto.hashLength(); i++) {
			DHTBucket bucket = new DHTBucket(client, i);
			assertFalse(bucket.includes(makeIdOfDistanceOrder(i-1)));
		}
	}
	
	@Test
	public void testIncludesReturnsFalseIfDistanceIsGreaterThanBucketOrder() {
		for(int i = 0; i < 8*crypto.hashLength()-1; i++) {
			DHTBucket bucket = new DHTBucket(client, i);
			assertFalse(bucket.includes(makeIdOfDistanceOrder(i+1)));
		}
	}
	
	@Test
	public void testAddInsertsIntoBucket() {
		DHTBucket bucket = new DHTBucket(client, 16);
		DHTPeer peer = makePeer(0);
		
		assertFalse(bucket.peers.contains(peer));
		bucket.add(peer);
		assertTrue(bucket.peers.contains(peer));
	}
	
	@Test
	public void testAddTriggersPruneWhenAtCapacity() {
		DHTBucket bucket = new DHTBucket(client, 16);
		DHTPeer peer = makePeer(0);
		makePeerBad(peer);
		
		bucket.add(peer);
		
		for(int i = 0; i < DHTBucket.MAX_BUCKET_CAPACITY-1; i++) {
			bucket.add(makePeer(i+1));
		}
		
		assertTrue(bucket.peers.contains(peer));
		
		DHTPeer lastPeer = makePeer(DHTBucket.MAX_BUCKET_CAPACITY+1);
		bucket.add(lastPeer);
		assertFalse(bucket.peers.contains(peer));
		assertTrue(bucket.peers.contains(lastPeer));
	}
	
	@Test
	public void testAddDoesNotTriggerPruneWhenNotAtCapacity() {
		DHTBucket bucket = new DHTBucket(client, 16);
		DHTPeer peer = makePeer(0);
		makePeerBad(peer);
		bucket.add(peer);
		
		for(int i = 0; i < DHTBucket.MAX_BUCKET_CAPACITY-1; i++) {
			bucket.add(makePeer(i+1));
			assertTrue(bucket.peers.contains(peer));
		}
	}
	
	@Test
	public void testAddMarksBucketFresh() {
		Util.setCurrentTimeNanos(1*1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		DHTBucket bucket = new DHTBucket(client, 16);
		bucket.add(makePeer(0));
		
		Util.setCurrentTimeNanos(2*1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		assertTrue(bucket.needsFreshening());

		bucket.add(makePeer(0));
		assertFalse(bucket.needsFreshening());
	}
	
	@Test
	public void testRandomIdInRangeProducesIdOfBucketOrder() {
		for(int i = 0; i < 8*crypto.hashLength(); i++) {
			for(int j = 0; j < 100; j++) {
				DHTBucket bucket = new DHTBucket(client, i);
				assertEquals(i, bucket.randomIdInRange().xor(client.id).order());
			}
		}
	}
	
	@Test
	public void testMarkFreshClearsNeedsFreshening() {
		Util.setCurrentTimeNanos(1*1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		DHTBucket bucket = new DHTBucket(client, 16);
		bucket.markFresh();

		Util.setCurrentTimeNanos(2*1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		assertTrue(bucket.needsFreshening());
		bucket.markFresh();
		assertFalse(bucket.needsFreshening());
		
		Util.setCurrentTimeNanos(3*1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		assertTrue(bucket.needsFreshening());
		bucket.markFresh();
		assertFalse(bucket.needsFreshening());
	}
	
	@Test
	public void testNeedsFresheningReturnsFalseIfBucketHasBeenRecentlyFreshened() {
		Util.setCurrentTimeNanos(1*1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		DHTBucket bucket = new DHTBucket(client, 16);
		bucket.markFresh();
		
		Util.setCurrentTimeNanos(2*1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS-1);
		assertFalse(bucket.needsFreshening());
	}
	
	@Test
	public void testNeedsFresheningReturnsTrueIfBucketHasNotBeenRecentlyFreshened() {
		Util.setCurrentTimeNanos(1*1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		DHTBucket bucket = new DHTBucket(client, 16);
		bucket.markFresh();
		
		Util.setCurrentTimeNanos(2*1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		assertTrue(bucket.needsFreshening());
	}
	
	@Test
	public void testNeedsFresheningReturnsFalseIfBucketHasNeverHadContents() {
		Util.setCurrentTimeNanos(1*1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		DHTBucket bucket = new DHTBucket(client, 16);
		Util.setCurrentTimeNanos(2*1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		assertFalse(bucket.needsFreshening());
	}
	
	@Test
	public void testPrunePingsStalestPeer() {
		DHTBucket bucket = new DHTBucket(client, 16);
		ArrayList<DummyPeer> peers = new ArrayList<>();
		
		for(int i = 0; i < DHTBucket.MAX_BUCKET_CAPACITY; i++) {
			Util.setCurrentTimeNanos(i*1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
			DummyPeer peer = makePeer(i);
			peers.add(peer);
			peer.acknowledgedMessage();
			bucket.add(peer);
		}
		
		Util.setCurrentTimeNanos(DHTBucket.MAX_BUCKET_CAPACITY*1000l*1000l*DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		bucket.prune();
		for(int i = 0; i < peers.size(); i++) {
			assertEquals(i == 0, peers.get(i).pinged);
		}
	}
	
	@Test
	public void testPruneDoesNotPingNonquestionablePeers() {
		DHTBucket bucket = new DHTBucket(client, 16);
		ArrayList<DummyPeer> peers = new ArrayList<>();
		Util.setCurrentTimeMillis(0);
		
		for(int i = 0; i < DHTBucket.MAX_BUCKET_CAPACITY; i++) {
			DummyPeer peer = makePeer(i);
			peers.add(peer);
			peer.acknowledgedMessage();
			bucket.add(peer);
		}
		
		Util.setCurrentTimeMillis(DHTBucket.BUCKET_FRESHEN_INTERVAL_MS-1);
		bucket.prune();
		for(int i = 0; i < peers.size(); i++) {
			assertFalse(peers.get(i).pinged);
		}
	}
}
