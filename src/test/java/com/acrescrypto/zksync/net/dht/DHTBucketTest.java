package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigDefaults;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksync.utility.Util;

public class DHTBucketTest {
	class DummyMaster extends ZKMaster {
		public DummyMaster() {
			this.storage = new RAMFS(); 
			try {
				this.globalConfig = new ConfigFile(storage, "config.json");
			} catch (IOException e) {
				fail();
			}
			globalConfig.apply(ConfigDefaults.getActiveDefaults());
		}
	}
	
	class DummyClient extends DHTClient {
		public DummyClient() {
			this.crypto       = DHTBucketTest.crypto;
			this.routingTable = new DummyRoutingTable(this);
			this.id           = DHTID.withBytes(crypto.rng(crypto.hashLength()));
			this.master       = new DummyMaster();
		}
	}
	
	class DummyRoutingTable extends DHTRoutingTable {
		public DummyRoutingTable(DHTClient client) {
			super(client);
		}

		@Override public void read() { reset(); }
		@Override public void freshenThread() {}
		@Override public long bucketFreshenInterval() { return ConfigDefaults.getActiveDefaults().getLong("net.dht.bucketFreshenIntervalMs"); }
	}
	
	class DummyPeer extends DHTPeer {
		boolean pinged;
		
		public DummyPeer(DHTClient client, String address, int port, byte[] keyBytes) throws UnknownHostException {
			super(client, address, port, keyBytes);
		}
		
		@Override public void ping() { pinged = true; }
	}
	
	static CryptoSupport crypto;
	       DummyClient   client;
	       DHTBucket     bucket;
	
	DummyPeer makePeer(int i) {
		try {
			return new DummyPeer(
					client,
					"10.0.0." + i,
					1000      + i,
					crypto.makePrivateDHKey().publicKey().getBytes());
		} catch(UnknownHostException exc) {
			fail();
			return null;
		}
	}
	
	DummyPeer makePeer(DHTBucket bucket) {
		try {
			DummyPeer peer = new DummyPeer(
					client,
					"10.0.1." + bucket.peers.size(),
					2000      + bucket.peers.size(),
					crypto.makePrivateDHKey().publicKey().getBytes());
			peer.id = bucket.randomIdInRange();
			return peer;
		} catch(UnknownHostException exc) {
			fail();
			return null;
		}
	}
	
	void makePeerBad(DHTPeer peer) {
		peer.missedMessage();
		peer.missedMessage();
	}
	
	@BeforeClass
	public static void beforeAll() throws IOException, InvalidBlacklistException {
		TestUtils.startDebugMode();
		crypto = CryptoSupport.defaultCrypto();
	}
	
	@Before
	public void beforeEach() {
		client         = new DummyClient();
		bucket         = client.getRoutingTable().buckets.get(0);
	}
	
	@After
	public void afterEach() {
		Util.setCurrentTimeNanos(-1);
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	public void fillBucket(DHTBucket bucket) {
		fillBucket(bucket, bucket.maxCapacity());
	}
	
	public void fillBucket(DHTBucket bucket, int count) {
		for(int i = 0; i < count; i++) {
			bucket.add(makePeer(i));
		}
	}
	
	@Test
	public void testHasCapacityReturnsTrueIfEmptySlotsAvailable() {
		assertTrue(bucket.hasCapacity());
		
		for(int i = 0; i < bucket.maxCapacity()-1; i++) {
			bucket.add(makePeer(i));
			assertTrue(bucket.hasCapacity());
		}
	}

	@Test
	public void testHasCapacityReturnsTrueIfBadPeersPresent() {
		for(int i = 0; i < bucket.maxCapacity(); i++) {
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
		assertTrue(bucket.hasCapacity());
		
		for(int i = 0; i < bucket.maxCapacity(); i++) {
			bucket.add(makePeer(i));
		}
		
		assertFalse(bucket.hasCapacity());
	}
	
	@Test
	public void testIncludesReturnsTrueIfIdIsInRange() {
		for(int i = 0; i < 8*client.idLength(); i++) {
			DHTID id = bucket.randomIdInRange();
			assertTrue(bucket.includes(id));
			bucket.split();
		}
	}
	
	@Test
	public void testIncludesReturnsFalseIfIdLessThanMin() {
		DHTID originalMin = bucket.min();
		
		while(bucket.min() == originalMin) {
			bucket.split();
		}
		
		BigInteger minusOne    = BigInteger.valueOf(-1);
		BigInteger oneLessVal  = bucket.min().id().add(minusOne);
		DHTID      oneLess     = new DHTID(oneLessVal, originalMin.length);
		assertFalse(bucket.includes(oneLess));
	}
	
	@Test
	public void testIncludesReturnsTrueIfIdEqualsMin() {
		DHTID originalMin = bucket.min();
		assertTrue(bucket.includes(originalMin));
		
		while(bucket.min() == originalMin) {
			bucket.split();
		}
		assertTrue(bucket.includes(bucket.min()));
	}
	
	@Test
	public void testIncludesReturnsFalseIfIdGreaterThanOrEqualToMax() {
		DHTID originalMax = bucket.max();
		assertFalse(bucket.includes(bucket.max()));
		
		while(bucket.max() == originalMax) {
			bucket.split();
		}
		
		assertFalse(bucket.includes(bucket.max()));
		
		BigInteger one         = BigInteger.valueOf(1);
		BigInteger oneMoreVal  = bucket.max().id().add(one);
		DHTID      oneMore     = new DHTID(oneMoreVal, originalMax.length);
		
		assertFalse(bucket.includes(oneMore));
	}
	
	@Test
	public void testAddInsertsIntoBucket() {
		DHTPeer peer = makePeer(0);
		
		assertFalse(bucket.peers.contains(peer));
		bucket.add(peer);
		assertTrue(bucket.peers.contains(peer));
	}
	
	@Test
	public void testAddTriggersPruneWhenAtCapacity() {
		DHTPeer peer = makePeer(0);
		makePeerBad(peer);
		
		bucket.add(peer);
		
		for(int i = 0; i < bucket.maxCapacity()-1; i++) {
			bucket.add(makePeer(i+1));
		}
		
		assertTrue(bucket.peers.contains(peer));
		
		DHTPeer lastPeer = makePeer(bucket.maxCapacity()+1);
		bucket.add(lastPeer);
		assertFalse(bucket.peers.contains(peer));
		assertTrue(bucket.peers.contains(lastPeer));
	}
	
	@Test
	public void testAddDoesNotTriggerPruneWhenNotAtCapacity() {
		DHTPeer peer = makePeer(0);
		makePeerBad(peer);
		bucket.add(peer);
		
		for(int i = 0; i < bucket.maxCapacity()-1; i++) {
			bucket.add(makePeer(i+1));
			assertTrue(bucket.peers.contains(peer));
		}
	}
	
	@Test
	public void testAddMarksBucketFresh() {
		Util.setCurrentTimeNanos(1*1000l*1000l*client.getRoutingTable().bucketFreshenInterval());
		bucket.add(makePeer(0));
		
		Util.setCurrentTimeNanos(2*1000l*1000l*client.getRoutingTable().bucketFreshenInterval());
		assertTrue(bucket.needsFreshening());

		bucket.add(makePeer(0));
		assertFalse(bucket.needsFreshening());
	}
	
	@Test
	public void testRandomIdInRangeProducesIdInRange() {
		for(int i = 0; i < 8*client.idLength(); i++) {
			DHTID id = bucket.randomIdInRange();
			assertTrue(id.compareTo(bucket.min()) >= 0);
			assertTrue(id.compareTo(bucket.max()) <  0);
			bucket.split();
		}
	}
	
	@Test
	public void testMarkFreshClearsNeedsFreshening() {
		bucket.add(makePeer(0));
		bucket.setFresh(0);
		
		Util.setCurrentTimeNanos(1*1000l*1000l*client.getRoutingTable().bucketFreshenInterval());
		bucket.markFresh();

		Util.setCurrentTimeNanos(2*1000l*1000l*client.getRoutingTable().bucketFreshenInterval());
		assertTrue(bucket.needsFreshening());
		bucket.markFresh();
		assertFalse(bucket.needsFreshening());
		
		Util.setCurrentTimeNanos(3*1000l*1000l*client.getRoutingTable().bucketFreshenInterval());
		assertTrue(bucket.needsFreshening());
		bucket.markFresh();
		assertFalse(bucket.needsFreshening());
	}
	
	@Test
	public void testNeedsFresheningReturnsFalseIfBucketHasBeenRecentlyFreshened() {
		Util.setCurrentTimeNanos(1*1000l*1000l*client.getRoutingTable().bucketFreshenInterval());
		bucket.markFresh();
		
		Util.setCurrentTimeNanos(2*1000l*1000l*client.getRoutingTable().bucketFreshenInterval()-1);
		assertFalse(bucket.needsFreshening());
	}
	
	@Test
	public void testNeedsFresheningReturnsTrueIfBucketHasNotBeenRecentlyFreshened() {
		Util.setCurrentTimeNanos(1*1000l*1000l*client.getRoutingTable().bucketFreshenInterval());
		bucket.add(makePeer(0));
		
		Util.setCurrentTimeNanos(2*1000l*1000l*client.getRoutingTable().bucketFreshenInterval());
		assertTrue(bucket.needsFreshening());
	}
	
	@Test
	public void testNeedsFresheningReturnsFalseIfBucketHasNeverHadContents() {
		Util.setCurrentTimeNanos(bucket.lastChanged + client.getRoutingTable().bucketFreshenInterval());
		assertFalse(bucket.needsFreshening());
	}
	
	@Test
	public void testPrunePingsStalestPeer() {
		ArrayList<DummyPeer> peers = new ArrayList<>();
		
		for(int i = 0; i < bucket.maxCapacity(); i++) {
			Util.setCurrentTimeNanos(i*1000l*1000l*client.getRoutingTable().bucketFreshenInterval());
			DummyPeer peer = makePeer(i);
			peers.add(peer);
			peer.acknowledgedMessage();
			bucket.add(peer);
		}
		
		Util.setCurrentTimeNanos(bucket.maxCapacity()*1000l*1000l*client.getRoutingTable().bucketFreshenInterval());
		bucket.prune();
		for(int i = 0; i < peers.size(); i++) {
			assertEquals(i == 0, peers.get(i).pinged);
		}
	}
	
	@Test
	public void testPruneDoesNotPingNonquestionablePeers() {
		ArrayList<DummyPeer> peers = new ArrayList<>();
		Util.setCurrentTimeMillis(0);
		
		for(int i = 0; i < bucket.maxCapacity(); i++) {
			DummyPeer peer = makePeer(i);
			peers.add(peer);
			peer.acknowledgedMessage();
			bucket.add(peer);
		}
		
		Util.setCurrentTimeMillis(client.getRoutingTable().bucketFreshenInterval()-1);
		bucket.prune();
		for(int i = 0; i < peers.size(); i++) {
			assertFalse(peers.get(i).pinged);
		}
	}
	
	@Test
	public void testNeedsSplitReturnsFalseIfBucketHasCapacityAndDoesNotContainClientId() {
		DHTBucket newBucket = bucket.split();
		assertFalse(newBucket.needsSplit());
	}
	
	@Test
	public void testNeedsSplitReturnsFalseIfBucketHasCapacityAndContainsClientId() {
		assertFalse(bucket.needsSplit());
	}
	
	@Test
	public void testNeedsSplitReturnsFalseIfBucketDoesNotContainClientId() {
		DHTBucket newBucket = bucket.split();
		
		while(newBucket.hasCapacity()) {
			newBucket.add(makePeer(newBucket));
		}
		
		assertFalse(newBucket.needsSplit());
	}
	
	@Test
	public void testNeedsSplitReturnsTrueIfBucketContainsClientIdAndIsFull() {
		while(bucket.hasCapacity()) {
			bucket.add(makePeer(bucket));
		}
		
		assertTrue(bucket.needsSplit());
	}
	
	@Test
	public void testSplitSetsAppropriateRanges() {
		DHTID     oldMin    = bucket.min(),
			      oldMax    = bucket.max(),
			      mid       = bucket.max().midpoint(bucket.min());
		DHTBucket newBucket = bucket.split();
		
		if(newBucket.min().compareTo(bucket.max()) == 0) {
			// existing bucket is lower half
			assertEquals(oldMin,    bucket.min());
			assertEquals(mid,       bucket.max());
			assertEquals(mid,    newBucket.min());
			assertEquals(oldMax, newBucket.max());
		} else {
			// existing bucket is upper half
			assertEquals(oldMin, newBucket.min());
			assertEquals(mid,    newBucket.max());
			assertEquals(mid,       bucket.min());
			assertEquals(oldMax,    bucket.max());
		}
	}
	
	@Test
	public void testSplitEnsuresExistingBucketIncludesClientId() {
		assertTrue(bucket.includes(client.getId()));
		
		for(int i = 0; i < 8*client.idLength(); i++) {
			DHTBucket   newBucket = bucket.split();
			assertTrue (bucket   .includes(client.getId()));
			assertFalse(newBucket.includes(client.getId()));
		}
	}
	
	@Test
	public void testSplitApportionsExistingPeersBetweenBucketsAppropriately() {
		while(bucket.hasCapacity()) {
			bucket.add(makePeer(bucket));
		}
		
		HashSet<DHTPeer> existing   = new HashSet<>(bucket.peers);
		DHTBucket        newBucket  = bucket.split();
		int              totalPeers = bucket.peers.size() + newBucket.peers.size();
		
		assertEquals(bucket.maxCapacity(), existing.size());
		assertEquals(existing.size(),      totalPeers);
		
		for(DHTPeer peer : existing) {
			if(bucket.includes(peer.getId())) {
				assertTrue (   bucket.peers.contains(peer));
				assertFalse(newBucket.peers.contains(peer));
			} else {
				assertFalse(   bucket.peers.contains(peer));
				assertTrue (newBucket.peers.contains(peer));
			}
		}
	}
}
