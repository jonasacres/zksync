package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigDefaults;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksync.net.dht.DHTClient.LookupCallback;
import com.acrescrypto.zksync.utility.Util;

public class DHTRoutingTableTest {
	class DummyMaster extends ZKMaster {
		public DummyMaster() {
			this.storage      = new RAMFS();
			try {
				this.globalConfig = new ConfigFile(storage, "config.json");
			} catch (IOException e) {
				fail();
			}
			globalConfig.apply(ConfigDefaults.getActiveDefaults());
		}
	}

	class DummyClient extends DHTClient {
		LinkedList<DHTID> lookupIds;
		RAMFS storage;
		
		public DummyClient() {
			this.crypto           = CryptoSupport.defaultCrypto();
			this.privateKey       = crypto.makePrivateDHKey();
			this.storage          = new RAMFS();
			this.storageKey       = new Key(this.crypto);
			this.lookupIds        = new LinkedList<>();
			this.master           = new DummyMaster();
			this.id               = DHTID.withKey(privateKey.publicKey());
			this.networkId        = crypto.hash("test".getBytes());
			
			super.protocolManager = this.protocolManager = new DummyProtocolManager(this);
			super.socketManager   = this.socketManager   = new DummySocketManager(this);
		}
		
		@Override
		public RAMFS getStorage() {
			return storage;
		}
		
		@Override
		public DHTRoutingTable getRoutingTable() {
			return table;
		}
	}
	
	class DummyProtocolManager extends DHTProtocolManager {
		DummyClient client;
		
		public DummyProtocolManager(DummyClient client) {
			super.client = this.client = client;
		}

		@Override
		public void lookup(DHTID id, Key lookupKey, LookupCallback callback) {
			client.lookupIds.add(id);
		}
		
		@Override public void watchForResponse(DHTMessage msg) {}
	}
	
	class DummySocketManager extends DHTSocketManager {
		DummyClient client;
		
		public DummySocketManager(DummyClient client) {
			this.client = client;
		}

		@Override
		public boolean isListening() {
			return true;
		}
		
		@Override public void sendDatagram(DatagramPacket msg) {}
	}
	
	DummyClient client;
	DHTRoutingTable table;
	
	public void fillTable(int numPeers) {
		for(int i = 0; i < numPeers; i++) {
			DHTPeer peer;
			
			do {
				LinkedList<DHTBucket> availableBuckets = new LinkedList<>(table.buckets);
				availableBuckets.removeIf((bb)->!bb.hasCapacity() && !bb.needsSplit());
				
				int       r      = client.crypto.defaultPrng().getInt(availableBuckets.size());
				DHTBucket bucket = availableBuckets.get(r);
				DHTID     id     = bucket.randomIdInRange();
				          peer   = makeTestPeer(id);
			} while(!table.suggestPeer(peer));
		}
	}
	
	public void fillTableSlow(int numPeers) {
		int j = 0;
		
		for(int i = 0; i < numPeers; i++) {
			DHTPeer peer;
			
			do {
				peer = makeTestPeer(j++);
			} while(!table.suggestPeer(peer));
		}
	}
	
	public DHTPeer makeTestPeer(int i) {
		try {
			byte[] seed = ByteBuffer.allocate(4).putInt(i).array();
			byte[] pubKey = client.crypto.prng(seed).getBytes(client.crypto.asymPublicDHKeySize());
			
			int ii = (i / (1      )) % 256,
				jj = (i / (256    )) % 256,
				kk = (i / (256*256)) % 256;
			return new DHTPeer(
					client,
					"10."+kk+"."+jj+"."+ii,
					1000+i,
					pubKey);
		} catch(UnknownHostException exc) {
			fail();
			return null;
		}
	}
	
	public DHTPeer makeTestPeer(DHTID id) {
		/* this can guarantee a peer falls into a bucket, but because it overrides the
		 * ID field from its usual public key-based value, and the public key is what is
		 * serialized when writing the routing table, peers created here do not deserialize
		 * correctly.
		 */
		try {
			byte[] seed   = id.serialize();
			byte[] pubKey = client
					          .crypto
					          .prng(seed)
					          .getBytes(client.crypto.asymPublicDHKeySize());
			DHTPeer peer  = new DHTPeer(
					client,
					"10.0.0."+(client.crypto.prng(seed).getInt(256)),
					(client.crypto.prng(seed).getInt(65536)), // port
					pubKey);
			peer.id       = id;
			return peer;
		} catch(UnknownHostException exc) {
			fail();
			return null;
		}
	}
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() {
		client = new DummyClient();
		table  = new DHTRoutingTable(client);
	}
	
	@After
	public void afterEach() {
		Util.setCurrentTimeNanos(-1);
		table.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Test
	public void testInitWithoutStoredFile() {
		assertTrue(table.allPeers().isEmpty());
	}
	
	@Test
	public void testInitWithStoredFile() {
		fillTableSlow(table.maxBucketCapacity());
		
		DHTRoutingTable table2 = new DHTRoutingTable(client);
		assertEquals(table.allPeers().size(), table2.allPeers().size());
		for(DHTPeer peer : table.allPeers()) {
			assertTrue(table2.allPeers().contains(peer));
		}
		table2.close();
	}
	
	@Test
	public void testInitWithCorruptedFile() throws IOException {
		for(int i = 0; i < table.maxBucketCapacity(); i++) {
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
		
		DHTPeer peer     = makeTestPeer(0);
		DHTBucket bucket = table.bucketForId(peer.getId());
		table.suggestPeer(peer);

		Util.setCurrentTimeMillis(table.bucketFreshenInterval());
		
		assertTrue (bucket.needsFreshening());
		table.markFresh(peer);
		assertFalse(bucket.needsFreshening());
	}
	
	@Test
	public void testMarkFreshDoesNotMarkIrrelevantBucketsAsFresh() {
		Util.setCurrentTimeMillis(1);
		fillTable(8*table.maxBucketCapacity());
		
		DHTPeer   peer    = makeTestPeer(0);
		
		for(DHTBucket bucket : table.buckets()) {
			bucket.markFresh();
		}

		table.suggestPeer(peer);
		Util.setCurrentTimeMillis(table.bucketFreshenInterval()+1);
		table.markFresh(peer);

		for(DHTBucket bucket : table.buckets()) {
			if(bucket.includes(peer.getId())) continue;
			assertTrue(bucket.needsFreshening());
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
		
		assertTrue(table.allPeers().isEmpty());
		for(int i = 0; i <= 8*client.idLength(); i++) {
			assertTrue(table.buckets.size() == 1);
			assertTrue(table.buckets.get(0).peers().isEmpty());
		}
	}
	
	@Test
	public void testClosestPeersReturnsPeersClosestToId() {
		for(int i = 0; i < 1000; i++) {
			table.suggestPeer(makeTestPeer(i));
		}
		
		int numPeers = client.getMaster().getGlobalConfig().getInt("net.dht.maxResults");
		DHTID id = DHTID.withBytes(client.crypto.rng(client.idLength()));
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
		DHTID id = DHTID.withBytes(client.crypto.rng(client.idLength()));
		assertEquals(0, table.closestPeers(id, 1).size());
	}
	
	@Test
	public void testClosestPeersReturnsPartialListIfTableIsSmallerThanRequestedSize() {
		int numPeers = 16;
		for(int i = 0; i < numPeers-1; i++) {
			while(true) {
				DHTID randomId = DHTID.withBytes(client.crypto.rng(client.idLength()));
				if(table.suggestPeer(makeTestPeer(randomId))) {
					break;
				}
			}
		}
		
		DHTID id = DHTID.withBytes(client.crypto.rng(client.idLength()));
		assertEquals(numPeers-1, table.closestPeers(id, numPeers).size());
	}
	
	@Test
	public void testFreshenCallsLookupForRandomIdInEachStaleBucket() {
		fillTable(8*table.maxBucketCapacity());
		HashSet<DHTBucket> staleBuckets = new HashSet<>();
		int                i            = 0;
		
		// mark some of our buckets stale, note which ones
		for(DHTBucket bucket : table.buckets()) {
			boolean makeStale = i++ % 2               == 0
					         && bucket.peers().size() >  0;
			if(!makeStale) continue;
			
			staleBuckets.add(bucket);
			bucket.setFresh(0);
		}
		
		table.freshen();
		
		/* each bucket queries exactly one id if stale, or 0 if fresh */
		for(DHTID id : client.lookupIds) {
			boolean found = false;
			for(DHTBucket stale : staleBuckets) {
				if(!stale.includes(id)) continue;
				
				// remove bucket from set
				staleBuckets.remove(stale);
				found = true;
				break;
			}
			
			if(!found) fail("DHTID was queried from a non-stale bucket");
		}
		
		// shouldn't be any buckets left in the set
		assertEquals(0, staleBuckets.size());
	}
	
	@Test
	public void testFreshenCallsPruneForAllBuckets() {
		Util.setCurrentTimeMillis(0);
		
		fillTable(16*table.maxBucketCapacity());
		
		for(DHTPeer peer : table.allPeers()) {
			peer.missedMessage();
			peer.missedMessage();
		}
		
		table.freshen();
		for(DHTBucket bucket : table.buckets) {
			assertTrue(bucket.hasCapacity());
		}
	}
	
	@Test
	public void testSuggestPeerAddsPeerToBucketIfBucketHasCapacity() {
		DHTPeer peer = makeTestPeer(0);
		assertTrue(table.suggestPeer(peer));
		assertTrue(table.bucketForId(peer.id).peers().contains(peer));
	}
	
	@Test
	public void testSuggestPeerDoesNotAddsPeerToBucketIfBucketFull() {
		// ensure we have multiple buckets
		fillTable(2*table.maxBucketCapacity());
		
		DHTBucket bucket = null;
		
		// find a bucket that is non-splittable
		for(DHTBucket bkt : table.buckets) {
			if(bkt.includes(client.getId())) continue;
			
			bucket = bkt;
			break;
		}
		
		// bucket should be non-null, but let's just test
		if(bucket == null) fail("No non-splittable buckets");
		
		// make sure bucket is full
		while(bucket.hasCapacity()) {
			bucket.add(makeTestPeer(bucket.randomIdInRange()));
		}
		
		// inserting a peer should fail
		DHTPeer peer = makeTestPeer(bucket.randomIdInRange());
		assertFalse(table.suggestPeer(peer));
		assertFalse(bucket.peers().contains(peer));
		assertFalse(table.allPeers().contains(peer));
	}
	
	
	@Test
	public void testSuggestPeerRejectsSelf() throws UnknownHostException {
		DHTPeer self = new DHTPeer(client, "127.0.0.1", 0, table.client.getPublicKey());
		assertFalse (table.suggestPeer(self));
		assertEquals(0, table.allPeers().size());
		assertEquals(0, table.bucketForId(self.getId()).peers().size());
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
		client.getMaster().getGlobalConfig().set("net.dht.freshenIntervalMs", 5);
		Util.setCurrentTimeMillis(0);
		table.suggestPeer(makeTestPeer(0));
		table.close();
		
		Util.setCurrentTimeMillis(table.bucketFreshenInterval());
		assert(client.lookupIds.isEmpty());
		table = new DHTRoutingTable(client);
		assertTrue(Util.waitUntil(50, ()->!client.lookupIds.isEmpty()));
	}
}
