package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.TCPPeerAdvertisement;
import com.acrescrypto.zksync.net.dht.DHTClient.LookupCallback;
import com.acrescrypto.zksync.utility.Util;

public class DHTZKArchiveDiscoveryTest {
	public class DummyDHTClient extends DHTClient {
		DHTID searchId;
		LookupCallback callback;
		boolean initialized;
		DummyProtocolManager protocolManager;
		
		HashMap<DHTID, DHTRecord> records = new HashMap<>();
		
		public DummyDHTClient() { 
			initialized = true;
			super.protocolManager = this.protocolManager = new DummyProtocolManager(this);
		}
		
		public void close() {}
		
		@Override
		public boolean isInitialized() {
			return initialized;
		}
	}
	
	class DummyProtocolManager extends DHTProtocolManager {
		DummyDHTClient client;
		
		public DummyProtocolManager(DummyDHTClient client) {
			this.client = client;
		}

		@Override
		public void lookup(DHTID searchId, Key lookupKey, LookupCallback callback) {
			client.searchId = searchId;
			client.callback = callback;
		}
		
		@Override
		public void addRecord(DHTID recordId, Key lookupKey, DHTRecord record) {
			client.records.put(recordId, record);
		}
	}

	
	CryptoSupport crypto;
	ZKMaster master;
	ZKArchive archive;
	DummyDHTClient client;
	DHTZKArchiveDiscovery discovery;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@SuppressWarnings("deprecation")
	@Before
	public void beforeEach() throws IOException {
		crypto = CryptoSupport.defaultCrypto();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "test");
		client = new DummyDHTClient();
		master.setDHTClient(client);
		discovery = new DHTZKArchiveDiscovery();
		discovery.discoveryIntervalMs = 1;
		discovery.advertisementIntervalMs = 1;
	}
	
	@After
	public void afterEach() {
		client.close();
		archive.close();
		master.close();
		discovery.stopDiscoveringArchives(archive.getConfig().getAccessor());
		Util.setCurrentTimeMillis(-1);
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Test
	public void testDiscoverArchivesStartsDiscoveryThreadForArchive() {
		assertNull(client.searchId);
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertTrue(Util.waitUntil(50, ()->client.searchId != null));
	}
	
	@Test
	public void testDiscoveryThreadBlocksUntilDHTClientInitialized() {
		assertNull(client.searchId);
		client.initialized = false;
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertFalse(Util.waitUntil(50, ()->client.searchId != null));
		client.initialized = true;
		assertTrue(Util.waitUntil(50, ()->client.searchId != null));
	}
	
	@Test
	public void testDiscoveryThreadTerminatesWhenStopDiscoveringArchivesCalled() throws IOException {
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertTrue(Util.waitUntil(50, ()->client.searchId != null));
		discovery.stopDiscoveringArchives(archive.getConfig().getAccessor());
		Util.sleep(10);
		client.searchId = null;
		assertFalse(Util.waitUntil(50, ()->client.searchId != null));
	}
	
	@Test
	public void testDiscoveryThreadPerformsLookupForCurrentTemporalSeedID() {
		discovery.discoverArchives(archive.getConfig().getAccessor());
		for(int i = 0; i < 8; i++) {
			Util.setCurrentTimeMillis(i*ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS);
			assertTrue(Util.waitUntil(50, ()->client.searchId != null && Arrays.equals(archive.getConfig().getAccessor().temporalSeedId(0), client.searchId.rawId)));	
		}
	}
	
	@Test
	public void testDiscoveryThreadAddsDiscoveredAdvertisementsToSwarm() {
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertTrue(Util.waitUntil(50, ()->client.searchId != null));
		
		PublicDHKey pubKey = crypto.makePrivateDHKey().publicKey();
		byte[] encryptedArchiveId = archive.getConfig().getEncryptedArchiveId(pubKey.getBytes());
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(pubKey, "127.0.0.1", 12345, encryptedArchiveId);
		DHTRecord record = new DHTAdvertisementRecord(crypto, ad);
		
		client.callback.receivedRecord(record);
		assertTrue(archive.getConfig().getSwarm().knownAds().contains(ad));
	}
	
	@Test
	public void testDiscoveryThreadWaitsIntervalBetweenLookups() {
		discovery.discoveryIntervalMs = 100;
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertTrue(Util.waitUntil(discovery.discoveryIntervalMs, ()->client.searchId != null));
		
		for(int i = 0; i < 4; i++) {
			client.searchId = null;
			assertFalse(Util.waitUntil(discovery.discoveryIntervalMs-50, ()->client.searchId != null));
			
			// TODO: ITF here, 2020-06-07, 135bcbe 
			assertTrue(Util.waitUntil(60, ()->client.searchId != null));
		}
	}
	
	@Test
	public void testIsDiscoveringReturnsTrueIfArchiveBeingActivelyDiscovered() {
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertTrue(discovery.isDiscovering(archive.getConfig().getAccessor()));
	}
	
	@Test
	public void testIsDiscoveringReturnsFalseIfArchiveNotBeingActivelyDiscovered() {
		assertFalse(discovery.isDiscovering(archive.getConfig().getAccessor()));
		discovery.discoverArchives(archive.getConfig().getAccessor());
		discovery.stopDiscoveringArchives(archive.getConfig().getAccessor());
		assertFalse(discovery.isDiscovering(archive.getConfig().getAccessor()));
	}

	@Test
	public void testIsAdvertisingReturnsTrueIfArchiveBeingActivelyDiscovered() {
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertTrue(discovery.isAdvertising(archive.getConfig().getAccessor()));
	}
	
	@Test
	public void testIsAdvertisingReturnsFalseIfArchiveNotBeingActivelyDiscovered() {
		assertFalse(discovery.isAdvertising(archive.getConfig().getAccessor()));
		discovery.discoverArchives(archive.getConfig().getAccessor());
		discovery.stopDiscoveringArchives(archive.getConfig().getAccessor());
		assertFalse(discovery.isAdvertising(archive.getConfig().getAccessor()));
	}

	@Test
	public void testAdvertisementThreadListsSelfInDHTIfConfigFilePresentAndListenerOpen() throws IOException, UnconnectableAdvertisementException {
		Util.setCurrentTimeMillis(100*ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS);
		
		master.getGlobalConfig().set("net.swarm.enabled", true);
		archive.getConfig().advertise();
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertTrue(Util.waitUntil(100, ()->client.records.size() > 0));
		
		for(int i = -1; i <= 1; i++) {
			DHTID id = new DHTID(archive.getConfig().getAccessor().temporalSeedId(i));
			assertTrue(client.records.containsKey(id));
			assertTrue(client.records.get(id) instanceof DHTAdvertisementRecord);
			
			DHTAdvertisementRecord adRecord = (DHTAdvertisementRecord) client.records.get(id);
			assertTrue(adRecord.ad instanceof TCPPeerAdvertisement);
			TCPPeerAdvertisement ad = (TCPPeerAdvertisement) adRecord.ad;
			assertEquals(ad, master.getTCPListener().listenerForSwarm(archive.getConfig().getSwarm()).localAd());
		}
	}
	
	@Test
	public void testAdvertisementThreadWaitsIntervalBetweenListings() throws IOException {
		master.getGlobalConfig().set("net.swarm.enabled", true);
		discovery.advertisementIntervalMs = 50;
		archive.getConfig().advertise();
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertTrue(Util.waitUntil(discovery.advertisementIntervalMs, ()->!client.records.isEmpty()));
		
		long timeStart = Util.currentTimeMillis();
		
		for(int i = 0; i < 4; i++) {
			client.records.clear();
			assertTrue(Util.waitUntil(100+discovery.advertisementIntervalMs, ()->!client.records.isEmpty()));
			
			// these time-based tests seem rife with ITFs.
			long elapsed = Util.currentTimeMillis() - timeStart;
			int minimumTime = i * (discovery.advertisementIntervalMs - 10);
			assertTrue(elapsed > minimumTime);
		}
	}

	@Test
	public void testAdvertisementThreadDoesNotListSelfInDHTIfSocketListenerNotOpen() {
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertFalse(Util.waitUntil(50, ()->client.records.size() > 0));
	}
	
	@Test
	public void testAdvertisementThreadDoesNotListSelfInDHTIfSwarmListenerNotOpen() throws IOException {
		master.getGlobalConfig().set("net.swarm.enabled", true);
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertFalse(Util.waitUntil(50, ()->client.records.size() > 0));
	}
	
	@Test
	public void testAdvertisementThreadActivatesIfSwarmListenerAddedAfterLaunch() throws IOException {
		archive.getConfig().advertise();
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertFalse(Util.waitUntil(50, ()->client.records.size() > 0));
		master.getGlobalConfig().set("net.swarm.enabled", true);
		master.getTCPListener().advertise(archive.getConfig().getSwarm());
		assertTrue(Util.waitUntil(50, ()->client.records.size() > 0));
	}
	
	@Test
	public void testForceUpdateTriggersImmediateAdvertisement() throws IOException {
		master.getGlobalConfig().set("net.swarm.enabled", true);
		archive.getConfig().advertise();
		discovery.advertisementIntervalMs = 1000;
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertTrue(Util.waitUntil(50, ()->!client.records.isEmpty()));
		client.records.clear();
		discovery.forceUpdate(archive.getConfig().getAccessor());
		// TODO API: (itf) 511a8be+ linux 12/14/18 UniversalTests, assertion failed
		// and again 12/15, after doubling the timeout to 100... gonna try 500 and see.
		assertTrue(Util.waitUntil(500, ()->!client.records.isEmpty()));
	}
	
	@Test
	public void testForceUpdateTriggersImmediateDiscovery() throws IOException {
		master.getGlobalConfig().set("net.swarm.enabled", true);
		archive.getConfig().advertise();
		discovery.discoveryIntervalMs = 1000;
		discovery.discoverArchives(archive.getConfig().getAccessor());
		assertTrue(Util.waitUntil(discovery.discoveryIntervalMs, ()->client.searchId != null));
		client.searchId = null;
		discovery.forceUpdate(archive.getConfig().getAccessor());
		// TODO API: (itf) a215023+ linux 11/29/18 AllTests, assertion failed
		assertTrue(Util.waitUntil(500, ()->client.searchId != null));
	}
}
