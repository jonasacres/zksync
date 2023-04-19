package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PRNG;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigDefaults;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.net.TCPPeerAdvertisement;
import com.acrescrypto.zksync.utility.Util;

public class DHTModuleTest {
	class DummyMaster extends ZKMaster {
		public DummyMaster()
				throws IOException, InvalidBlacklistException {
			super();
			this.crypto = CryptoSupport.defaultCrypto();
			this.threadGroup = Thread.currentThread().getThreadGroup();
			this.storage = new RAMFS();
			this.blacklist = new Blacklist(storage, "blacklist", new Key(crypto));
			this.globalConfig = new ConfigFile(storage, "config.json");
			globalConfig.apply(ConfigDefaults.getActiveDefaults());
			setupBandwidth();
		}
		
		@Override
		public void close() {}
	}

	Blacklist blacklist;
	CryptoSupport crypto;
	DHTClient root;
	DHTPeer rootPeer;
	ZKMaster master;
	
	DHTPeer makePeer(DHTClient dest, DHTClient owner) {
		try {
			return new DHTPeer(owner, "127.0.0.1", dest.getPort(), dest.getPublicKey().getBytes());
		} catch(UnknownHostException exc) {
			fail();
			return null;
		}
	}
	
	ArrayList<DHTClient> makeClients(int numClients) throws IOException, InvalidBlacklistException {
		ArrayList<DHTClient> clients = new ArrayList<>();
		
		for(int i = 0; i < numClients; i++) {
			DummyMaster master = new DummyMaster();
			DHTClient client = new DHTClient(new Key(crypto), master);
			clients.add(client);
			client.routingTable.reset();
			client.addPeer(makePeer(root, client));
			client.listen(null, 0);
			client.getProtocolManager().findPeers();
		}
		
		for(DHTClient client : clients) {
			int maxSearchQueryWaitTimeMs = client.getMaster().getGlobalConfig().getInt("net.dht.maxSearchQueryWaitTimeMs");
			assertTrue(Util.waitUntil(maxSearchQueryWaitTimeMs + 1000,
					()->client.isInitialized()));
		}
		
		return clients;
	}
	
	DHTAdvertisementRecord makeBogusAd(int i) {
		PRNG prng = crypto.prng(ByteBuffer.allocate(4).putInt(i).array());
		PublicDHKey pubKey = crypto.makePublicDHKey(prng.getBytes(crypto.asymPublicDHKeySize()));
		byte[] encryptedArchiveId = prng.getBytes(crypto.hashLength());
		String addr = "10." + (i/(256*256)) + "." + ((i/256) % 256) + "." + (i%256);
		int port = prng.getInt() & 0xffff;
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(pubKey, addr, port, encryptedArchiveId);
		return new DHTAdvertisementRecord(crypto, ad);
	}
	
	@BeforeClass
	public static void beforeAll() {
		ConfigDefaults.getActiveDefaults().set("net.dht.enabled",                  false);
		ConfigDefaults.getActiveDefaults().set("net.dht.bootstrap.enabled",        false);
		ConfigDefaults.getActiveDefaults().set("net.dht.socketCycleDelayMs",          50);
		ConfigDefaults.getActiveDefaults().set("net.dht.socketOpenFailCycleDelayMs", 100);
		ConfigDefaults.getActiveDefaults().set("net.dht.messageExpirationTimeMs",    500);
		ConfigDefaults.getActiveDefaults().set("net.dht.messageRetryTimeMs",          50);
		ConfigDefaults.getActiveDefaults().set("net.dht.lookupResultMaxWaitTimeMs",   50);
		ConfigDefaults.getActiveDefaults().set("net.dht.searchQueryTimeoutMs",       100);
		ConfigDefaults.getActiveDefaults().set("net.dht.maxSearchQueryWaitTimeMs",  1000);
		
		TestUtils.startDebugMode();
		TCPPeerAdvertisement.disableReachabilityTest = true;
	}
	
	@Before
	public void beforeEach() throws IOException, InvalidBlacklistException {
		crypto = CryptoSupport.defaultCrypto();
		master = ZKMaster.openBlankTestVolume();
		root = master.getDHTClient();
		root.listen(null, 0);
		root.routingTable.reset();
		assertTrue(Util.waitUntil(100, ()->root.getStatus() >= DHTClient.STATUS_QUESTIONABLE));
	}
	
	@After
	public void afterEach() {
		master.close();
		root.close();
	}
	
	@AfterClass
	public static void afterAll() {
		Util.waitUntil(1000, ()->TestUtils.isTidy()); // add a ton of grace time to wrap up threads on this test
		ConfigDefaults.resetDefaults();
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
		TCPPeerAdvertisement.disableReachabilityTest = false;
	}
	
	@Test
	public void testPeerDiscovery() throws IOException, InvalidBlacklistException {
		// TODO Urgent: (itf) Linux 81cc346 2018-12-12 UniversalTests, assertion failed
		ArrayList<DHTClient> clients = makeClients(32);
		DHTID id = DHTID.withBytes(crypto.rng(crypto.hashLength()));
		DHTAdvertisementRecord ad = makeBogusAd(0);
		Key lookupKey = new Key(crypto);
		
		for(int i = 0; i < 16; i++) {
			clients.get(i).getProtocolManager().addRecord(id, lookupKey, ad);
		}
		
		MutableBoolean finished = new MutableBoolean(), found = new MutableBoolean();
		
		for(int i = 0; i < 32; i++) {
			clients.get(i).getProtocolManager().lookup(id, lookupKey, (result)->{
				if(result == null) {
					finished.setTrue();
				} else {
					if(result.asAd().asTcp().getPubKey().equals(ad.asTcp().getPubKey())) found.setTrue();
				}
			});
		}
		
		
		// TODO: (itf) linux 2023-04-19 UniversalTests 45a0081+ AssertionFailed
		assertTrue(Util.waitUntil(100, ()->finished.booleanValue()));
		assertTrue(found.toBoolean());
		
		for(DHTClient client : clients) {
			client.close();
			client.master.close();
		}
	}
}
