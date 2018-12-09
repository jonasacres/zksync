package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
		return new DHTPeer(owner, "127.0.0.1", dest.getPort(), dest.key.publicKey().getBytes());
	}
	
	ArrayList<DHTClient> makeClients(int numClients) throws IOException, InvalidBlacklistException {
		ArrayList<DHTClient> clients = new ArrayList<>();
		
		for(int i = 0; i < numClients; i++) {
			DummyMaster master = new DummyMaster();
			DHTClient client = new DHTClient(new Key(crypto), master);
			clients.add(client);
			client.addPeer(makePeer(root, client));
			client.listen(null, 0);
			client.findPeers();
		}
		
		for(DHTClient client : clients) {
			assertTrue(Util.waitUntil(2000, ()->client.isInitialized()));
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
		TCPPeerAdvertisement.disableReachabilityTest = true;
		// DHTRoutingTable.freshenIntervalMs = 400;
		DHTClient.messageExpirationTimeMs = 500;
		DHTClient.messageRetryTimeMs = 50;
		DHTClient.socketCycleDelayMs = 50;
		DHTClient.socketOpenFailCycleDelayMs = 100;
		DHTClient.lookupResultMaxWaitTimeMs = 50;
	}
	
	@Before
	public void beforeEach() throws IOException, InvalidBlacklistException {
		crypto = CryptoSupport.defaultCrypto();
		master = ZKMaster.openBlankTestVolume();
		root = master.getDHTClient();
		root.listen(null, 0);
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
		TestUtils.assertTidy();
		TCPPeerAdvertisement.disableReachabilityTest = false;
		DHTRoutingTable.freshenIntervalMs = DHTRoutingTable.DEFAULT_FRESHEN_INTERVAL_MS;
		DHTClient.messageExpirationTimeMs = DHTClient.DEFAULT_MESSAGE_EXPIRATION_TIME_MS;
		DHTClient.messageRetryTimeMs = DHTClient.DEFAULT_MESSAGE_RETRY_TIME_MS;
		DHTClient.socketCycleDelayMs = DHTClient.DEFAULT_SOCKET_CYCLE_DELAY_MS;
		DHTClient.socketOpenFailCycleDelayMs = DHTClient.DEFAULT_SOCKET_OPEN_FAIL_CYCLE_DELAY_MS;
		DHTClient.lookupResultMaxWaitTimeMs = DHTClient.DEFAULT_LOOKUP_RESULT_MAX_WAIT_TIME_MS;
	}
	
	@Test
	public void testPeerDiscovery() throws IOException, InvalidBlacklistException {
		ArrayList<DHTClient> clients = makeClients(256);
		DHTID id = new DHTID(crypto.rng(crypto.hashLength()));
		DHTRecord ad = makeBogusAd(0);
		Key lookupKey = new Key(crypto);
		
		for(int i = 0; i < 16; i++) {
			clients.get(i).addRecord(id, lookupKey, ad);
		}
		
		MutableBoolean finished = new MutableBoolean(), found = new MutableBoolean();
		
		for(int i = 0; i < 32; i++) {
			clients.get(i).lookup(id, lookupKey, (result)->{
				if(result == null) {
					finished.setTrue();
				} else {
					if(result.equals(ad)) found.setTrue();
				}
			});
		}
		
		assertTrue(Util.waitUntil(100, ()->finished.booleanValue()));
		assertTrue(found.toBoolean());
		
		for(DHTClient client : clients) {
			client.close();
			client.master.close();
		}
	}
}
