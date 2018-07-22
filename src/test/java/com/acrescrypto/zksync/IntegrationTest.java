package com.acrescrypto.zksync;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.net.dht.DHTPeer;
import com.acrescrypto.zksync.utility.Util;

public class IntegrationTest {
	DHTPeer root;
	DHTClient rootClient;
	CryptoSupport crypto;
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException, InvalidBlacklistException {
		crypto = new CryptoSupport();
		Blacklist blacklist = new Blacklist(new RAMFS(), "blacklist", new Key(crypto));
		rootClient = new DHTClient(new Key(crypto), blacklist);
		root = rootClient.getLocalPeer();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Test
	public void testArchiveIdDiscovery() throws IOException {
		ZKMaster[] masters = new ZKMaster[3];
		ZKArchive[] archives = new ZKArchive[3];
		
		// init some DHT-seeded archives with the same passphrase (openBlankTestVolume defaults the pp to "zksync")
		for(int i = 0; i < masters.length; i++) {
			masters[i] = ZKMaster.openBlankTestVolume("test"+i);
			archives[i] = masters[i].createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "test"+i);
			masters[i].listenOnTCP(0);
			masters[i].activateDHT("127.0.0.1", 0, root);
			masters[i].getTCPListener().advertise(archives[i].getConfig().getSwarm());
			archives[i].getConfig().getAccessor().discoverOnDHT();
		}
		
		// make a new master and look for that passphrase on the DHT and expect to find all of them
		ZKMaster blankMaster = ZKMaster.openBlankTestVolume("blank");
		blankMaster.activateDHT("127.0.0.1", 0, root);
		ArchiveAccessor accessor = blankMaster.makeAccessorForPassphrase("zksync".getBytes());
		accessor.discoverOnDHT();
		
		assertTrue(Util.waitUntil(3000, ()->blankMaster.allConfigs().size() >= masters.length));
		
		for(int i = 0; i < masters.length; i++) {
			archives[i].close();
			masters[i].close();
		}
	}
}
