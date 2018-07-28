package com.acrescrypto.zksync;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
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
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.net.dht.DHTPeer;
import com.acrescrypto.zksync.net.dht.DHTSearchOperation;
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
		rootClient.listen("127.0.0.1", 0);
		assertTrue(Util.waitUntil(50, ()->rootClient.getStatus() >= DHTClient.STATUS_QUESTIONABLE));
		root = rootClient.getLocalPeer();
	}
	
	@After
	public void afterEach() {
		DHTSearchOperation.searchQueryTimeoutMs = DHTSearchOperation.DEFAULT_SEARCH_QUERY_TIMEOUT_MS;
		rootClient.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Test
	public void testIntegratedDiscovery() throws IOException {
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
			
			ZKFS fs = archives[i].openBlank();
			fs.write("immediate", crypto.prng(archives[i].getConfig().getArchiveId()).getBytes(crypto.hashLength()-1));
			fs.write("1page", crypto.prng(archives[i].getConfig().getArchiveId()).getBytes(archives[i].getConfig().getPageSize()-1));
			fs.write("multipage", crypto.prng(archives[i].getConfig().getArchiveId()).getBytes(10*archives[i].getConfig().getPageSize()));
			fs.commit();
			fs.close();
		}
		
		// make a new master and look for that passphrase on the DHT and expect to find all of them
		ZKMaster blankMaster = ZKMaster.openBlankTestVolume("blank");
		blankMaster.listenOnTCP(0);
		blankMaster.activateDHT("127.0.0.1", 0, root);
		ArchiveAccessor accessor = blankMaster.makeAccessorForPassphrase("zksync".getBytes());
		accessor.discoverOnDHT();

		// wait for discovery of all the archive IDs
		assertTrue(Util.waitUntil(3000, ()->blankMaster.allConfigs().size() >= masters.length));
		
		// now see if we can actually get the expected data from each ID
		for(ZKArchiveConfig config : blankMaster.allConfigs()) {
			config.finishOpening();
			assertTrue(Util.waitUntil(3000, ()->config.getRevisionTree().plainBranchTips().size() > 0));
			ZKFS fs = config.getRevisionTree().plainBranchTips().get(0).getFS();
			
			byte[] expectedImmediate = crypto.prng(config.getArchiveId()).getBytes(crypto.hashLength()-1);
			byte[] expected1page = crypto.prng(config.getArchiveId()).getBytes(config.getPageSize()-1);
			byte[] expectedMultipage = crypto.prng(config.getArchiveId()).getBytes(10*config.getPageSize());
			
			assertArrayEquals(expectedImmediate, fs.read("immediate"));
			assertArrayEquals(expected1page, fs.read("1page"));
			assertArrayEquals(expectedMultipage, fs.read("multipage"));
			
			fs.close();
		}
		
		for(int i = 0; i < masters.length; i++) {
			archives[i].close();
			masters[i].close();
		}
	}
	
	@Test
	public void testSeedPeerIntegration() throws IOException {
		DHTSearchOperation.searchQueryTimeoutMs = 50; // let DHT lookups timeout quickly
		
		// first, make an original archive
		ZKMaster originalMaster = ZKMaster.openBlankTestVolume("original");
		ZKArchive originalArchive = originalMaster.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "an archive");
		originalMaster.listenOnTCP(0);
		originalMaster.activateDHT("127.0.0.1", 0, root);
		originalArchive.getConfig().getAccessor().discoverOnDHT();
		originalMaster.getTCPListener().advertise(originalArchive.getConfig().getSwarm());
		byte[] archiveId = originalArchive.getConfig().getArchiveId();

		byte[] expectedImmediate = crypto.prng(originalArchive.getConfig().getArchiveId()).getBytes(crypto.hashLength()-1);
		byte[] expected1page = crypto.prng(originalArchive.getConfig().getArchiveId()).getBytes(originalArchive.getConfig().getPageSize()-1);
		byte[] expectedMultipage = crypto.prng(originalArchive.getConfig().getArchiveId()).getBytes(10*originalArchive.getConfig().getPageSize());

		// populate it with some data
		ZKFS fs = originalArchive.openBlank();
		fs.write("immediate", expectedImmediate);
		fs.write("1page", expected1page);
		fs.write("multipage", expectedMultipage);
		fs.commit();
		fs.close();
		
		// now make a seed-only peer
		ZKMaster seedMaster = ZKMaster.openBlankTestVolume("seed");
		ArchiveAccessor seedAccessor = seedMaster.makeAccessorForRoot(originalArchive.getConfig().getAccessor().getSeedRoot(), true);
		seedMaster.listenOnTCP(0);
		seedMaster.activateDHT("127.0.0.1", 0, root);
		seedAccessor.discoverOnDHT();
		assertTrue(Util.waitUntil(3000, ()->seedAccessor.configWithId(archiveId) != null));
		
		// grab everything from the original
		ZKArchiveConfig seedConfig = seedAccessor.configWithId(archiveId);
		seedConfig.finishOpening();
		seedConfig.getSwarm().requestAll();
		assertTrue(Util.waitUntil(3000, ()->seedConfig.getArchive().allPageTags().size() == originalArchive.allPageTags().size()));
		seedMaster.getTCPListener().advertise(seedConfig.getSwarm());
		
		// original goes offline; now the seed is the only place to get data
		originalArchive.close();
		originalMaster.close();
		
		// now make another peer with the passphrase
		ZKMaster cloneMaster = ZKMaster.openBlankTestVolume("clone");
		Util.sleep(1000);
		cloneMaster.listenOnTCP(0);
		cloneMaster.activateDHT("127.0.0.1", 0, root);
		ArchiveAccessor cloneAccessor = cloneMaster.makeAccessorForPassphrase("zksync".getBytes());
		cloneAccessor.discoverOnDHT();
		assertTrue(Util.waitUntil(3000, ()->cloneAccessor.configWithId(archiveId) != null));
		
		// grab the archive from the network (i.e. the blind peer)
		ZKArchiveConfig cloneConfig = cloneAccessor.configWithId(archiveId);
		cloneConfig.finishOpening();
		Util.waitUntil(3000, ()->cloneConfig.getRevisionTree().plainBranchTips().size() > 0);
		ZKFS cloneFs = cloneConfig.getArchive().openRevision(cloneConfig.getRevisionTree().plainBranchTips().get(0));
		
		// make sure the data matches up
		assertArrayEquals(expectedImmediate, fs.read("immediate"));
		assertArrayEquals(expected1page, fs.read("1page"));
		assertArrayEquals(expectedMultipage, fs.read("multipage"));

		cloneFs.close();
		cloneConfig.getArchive().close();
		cloneMaster.close();
	}
}
