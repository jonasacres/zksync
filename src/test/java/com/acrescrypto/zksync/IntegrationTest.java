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
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.net.dht.DHTPeer;
import com.acrescrypto.zksync.net.dht.DHTSearchOperation;
import com.acrescrypto.zksync.utility.Util;

public class IntegrationTest {
	DHTPeer root;
	DHTClient rootClient;
	CryptoSupport crypto;
	
	public byte[] readFile(ZKFS fs, String path) {
		try {
			return fs.read(path);
		} catch(Exception exc) {
			return new byte[0];
		}
	}
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException, InvalidBlacklistException {
		crypto = new CryptoSupport();
		ZKMaster master = ZKMaster.openBlankTestVolume();
		rootClient = master.getDHTClient();
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
			assertTrue(Util.waitUntil(3000, ()->config.getRevisionList().branchTips().size() > 0));
			ZKFS fs = config.getRevisionList().branchTips().get(0).getFS();
			
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
		Util.waitUntil(3000, ()->cloneConfig.getRevisionList().branchTips().size() > 0);
		ZKFS cloneFs = cloneConfig.getArchive().openRevision(cloneConfig.getRevisionList().branchTips().get(0));
		
		// make sure the data matches up
		assertArrayEquals(expectedImmediate, fs.read("immediate"));
		assertArrayEquals(expected1page, fs.read("1page"));
		assertArrayEquals(expectedMultipage, fs.read("multipage"));

		cloneFs.close();
		cloneConfig.getArchive().close();
		cloneMaster.close();
	}
	
	@Test
	public void testDefaultArchiveIntegration() throws IOException {
		DHTSearchOperation.searchQueryTimeoutMs = 50; // let DHT lookups timeout quickly
		ZKMaster[] masters = new ZKMaster[8];
		ZKArchive[] archives = new ZKArchive[masters.length];
		ZKFS[] fs = new ZKFS[masters.length];
		
		for(int i = 0; i < masters.length; i++) {
			masters[i] = ZKMaster.openBlankTestVolume(""+i);
			archives[i] = masters[i].createDefaultArchive();

			masters[i].listenOnTCP(0);
			masters[i].activateDHT("127.0.0.1", 0, root);
			masters[i].getTCPListener().advertise(archives[i].getConfig().getSwarm());
			archives[i].getConfig().getAccessor().discoverOnDHT();
			archives[i].getConfig().getRevisionList().setAutomerge(true);
			archives[i].getConfig().getSwarm().requestAll();
			
			fs[i] = archives[i].openBlank();
			fs[i].write("immediate-"+i, crypto.prng(Util.serializeInt(i)).getBytes(crypto.hashLength()-1));
			fs[i].write("1page-"+i, crypto.prng(Util.serializeInt(i)).getBytes(archives[i].getConfig().getPageSize()));
			fs[i].write("multipage-"+i, crypto.prng(Util.serializeInt(i)).getBytes(10*archives[i].getConfig().getPageSize()));
			fs[i].commit();
		}
		
		assertTrue(Util.waitUntil(30000, ()->{
			// wait for everyone to merge to the same revtag
			for(int i = 0; i < masters.length; i++) {
				if(archives[i].getConfig().getRevisionList().branchTips().size() > 1) return false;
				
				RevisionTag baseTag = archives[0].getConfig().getRevisionList().branchTips().get(0);
				RevisionTag tag = archives[i].getConfig().getRevisionList().branchTips().get(0);
				if(!tag.equals(baseTag)) return false;
			}
			
			return true;
		}));
		
		for(int i = 0; i < masters.length; i++) {
			// can everyone get a correct copy of every file?
			ZKFS mergedFs = archives[i].openLatest();
			
			for(int j = 0; j < masters.length; j++) {
				final int jj = j;
				byte[] expectedImmediate = crypto.prng(Util.serializeInt(j)).getBytes(crypto.hashLength()-1);
				byte[] expected1Page = crypto.prng(Util.serializeInt(j)).getBytes(archives[j].getConfig().getPageSize());
				byte[] expectedMultipage = crypto.prng(Util.serializeInt(j)).getBytes(10*archives[j].getConfig().getPageSize());
				
				assertArrayEquals(expectedImmediate, readFile(mergedFs, "immediate-"+jj));
				assertArrayEquals(expected1Page, readFile(mergedFs, "1page-"+jj));
				assertArrayEquals(expectedMultipage, readFile(mergedFs, "multipage-"+jj));
			}
			
			mergedFs.close();
		}
		
		for(int i = 0; i < masters.length; i++) {
			fs[i].close();
			archives[i].close();
			masters[i].close();
		}
	}
}
