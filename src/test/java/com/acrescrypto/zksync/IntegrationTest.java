package com.acrescrypto.zksync;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PRNG;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.net.dht.DHTPeer;
import com.acrescrypto.zksync.net.dht.DHTSearchOperation;
import com.acrescrypto.zksync.utility.Util;

public class IntegrationTest {
	DHTPeer root;
	DHTClient rootClient;
	CryptoSupport crypto;
	ZKMaster rootMaster;
	
	public byte[] readFile(ZKFS fs, String path) {
		try {
			return fs.read(path);
		} catch(Exception exc) {
			return new byte[0];
		}
	}
	
	ZKArchive createPeeredArchive(ZKMaster master) throws IOException {
		ZKArchive archive = master.createDefaultArchive();
		activateDHT(master, archive.getConfig());
		return archive;
	}

	ZKArchive createPeeredWriteKeyArchive(ZKMaster master) throws IOException {
		ZKArchive archive = master.createArchiveWithWriteRoot(ZKArchive.DEFAULT_PAGE_SIZE >> 1, "write key test");
		activateDHT(master, archive.getConfig());
		return archive;
	}
	
	ZKArchive joinPeeredWriteKeyArchiveAsReader(ZKMaster master, ZKArchive existing) throws IOException {
		// TODO Release: (refactor) We need a better way to join an existing archive than this.
		ArchiveAccessor accessor = new ArchiveAccessor(master, existing.getConfig().getAccessor());
		ZKArchiveConfig config = ZKArchiveConfig.openExisting(accessor, existing.getConfig().getArchiveId(), false, null);
		activateDHT(master, config);
		assertTrue(Util.waitUntil(5000, ()->{
			Util.sleep(100);
			master.getDHTDiscovery().forceUpdate(config.getAccessor());
			return config.getSwarm().getConnections().size() > 0;
		}));
		config.finishOpening();
		return config.getArchive();
	}
	
	void activateDHT(ZKMaster master, ZKArchiveConfig config) throws IOException {
		master.getGlobalConfig().set("net.swarm.enabled", true);
		master.activateDHT("127.0.0.1", 0, root);
		config.advertise();
		config.getRevisionList().automergeDelayMs = 5;
		config.getRevisionList().maxAutomergeDelayMs = 100;
		config.getAccessor().discoverOnDHT();
		config.getRevisionList().setAutomerge(true);
		config.getSwarm().requestAll();
	}

	RevisionTag createInitialCommit(ZKArchive archive, int i) throws IOException {
		ZKFS fs = archive.openBlank();
		fs.write("immediate-"+i, crypto.prng(Util.serializeInt(i)).getBytes(crypto.hashLength()-1));
		fs.write("1page-"+i, crypto.prng(Util.serializeInt(i)).getBytes(archive.getConfig().getPageSize()));
		fs.write("multipage-"+i, crypto.prng(Util.serializeInt(i)).getBytes(10*archive.getConfig().getPageSize()));
		RevisionTag tag = fs.commit();
		fs.close();
		
		return tag;
	}
	
	void expectCommitData(ZKFS fs, int i) {
		byte[] expectedImmediate = crypto.prng(Util.serializeInt(i)).getBytes(crypto.hashLength()-1);
		byte[] expected1Page = crypto.prng(Util.serializeInt(i)).getBytes(fs.getArchive().getConfig().getPageSize());
		byte[] expectedMultipage = crypto.prng(Util.serializeInt(i)).getBytes(10*fs.getArchive().getConfig().getPageSize());
		
		assertArrayEquals(expectedImmediate, readFile(fs, "immediate-"+i));
		assertArrayEquals(expected1Page, readFile(fs, "1page-"+i));
		assertArrayEquals(expectedMultipage, readFile(fs, "multipage-"+i));
	}
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException, InvalidBlacklistException {
		crypto = CryptoSupport.defaultCrypto();
		rootMaster = ZKMaster.openBlankTestVolume();
		rootClient = rootMaster.getDHTClient();
		rootClient.listen("127.0.0.1", 0);
		assertTrue(Util.waitUntil(50, ()->rootClient.getStatus() >= DHTClient.STATUS_QUESTIONABLE));
		root = rootClient.getLocalPeer();
	}
	
	@After
	public void afterEach() {
		DHTSearchOperation.searchQueryTimeoutMs = DHTSearchOperation.DEFAULT_SEARCH_QUERY_TIMEOUT_MS;
		rootClient.close();
		rootMaster.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Test
	public void testIntegratedDiscovery() throws IOException {
		ZKMaster[] masters = new ZKMaster[3];
		ZKArchive[] archives = new ZKArchive[3];
		
		// init some DHT-seeded archives with the same passphrase (openBlankTestVolume defaults the pp to "zksync")
		for(int i = 0; i < masters.length; i++) {
			masters[i] = ZKMaster.openBlankTestVolume("test"+i);
			archives[i] = masters[i].createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "test"+i);
			masters[i].getGlobalConfig().set("net.swarm.enabled", true);
			masters[i].activateDHT("127.0.0.1", 0, root);
			archives[i].getConfig().advertise();
			
			ZKFS fs = archives[i].openBlank();
			fs.write("immediate", crypto.prng(archives[i].getConfig().getArchiveId()).getBytes(crypto.hashLength()-1));
			fs.write("1page", crypto.prng(archives[i].getConfig().getArchiveId()).getBytes(archives[i].getConfig().getPageSize()-1));
			fs.write("multipage", crypto.prng(archives[i].getConfig().getArchiveId()).getBytes(10*archives[i].getConfig().getPageSize()));
			fs.commit();
			fs.close();
		}
		
		// make a new master and look for that passphrase on the DHT and expect to find all of them
		ZKMaster blankMaster = ZKMaster.openBlankTestVolume("blank");
		blankMaster.getGlobalConfig().set("net.swarm.enabled", true);
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
		originalMaster.getGlobalConfig().set("net.swarm.enabled", true);
		originalMaster.activateDHT("127.0.0.1", 0, root);
		originalArchive.getConfig().advertise();
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
		seedMaster.getGlobalConfig().set("net.swarm.enabled", true);
		seedMaster.activateDHT("127.0.0.1", 0, root);
		seedAccessor.discoverOnDHT();
		assertTrue(Util.waitUntil(3000, ()->seedAccessor.configWithId(archiveId) != null));
		
		// grab everything from the original
		ZKArchiveConfig seedConfig = seedAccessor.configWithId(archiveId);
		seedConfig.finishOpeningFromSwarm(3000);
		seedConfig.getSwarm().requestAll();
		assertTrue(Util.waitUntil(3000, ()->seedConfig.getArchive().allPageTags().size() == originalArchive.allPageTags().size()));
		
		// original goes offline; now the seed is the only place to get data
		originalArchive.close();
		originalMaster.close();
		
		// now make another peer with the passphrase
		ZKMaster cloneMaster = ZKMaster.openBlankTestVolume("clone");
		Util.sleep(1000);
		cloneMaster.getGlobalConfig().set("net.swarm.enabled", true);
		cloneMaster.activateDHT("127.0.0.1", 0, root);
		ArchiveAccessor cloneAccessor = cloneMaster.makeAccessorForPassphrase("zksync".getBytes());
		cloneAccessor.discoverOnDHT();
		assertTrue(Util.waitUntil(3000, ()->cloneAccessor.configWithId(archiveId) != null));
		
		// grab the archive from the network (i.e. the blind peer)
		ZKArchiveConfig cloneConfig = cloneAccessor.configWithId(archiveId);
		cloneConfig.finishOpeningFromSwarm(3000);
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
		/* peers 1 ... k each commit to the archive
		 * each peer then discovers each other via the DHT and syncs via p2p
		 * all peers should converge to a common revision with a merge of all commits
		 */
		DHTSearchOperation.searchQueryTimeoutMs = 50; // let DHT lookups timeout quickly
		ZKMaster[] masters = new ZKMaster[8];
		ZKArchive[] archives = new ZKArchive[masters.length];
		
		for(int i = 0; i < masters.length; i++) {
			masters[i] = ZKMaster.openBlankTestVolume(""+i);
			archives[i] = createPeeredArchive(masters[i]);
			createInitialCommit(archives[i], i);
		}
		
		assertTrue(Util.waitUntil(10000, ()->{
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
				expectCommitData(mergedFs, j);
			}
			
			mergedFs.close();
		}
		
		for(int i = 0; i < masters.length; i++) {
			archives[i].close();
			masters[i].close();
		}
	}
	
	@Test
	public void testDefaultIntegrationWithDelayedUndevelopedPeer() throws IOException {
		/* peers 1 ... k-1 each commit and sync
		 * peer k then syncs (having no commits of its own) after everyone else converged
		 * peer k should get the common revision
		 */
		DHTSearchOperation.searchQueryTimeoutMs = 50; // let DHT lookups timeout quickly
		ZKMaster[] masters = new ZKMaster[8];
		ZKArchive[] archives = new ZKArchive[masters.length];
		
		for(int i = 0; i < masters.length; i++) {
			masters[i] = ZKMaster.openBlankTestVolume(""+i);
			archives[i] = createPeeredArchive(masters[i]);
			createInitialCommit(archives[i], i);
		}
		
		assertTrue(Util.waitUntil(10000, ()->{
			// wait for everyone to merge to the same revtag
			for(int i = 0; i < masters.length; i++) {
				if(archives[i].getConfig().getRevisionList().branchTips().size() > 1) return false;
				
				RevisionTag baseTag = archives[0].getConfig().getRevisionList().branchTips().get(0);
				RevisionTag tag = archives[i].getConfig().getRevisionList().branchTips().get(0);
				if(!tag.equals(baseTag)) return false;
			}
			
			return true;
		}));
		
		RevisionTag mergeTag = archives[0].getConfig().getRevisionList().branchTips().get(0);
		ZKMaster delayed = ZKMaster.openBlankTestVolume("delayed");
		ZKArchive delayedArchive = createPeeredArchive(delayed);
		assertTrue(Util.waitUntil(3000, ()->mergeTag.equals(delayedArchive.getConfig().getRevisionList().latest())));
		delayedArchive.close();
		delayed.close();
		
		for(int i = 0; i < masters.length; i++) {
			archives[i].close();
			masters[i].close();
		}
	}
	
	@Test
	public void testDefaultIntegrationWithSeparatelyDevelopedPeer() throws IOException {
		/* peers 1 ... k-1 each commit and sync
		 * peek k commits, waits to sync
		 * peer k then syncs after everyone else converged
		 * everyone should converge to a new common revision.
		 */
		DHTSearchOperation.searchQueryTimeoutMs = 50; // let DHT lookups timeout quickly
		ZKMaster[] masters = new ZKMaster[8];
		ZKArchive[] archives = new ZKArchive[masters.length];
		
		for(int i = 0; i < masters.length; i++) {
			masters[i] = ZKMaster.openBlankTestVolume(""+i);
			archives[i] = createPeeredArchive(masters[i]);
			createInitialCommit(archives[i], i);
		}
		
		assertTrue(Util.waitUntil(10000, ()->{
			// wait for everyone to merge to the same revtag
			for(int i = 0; i < masters.length; i++) {
				if(archives[i].getConfig().getRevisionList().branchTips().size() > 1) return false;
				
				RevisionTag baseTag = archives[0].getConfig().getRevisionList().branchTips().get(0);
				RevisionTag tag = archives[i].getConfig().getRevisionList().branchTips().get(0);
				if(!tag.equals(baseTag)) return false;
			}
			
			return true;
		}));
		
		RevisionTag mergeTag = archives[0].getConfig().getRevisionList().branchTips().get(0);
		ZKMaster separate = ZKMaster.openBlankTestVolume("delayed");
		ZKArchive separateArch = separate.createDefaultArchive();
		createInitialCommit(separateArch, masters.length);
		
		separate.getGlobalConfig().set("net.swarm.enabled", true);
		separate.activateDHT("127.0.0.1", 0, root);
		separate.getTCPListener().advertise(separateArch.getConfig().getSwarm());
		separateArch.getConfig().getAccessor().discoverOnDHT();
		separateArch.getConfig().getRevisionList().automergeDelayMs = archives[0].getConfig().getRevisionList().automergeDelayMs;
		separateArch.getConfig().getRevisionList().maxAutomergeDelayMs = archives[0].getConfig().getRevisionList().maxAutomergeDelayMs;
		separateArch.getConfig().getRevisionList().setAutomerge(true);
		separateArch.getConfig().getSwarm().requestAll();

		assertTrue(Util.waitUntil(30000, ()->{
			RevisionTag firstLatest = archives[0].getConfig().getRevisionList().latest();
			for(ZKArchive archive : archives) {
				if(!archive.getConfig().getRevisionList().latest().equals(firstLatest)) {
					return false;
				}
			}
			
			RevisionTag sepLatest = separateArch.getConfig().getRevisionList().latest();
			if(sepLatest.equals(mergeTag)) return false;
			return sepLatest.equals(firstLatest);
		}));
		
		ZKFS mergedFs = separateArch.openLatest();
		for(int i = 0; i <= masters.length; i++) {
			expectCommitData(mergedFs, i);
		}
		mergedFs.close();
		
		separateArch.close();
		separate.close();
		
		for(int i = 0; i < masters.length; i++) {
			archives[i].close();
			masters[i].close();
		}
	}
	
	@Test
	public void testDefaultRandomIntegration() throws InterruptedException, IOException, InvalidBlacklistException {
		/* N peers swarm up and just randomly commit stuff for a while.
		 * They should converge to a common revision.
		 */
		int workers = 4;
		LinkedList<Thread> threads = new LinkedList<>();
		ZKMaster[] masters = new ZKMaster[workers];
		ZKArchive[] archives = new ZKArchive[workers];
		final long endTime = Util.currentTimeMillis() + 30*1000;
		long[] maxHeights = new long[workers];
		
		for(int i = 0; i < workers; i++) {
			final int ii = i;
			Thread thread = new Thread(()->{
				PRNG prng = new PRNG();
				try {
					ZKMaster master = masters[ii] = ZKMaster.openBlankTestVolume(""+ii);
					ZKArchive archive = archives[ii] = createPeeredArchive(master);
					while(Util.currentTimeMillis() < endTime) {
						master.getDHTDiscovery().forceUpdate(archive.getConfig().getAccessor());
						long timeLeft = endTime - Util.currentTimeMillis();
						Util.sleep(Math.min(timeLeft, prng.getInt(1000)));
						ZKFS fs = archive.openLatest();
						fs.write("file"+(prng.getInt(4)), crypto.rng(prng.getInt(3*archive.getConfig().getPageSize())));
						maxHeights[ii] = fs.commit().getHeight();
						fs.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
					fail();
				}
			});
			threads.add(thread);
			thread.start();
		}
		
		for(Thread thread : threads) {
			thread.join();
		}
		
		long maxHeight = 0;
		for(long h : maxHeights) maxHeight = Math.max(h, maxHeight);
		
		assertTrue(Util.waitUntil(20000, ()->{
			RevisionTag firstLatest = archives[0].getConfig().getRevisionList().latest();
			for(ZKArchive archive : archives) {
				RevisionTag archLatest = archive.getConfig().getRevisionList().latest();
				if(!firstLatest.equals(archLatest)) return false;
			}
			return true;
		}));
		
		for(ZKArchive archive : archives) {
			// TODO EasySafe: (itf) 732b6523e+ linux 1/12/19 UniversalTests, assertion failed
			assertTrue(maxHeight <= archive.getConfig().getRevisionList().latest().getHeight());
			archive.close();
			archive.getMaster().close();
		}
	}
	
	@Test
	public void testWriteKeyIntegration() throws InterruptedException, IOException, InvalidBlacklistException {
		/* N peers swarm up and just randomly commit stuff for a while.
		 * They should converge to a common revision.
		 */
		int writers = 2;
		int readers = 4;
		int workers = writers + readers;
		
		LinkedList<Thread> threads = new LinkedList<>();
		ZKMaster[] masters = new ZKMaster[workers];
		ZKArchive[] archives = new ZKArchive[workers];
		final long endTime = Util.currentTimeMillis() + 10*1000;
		long[] maxHeights = new long[workers];
		
		for(int i = 0; i < workers; i++) {
			final int ii = i;
			Thread thread = new Thread(()->{
				PRNG prng = new PRNG();
				try {
					ZKMaster master = masters[ii] = ZKMaster.openBlankTestVolume(""+ii);
					ZKArchive archive;
					if(ii < writers) {
						 archive = archives[ii] = createPeeredWriteKeyArchive(master);
					} else {
						assertTrue(Util.waitUntil(1000, ()->archives[0] != null));
						archive = archives[ii] = joinPeeredWriteKeyArchiveAsReader(master, archives[0]);
						return;
					}
					
					while(Util.currentTimeMillis() < endTime) {
						master.getDHTDiscovery().forceUpdate(archive.getConfig().getAccessor());
						long timeLeft = endTime - Util.currentTimeMillis();
						Util.sleep(Math.min(timeLeft, prng.getInt(1000)));
						ZKFS fs = archive.openLatest();
						fs.write("file"+(prng.getInt(4)), crypto.rng(prng.getInt(3*archive.getConfig().getPageSize())));
						maxHeights[ii] = fs.commit().getHeight();
						fs.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
					fail();
				}
			});
			threads.add(thread);
			thread.start();
		}
		
		for(Thread thread : threads) {
			thread.join();
		}
		
		long maxHeight = 0;
		for(long h : maxHeights) maxHeight = Math.max(h, maxHeight);
		
		if(!Util.waitUntil(20000, ()->{
			RevisionTag firstLatest = archives[0].getConfig().getRevisionList().latest();
			for(ZKArchive archive : archives) {
				// TODO Urgent: (itf) 2018-12-17 UniversalTests Linux 656c833, NullPointerException
				RevisionTag archLatest = archive.getConfig().getRevisionList().latest();
				if(!firstLatest.equals(archLatest)) return false;
			}
			return true;
		})) {
			for(ZKArchive archive : archives) {
				archive.getConfig().getRevisionList().dump();
				archive.getConfig().getSwarm().dumpConnections();
			}
			System.exit(1);
		}
		
		for(ZKArchive archive : archives) {
			assertTrue(maxHeight <= archive.getConfig().getRevisionList().latest().getHeight());
			archive.close();
			archive.getMaster().close();
		}
	}
}
