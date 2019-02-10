package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.FS;

public class ZKMasterTest {
	ZKMaster master;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
	}
	
	@After
	public void afterEach() throws IOException {
		master.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testGetCryptoReturnsCryptoSupport() {
		assertNotNull(master.getCrypto());
	}
	
	@Test
	public void testGetStorageReturnsStorage() {
		assertNotNull(master.getStorage());
	}
	
	@Test
	public void testCreateArchiveCreatesArchiveWithRequestedPageSize() throws IOException {
		int size = 12345;
		ZKArchive archive = master.createArchive(size, "");
		assertEquals(size, archive.getConfig().getPageSize());
		archive.close();
	}
	
	@Test
	public void testCreateArchiveCreatesArchiveWithRequestedDescription() throws IOException {
		String desc = "happy archive";
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, desc);
		assertEquals(desc, archive.getConfig().getDescription());
		archive.close();
	}
	
	@Test
	public void testCreateArchiveCreatesArchiveWithMatchedWriteRoot() throws IOException {
		String desc = "happy archive";
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, desc);
		assertArrayEquals(archive.getConfig().archiveRoot.getRaw(), archive.getConfig().writeRoot.getRaw());
		archive.close();
	}
	
	@Test
	public void testCreateArchiveWithPromptedWriteRootCreatesArchiveAppropriately() throws IOException {
		String desc = "happy archive";
		int size = 12345;
		ZKArchive archive = master.createArchiveWithWriteRoot(size, desc);
		assertEquals(size, archive.getConfig().getPageSize());
		assertEquals(desc, archive.getConfig().getDescription());
		assertFalse(Arrays.equals(archive.getConfig().archiveRoot.getRaw(), archive.getConfig().writeRoot.getRaw()));
		archive.close();
	}

	@Test
	public void testCreateArchiveWithWriteRootCreatesArchiveAppropriately() throws IOException {
		String desc = "happy archive";
		int size = 12345;
		Key readRoot = new Key(master.crypto);
		Key writeRoot = new Key(master.crypto);
		ZKArchive archive = master.createArchiveWithWriteRoot(size, desc, readRoot, writeRoot);
		assertEquals(size, archive.getConfig().getPageSize());
		assertEquals(desc, archive.getConfig().getDescription());
		assertArrayEquals(writeRoot.getRaw(), archive.getConfig().writeRoot.getRaw());
		archive.close();
	}

	@Test
	public void testStoragePathForArchiveIdReturnsPathSpecificToArchiveId() throws IOException {
		ZKArchive arch0 = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		ZKArchive arch1 = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		
		FS fs0 = master.storageFsForArchiveId(arch0.config.archiveId);
		FS fs1 = master.storageFsForArchiveId(arch1.config.archiveId);
		
		fs0.write("test0", "contents".getBytes());
		fs1.write("test1", "contents".getBytes());

		fs0 = master.storageFsForArchiveId(arch0.config.archiveId);
		fs1 = master.storageFsForArchiveId(arch1.config.archiveId);
		
		assertTrue(fs0.exists("test0"));
		assertFalse(fs0.exists("test1"));
		assertFalse(fs1.exists("test0"));
		assertTrue(fs1.exists("test1"));
		
		fs0.close();
		fs1.close();
		arch0.close();
		arch1.close();
	}
	
	@Test
	public void testScratchStorageReturnsStorageScopedToScratchDirectory() throws IOException {
		FS scratch = master.scratchStorage();
		scratch.write("test", "some data".getBytes());
		assertFalse(master.storage.exists("test"));
		assertTrue(master.scratchStorage().exists("test"));
		scratch.close();
	}
	
	@Test
	public void testAccessorForRootReturnsNullIfNoSuchAccessorExists() {
		assertNull(master.accessorForRoot(new Key(master.crypto)));
	}
	
	@Test
	public void testAccessorForRootReturnsAppropriateAccessorIfPresent() throws IOException {
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		assertEquals(archive.config.accessor, master.accessorForRoot(archive.config.accessor.passphraseRoot));
		
		Key seedKey = new Key(master.crypto);
		master.makeAccessorForRoot(seedKey, true);
		assertTrue(Arrays.equals(seedKey.getRaw(), master.accessorForRoot(seedKey).seedRoot.getRaw()));
		
		archive.close();
	}
	
	@Test
	public void testMakeAccessorForRootCreatesAccessorForSpecifiedRootKey() {
		Key ppKey = new Key(master.crypto);
		master.makeAccessorForRoot(ppKey, false);
		assertTrue(Arrays.equals(ppKey.getRaw(), master.accessorForRoot(ppKey).passphraseRoot.getRaw()));
	}
	
	@Test
	public void testMakeAccessorForRootCreatesAccessorForSpecifiedSeedKey() {
		Key seedKey = new Key(master.crypto);
		master.makeAccessorForRoot(seedKey, true);
		assertTrue(Arrays.equals(seedKey.getRaw(), master.accessorForRoot(seedKey).seedRoot.getRaw()));
	}
	
	@Test
	public void testLocalStorageForArchiveIdReturnsFSSpecificToArchiveSeparateFromData() throws IOException {
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		FS localFs = master.localStorageFsForArchiveId(archive.config.archiveId);
		
		assertFalse(localFs.exists("test"));
		localFs.write("test", "test".getBytes());
		
		assertTrue(master.localStorageFsForArchiveId(archive.config.archiveId).exists("test"));
		assertFalse(master.storageFsForArchiveId(archive.config.archiveId).exists("test"));
		assertFalse(master.scratchStorage().exists("test"));
		assertFalse(archive.storage.exists("test"));
		
		archive.close();
	}
	
	@Test
	public void testAllConfigsReturnsListOfAllArchiveConfigs() throws IOException {
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		assertTrue(master.allConfigs.contains(archive.config));
		archive.close();
	}
	
	@Test
	public void testDiscoveredArchiveConfigAddsToListOfAllConfigs() throws IOException {
		ZKMaster masterPrime = ZKMaster.openBlankTestVolume();
		ZKArchive archive = masterPrime.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		
		assertFalse(master.allConfigs.contains(archive.config));
		master.discoveredArchiveConfig(archive.config);
		assertTrue(master.allConfigs.contains(archive.config));
		
		masterPrime.close();
		archive.close();
	}
	
	@Test
	public void testDiscoveredArchiveConfigReplacesSeedOnlyReferencesWithReadAccess() throws IOException {
		ZKMaster masterPrime = ZKMaster.openBlankTestVolume();
		ZKArchive archive = masterPrime.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		
		ArchiveAccessor roAccessor = archive.config.accessor.makeSeedOnly();
		ZKArchiveConfig roConfig = ZKArchiveConfig.openExisting(roAccessor, archive.config.archiveId);
		ZKArchive roArchive = roConfig.getArchive();
		
		assertFalse(master.allConfigs.contains(archive.config));
		assertFalse(master.allConfigs.contains(roArchive.config));
		
		master.discoveredArchiveConfig(roArchive.config);
		assertFalse(master.allConfigs.contains(archive.config));
		assertTrue(master.allConfigs.contains(roArchive.config));
		
		master.discoveredArchiveConfig(archive.config);
		assertTrue(master.allConfigs.contains(archive.config));
		assertFalse(master.allConfigs.contains(roArchive.config));
		
		roArchive.close();
		archive.close();
		masterPrime.close();
	}
	
	@Test
	public void testDiscoveredArchiveConfigDoesNotCreateDuplicateEntries() throws IOException {
		ZKMaster masterPrime = ZKMaster.openBlankTestVolume();
		ZKArchive archive = masterPrime.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		
		assertEquals(0, master.allConfigs.size());
		
		master.discoveredArchiveConfig(archive.config);
		assertEquals(1, master.allConfigs.size());
		
		master.discoveredArchiveConfig(archive.config);
		assertEquals(1, master.allConfigs.size());
		
		archive.close();
		masterPrime.close();
	}
	
	@Test
	public void testRemovedArchiveConfigRemovesConfigFromListOfAllConfigs() throws IOException {
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		assertTrue(master.allConfigs().contains(archive.config));
		master.removedArchiveConfig(archive.config);
		assertFalse(master.allConfigs().contains(archive.config));
		archive.close();
	}
	
	@Test
	public void testRemovedArchiveConfigToleratesConfigsNotInList() throws IOException {
		ZKMaster masterPrime = ZKMaster.openBlankTestVolume();
		ZKArchive archivePrime = masterPrime.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");

		assertEquals(1, master.allConfigs().size());
		master.removedArchiveConfig(archivePrime.config);
		assertEquals(1, master.allConfigs().size());
		
		archivePrime.close();
		masterPrime.close();
		archive.close();
	}
	
	@Test
	public void testPurgeErasesStorage() throws IOException {
		for(int i = 0; i < 32; i++) {
			String path = "" + (i % 2) + "/" + (i % 3) + "/" + (i % 5) + "/" + i;
			master.storage.write(path, ("test"+i).getBytes());
		}
		
		assertNotEquals(0, master.storage.opendir("/").listRecursive().length);
		master.purge();
		assertEquals(0, master.storage.opendir("/").listRecursive().length);
	}
}
