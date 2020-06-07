package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigDefaults;

public class StoredAccessTest {
	ZKMaster master;

	@BeforeClass
	public static void beforeClass() throws IOException {
		Security.addProvider(new BouncyCastleProvider());
		TestUtils.startDebugMode();
		
		// disable dht so we don't have to worry about rebinding port numbers when cloning ZKMaster
		ConfigDefaults.getActiveDefaults().setDefault("net.dht.enabled", false);
	}
	
	@AfterClass
	public static void afterClass() throws IOException {
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	@Before
	public void before() throws IOException {
		master = ZKMaster.openBlankTestVolume();
	}
	
	@After
	public void after() throws IOException {
		master.close();
	}
	
	@Test
	public void testStoreArchive() throws IOException {
		ZKArchiveConfig config = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").config;
		master.storedAccess.storeArchiveAccess(config, StoredAccess.ACCESS_LEVEL_READWRITE);
		assertTrue(master.allConfigs().contains(config));
		
		ZKMaster clone = ZKMaster.openTestVolume();
		assertTrue(clone.allConfigs().contains(config));
		assertEquals(1, clone.allConfigs.size());
		assertFalse(clone.allConfigs.getLast().accessor.isSeedOnly());
		
		config.close();
		clone.close();
	}
	
	@Test
	public void testStoreSeedOnly() throws IOException {
		ZKArchiveConfig config = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").config;
		ArchiveAccessor roAccessor = config.accessor.makeSeedOnly();
		ZKArchiveConfig roConfig = ZKArchiveConfig.openExisting(roAccessor, config.archiveId);

		master.storedAccess.storeArchiveAccess(config, StoredAccess.ACCESS_LEVEL_SEED);
		assertTrue(master.allConfigs().contains(config));
		
		ZKMaster clone = ZKMaster.openTestVolume();
		assertTrue(clone.allConfigs().contains(roConfig));
		assertEquals(1, clone.allConfigs.size());
		assertTrue(clone.allConfigs.getLast().accessor.isSeedOnly());
		
		config.close();
		roConfig.close();
		clone.close();
	}
	
	@Test
	public void testStoreReadOnly() throws IOException {
		ZKArchiveConfig config = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").config;
		config.getArchive().openBlank().commitAndClose(); // we want a revtag since that's caused crashes in the past
		ZKArchiveConfig roConfig = ZKArchiveConfig.openExisting(config.accessor, config.archiveId);
		roConfig.clearWriteRoot();

		master.storedAccess.storeArchiveAccess(config, StoredAccess.ACCESS_LEVEL_READ);
		assertTrue(master.allConfigs().contains(config));
		
		ZKMaster clone = ZKMaster.openTestVolume();
		assertTrue(clone.allConfigs().contains(roConfig));
		assertEquals(1, clone.allConfigs.size());
		assertFalse(clone.allConfigs.getLast().accessor.isSeedOnly());
		assertTrue(clone.allConfigs.getLast().isReadOnly());
		
		config.close();
		roConfig.close();
		clone.close();
	}
	
	@Test
	public void testDeleteArchiveAccess() throws IOException {
		ZKArchiveConfig configA = master.createArchiveWithPassphrase(ZKArchive.DEFAULT_PAGE_SIZE, "", "pp0".getBytes()).config;
		ZKArchiveConfig configB = master.createArchiveWithPassphrase(ZKArchive.DEFAULT_PAGE_SIZE, "", "pp1".getBytes()).config;
		
		assertFalse(Arrays.equals(configA.getArchiveId(), configB.getArchiveId()));
		master.storedAccess.storeArchiveAccess(configA, StoredAccess.ACCESS_LEVEL_READWRITE);
		master.storedAccess.storeArchiveAccess(configB, StoredAccess.ACCESS_LEVEL_READWRITE);
		assertTrue(master.allConfigs().contains(configA));
		assertTrue(master.allConfigs().contains(configB));
		
		ZKMaster clone = ZKMaster.openTestVolume();
		assertTrue(clone.allConfigs().contains(configA));
		assertEquals(2, clone.allConfigs.size());
		
		master.storedAccess.deleteArchiveAccess(configA);
		assertTrue(master.allConfigs().contains(configA));
		assertTrue(master.allConfigs().contains(configB));
		
		clone.close();
		clone = ZKMaster.openTestVolume();
		assertFalse(clone.allConfigs().contains(configA));
		assertTrue(clone.allConfigs().contains(configB));
		
		clone.close();
		configA.close();
		configB.close();
	}
	
	@Test
	public void testPurge() throws IOException {
		try(
				ZKArchive       archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
				ZKArchiveConfig  config = archive.config;
		) {
			master.storedAccess.storeArchiveAccess(config, StoredAccess.ACCESS_LEVEL_READWRITE);
			assertTrue(master.allConfigs().contains(config));
			master.storedAccess.purge();
			assertFalse(master.allConfigs().contains(config));

			try(ZKMaster clone = ZKMaster.openTestVolume()) {
				assertFalse(clone.allConfigs().contains(config));
			}
		}
	}
}
