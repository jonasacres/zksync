package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;

public class StoredAccessTest {
	ZKMaster master;

	@BeforeClass
	public static void beforeClass() throws IOException {
		Security.addProvider(new BouncyCastleProvider());
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@AfterClass
	public static void afterClass() throws IOException {
		ZKFSTest.restoreArgon2Costs();
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
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		master.storedAccess.storeArchiveAccess(archive, false);
		assertTrue(master.allConfigs().contains(archive.config));
		
		ZKMaster clone = ZKMaster.openTestVolume();
		assertTrue(clone.allConfigs().contains(archive.config));
		assertEquals(1, clone.allConfigs.size());
		assertFalse(clone.allConfigs.getLast().accessor.isSeedOnly());
		
		archive.close();
		clone.close();
	}
	
	@Test
	public void testStoreSeedOnly() throws IOException {
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		ArchiveAccessor roAccessor = archive.config.accessor.makeSeedOnly();
		ZKArchiveConfig roConfig = ZKArchiveConfig.openExisting(roAccessor, archive.config.archiveId);
		ZKArchive roArchive = roConfig.getArchive();

		master.storedAccess.storeArchiveAccess(archive, true);
		assertTrue(master.allConfigs().contains(archive.config));
		
		ZKMaster clone = ZKMaster.openTestVolume();
		assertTrue(clone.allConfigs().contains(roArchive.config));
		assertEquals(1, clone.allConfigs.size());
		assertTrue(clone.allConfigs.getLast().accessor.isSeedOnly());
		
		archive.close();
		roArchive.close();
		clone.close();
	}
	
	@Test
	public void testDeleteArchiveAccess() throws IOException {
		ZKArchive archiveA = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		ZKArchive archiveB = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		master.storedAccess.storeArchiveAccess(archiveA, false);
		master.storedAccess.storeArchiveAccess(archiveB, false);
		assertTrue(master.allConfigs().contains(archiveA.config));
		assertTrue(master.allConfigs().contains(archiveB.config));
		
		ZKMaster clone = ZKMaster.openTestVolume();
		assertTrue(clone.allConfigs().contains(archiveA.config));
		assertEquals(2, clone.allConfigs.size());
		
		master.storedAccess.deleteArchiveAccess(archiveA);
		assertFalse(master.allConfigs().contains(archiveA.config));
		assertTrue(master.allConfigs().contains(archiveB.config));
		
		clone.close();
		clone = ZKMaster.openTestVolume();
		assertFalse(clone.allConfigs().contains(archiveA.config));
		assertTrue(clone.allConfigs().contains(archiveB.config));
		
		clone.close();
		archiveA.close();
		archiveB.close();
	}
	
	@Test
	public void testPurge() throws IOException {
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		master.storedAccess.storeArchiveAccess(archive, false);
		assertTrue(master.allConfigs().contains(archive.config));
		master.storedAccess.purge();
		assertFalse(master.allConfigs().contains(archive.config));

		ZKMaster clone = ZKMaster.openTestVolume();
		assertFalse(clone.allConfigs().contains(archive.config));
		
		archive.close();
		clone.close();
}
}
