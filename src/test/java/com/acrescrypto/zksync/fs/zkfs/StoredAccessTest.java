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
		assertTrue(master.allArchives().contains(archive));
		
		ZKMaster clone = ZKMaster.openTestVolume();
		assertTrue(clone.allArchives().contains(archive));
		assertEquals(1, clone.allArchives.size());
		assertFalse(clone.allArchives.getLast().config.accessor.isSeedOnly());
		
		archive.close();
		clone.close();
	}
	
	@Test
	public void testStoreSeedOnly() throws IOException {
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		ArchiveAccessor roAccessor = archive.config.accessor.makeSeedOnly();
		ZKArchiveConfig roConfig = new ZKArchiveConfig(roAccessor, archive.config.archiveId);
		ZKArchive roArchive = roConfig.getArchive();

		master.storedAccess.storeArchiveAccess(archive, true);
		assertTrue(master.allArchives().contains(archive));
		
		ZKMaster clone = ZKMaster.openTestVolume();
		assertTrue(clone.allArchives().contains(roArchive));
		assertEquals(1, clone.allArchives.size());
		assertTrue(clone.allArchives.getLast().config.accessor.isSeedOnly());
		
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
		assertTrue(master.allArchives().contains(archiveA));
		
		ZKMaster clone = ZKMaster.openTestVolume();
		assertTrue(clone.allArchives().contains(archiveA));
		assertEquals(2, clone.allArchives.size());
		
		master.storedAccess.deleteArchiveAccess(archiveA);
		assertFalse(master.allArchives().contains(archiveA));
		assertTrue(master.allArchives().contains(archiveB));
		
		clone.close();
		clone = ZKMaster.openTestVolume();
		assertFalse(clone.allArchives().contains(archiveA));
		assertTrue(clone.allArchives().contains(archiveB));
		
		clone.close();
		archiveA.close();
		archiveB.close();
	}
	
	@Test
	public void testPurge() throws IOException {
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		master.storedAccess.storeArchiveAccess(archive, false);
		assertTrue(master.allArchives().contains(archive));
		master.storedAccess.purge();
		assertFalse(master.allArchives().contains(archive));

		ZKMaster clone = ZKMaster.openTestVolume();
		assertFalse(clone.allArchives().contains(archive));
		
		archive.close();
		clone.close();
}
}
