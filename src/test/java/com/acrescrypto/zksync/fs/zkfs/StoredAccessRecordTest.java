package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.Key;

public class StoredAccessRecordTest {
	ZKMaster master;
	ZKArchiveConfig config;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		config = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").getConfig();
	}
	
	@After
	public void afterEach() {
		config.close();
		master.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Test
	public void testSerializationWithPassphrase() throws IOException {
		StoredAccessRecord record = new StoredAccessRecord(config, StoredAccess.ACCESS_LEVEL_READWRITE);
		StoredAccessRecord deserialized = new StoredAccessRecord(master, ByteBuffer.wrap(record.serialize()));
		assertEquals(StoredAccess.ACCESS_LEVEL_READWRITE, deserialized.accessLevel);
		assertTrue(Arrays.equals(config.getArchiveId(), deserialized.getConfig().getArchiveId()));
		assertTrue(Arrays.equals(config.getAccessor().passphraseRoot.getRaw(), deserialized.getConfig().getAccessor().passphraseRoot.getRaw()));
		assertTrue(Arrays.equals(config.getAccessor().seedRoot.getRaw(), deserialized.getConfig().getAccessor().seedRoot.getRaw()));
		deserialized.close();
	}
	
	@Test
	public void testSerializationWithoutPassphrase() throws IOException {
		config.accessor.becomeSeedOnly();
		StoredAccessRecord record = new StoredAccessRecord(config, StoredAccess.ACCESS_LEVEL_READWRITE);
		StoredAccessRecord deserialized = new StoredAccessRecord(master, ByteBuffer.wrap(record.serialize()));
		assertEquals(StoredAccess.ACCESS_LEVEL_READWRITE, deserialized.accessLevel);
		assertTrue(Arrays.equals(config.getArchiveId(), deserialized.getConfig().getArchiveId()));
		assertTrue(Arrays.equals(config.getAccessor().seedRoot.getRaw(), deserialized.getConfig().getAccessor().seedRoot.getRaw()));
		assertEquals(config.getAccessor().passphraseRoot, null);
		assertEquals(deserialized.getConfig().getAccessor().passphraseRoot, null);
		deserialized.close();
	}
	
	@Test
	public void testSerializationWithPassphraseSeedOnly() throws IOException {
		StoredAccessRecord record = new StoredAccessRecord(config, StoredAccess.ACCESS_LEVEL_SEED);
		StoredAccessRecord deserialized = new StoredAccessRecord(master, ByteBuffer.wrap(record.serialize()));
		assertEquals(StoredAccess.ACCESS_LEVEL_SEED, deserialized.accessLevel);
		assertTrue(Arrays.equals(config.getArchiveId(), deserialized.getConfig().getArchiveId()));
		assertTrue(Arrays.equals(config.getAccessor().seedRoot.getRaw(), deserialized.getConfig().getAccessor().seedRoot.getRaw()));
		assertNotEquals(config.getAccessor().passphraseRoot, null);
		assertEquals(deserialized.getConfig().getAccessor().passphraseRoot, null);
		assertEquals(deserialized.getConfig().writeRoot, null);
		assertEquals(deserialized.getConfig().archiveRoot, null);
		deserialized.close();
	}
	
	@Test
	public void testSerializationWithWriteKey() throws IOException {
		Key archiveRoot = new Key(master.crypto), writeRoot = new Key(master.crypto);
		ZKArchiveConfig configWrite = ZKArchiveConfig.create(config.accessor, "", ZKArchive.DEFAULT_PAGE_SIZE, archiveRoot, writeRoot);
		StoredAccessRecord record = new StoredAccessRecord(configWrite, StoredAccess.ACCESS_LEVEL_READWRITE);
		StoredAccessRecord deserialized = new StoredAccessRecord(master, ByteBuffer.wrap(record.serialize()));
		
		assertArrayEquals(configWrite.archiveId, deserialized.getConfig().archiveId);
		assertArrayEquals(configWrite.accessor.passphraseRoot.getRaw(), deserialized.getConfig().accessor.passphraseRoot.getRaw());
		assertArrayEquals(configWrite.archiveRoot.getRaw(), deserialized.getConfig().archiveRoot.getRaw());
		assertArrayEquals(configWrite.writeRoot.getRaw(), deserialized.getConfig().writeRoot.getRaw());
		
		config.archive.close();
	}
}
