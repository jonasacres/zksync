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
	ZKArchive archive;
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
	}
	
	@After
	public void afterEach() {
		archive.close();
		master.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Test
	public void testSerializationWithPassphrase() throws IOException {
		StoredAccessRecord record = new StoredAccessRecord(archive, false);
		StoredAccessRecord deserialized = new StoredAccessRecord(master, ByteBuffer.wrap(record.serialize()));
		assertFalse(deserialized.seedOnly);
		assertTrue(Arrays.equals(archive.getConfig().getArchiveId(), deserialized.archive.getConfig().getArchiveId()));
		assertTrue(Arrays.equals(archive.getConfig().getAccessor().passphraseRoot.getRaw(), deserialized.archive.getConfig().getAccessor().passphraseRoot.getRaw()));
		assertTrue(Arrays.equals(archive.getConfig().getAccessor().seedRoot.getRaw(), deserialized.archive.getConfig().getAccessor().seedRoot.getRaw()));
		deserialized.close();
	}
	
	@Test
	public void testSerializationWithoutPassphrase() throws IOException {
		archive.config.accessor.becomeSeedOnly();
		StoredAccessRecord record = new StoredAccessRecord(archive, false);
		StoredAccessRecord deserialized = new StoredAccessRecord(master, ByteBuffer.wrap(record.serialize()));
		assertTrue(deserialized.seedOnly);
		assertTrue(Arrays.equals(archive.getConfig().getArchiveId(), deserialized.archive.getConfig().getArchiveId()));
		assertTrue(Arrays.equals(archive.getConfig().getAccessor().seedRoot.getRaw(), deserialized.archive.getConfig().getAccessor().seedRoot.getRaw()));
		assertEquals(archive.getConfig().getAccessor().passphraseRoot, null);
		assertEquals(deserialized.archive.getConfig().getAccessor().passphraseRoot, null);
		deserialized.close();
	}
	
	@Test
	public void testSerializationWithPassphraseSeedOnly() throws IOException {
		StoredAccessRecord record = new StoredAccessRecord(archive, true);
		StoredAccessRecord deserialized = new StoredAccessRecord(master, ByteBuffer.wrap(record.serialize()));
		assertTrue(deserialized.seedOnly);
		assertTrue(Arrays.equals(archive.getConfig().getArchiveId(), deserialized.archive.getConfig().getArchiveId()));
		assertTrue(Arrays.equals(archive.getConfig().getAccessor().seedRoot.getRaw(), deserialized.archive.getConfig().getAccessor().seedRoot.getRaw()));
		assertNotEquals(archive.getConfig().getAccessor().passphraseRoot, null);
		assertEquals(deserialized.archive.getConfig().getAccessor().passphraseRoot, null);
		assertEquals(deserialized.archive.config.writeRoot, null);
		assertEquals(deserialized.archive.config.archiveRoot, null);
		deserialized.close();
	}
	
	@Test
	public void testSerializationWithWriteKey() throws IOException {
		Key archiveRoot = new Key(master.crypto), writeRoot = new Key(master.crypto);
		ZKArchiveConfig config = ZKArchiveConfig.create(archive.config.accessor, "", ZKArchive.DEFAULT_PAGE_SIZE, archiveRoot, writeRoot);
		StoredAccessRecord record = new StoredAccessRecord(config.archive, false);
		StoredAccessRecord deserialized = new StoredAccessRecord(master, ByteBuffer.wrap(record.serialize()));
		
		assertArrayEquals(config.archiveId, deserialized.archive.config.archiveId);
		assertArrayEquals(config.accessor.passphraseRoot.getRaw(), deserialized.archive.config.accessor.passphraseRoot.getRaw());
		assertArrayEquals(config.archiveRoot.getRaw(), deserialized.archive.config.archiveRoot.getRaw());
		assertArrayEquals(config.writeRoot.getRaw(), deserialized.archive.config.writeRoot.getRaw());
		
		config.archive.close();
	}
}
