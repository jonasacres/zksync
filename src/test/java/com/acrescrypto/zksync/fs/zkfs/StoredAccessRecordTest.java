package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StoredAccessRecordTest {
	ZKMaster master;
	ZKArchive archive;
	
	@Before
	public void beforeClass() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		Security.addProvider(new BouncyCastleProvider());
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
	}
	
	@After
	public void afterClass() throws IOException {
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
	}
}
