package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.utility.Util;

public class RevisionTagTest {
	CryptoSupport crypto;
	ZKMaster master;
	ZKArchiveConfig config;
	ZKArchive archive;
	ZKFS fs;
	RevisionTag revTag;
	RevisionTag packed;
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		crypto = CryptoSupport.defaultCrypto();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createDefaultArchive();
		config = archive.config;
		fs = archive.openBlank();
		revTag = fs.commit();
	}
	
	@After
	public void afterEach() throws IOException {
		fs.close();
		archive.close();
		config.close();
		master.close();
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	public void setupPacked(boolean finish) throws IOException {
		ZKArchiveConfig config2 = ZKArchiveConfig.openExisting(config.accessor, config.getArchiveId(), false, config.writeRoot);
		config.close();
		config = config2;
		packed = new RevisionTag(config, revTag.serialize(), false);
		if(finish) config.finishOpening();
	}
	
	@Test
	public void testSizeForConfigMatchesSerializedLength() {
		assertEquals(revTag.getBytes().length, RevisionTag.sizeForConfig(config));
	}
	
	@Test
	public void testBlank() {
		RevisionTag blank = RevisionTag.blank(config);
		assertEquals(0, blank.getHeight());
		assertEquals(0, blank.getParentHash());
		assertTrue(blank.getRefTag().isBlank());
	}
	
	@Test
	public void testConstructFromRefTag() {
		RefTag tag = new RefTag(archive, crypto.rng(config.refTagSize()));
		RevisionTag revTag = new RevisionTag(tag, 1, 2);
		assertEquals(tag, revTag.getRefTag());
		assertEquals(1, revTag.getParentHash());
		assertEquals(2, revTag.getHeight());
		assertArrayEquals(revTag.serialized, revTag.serialize());
		assertFalse(revTag.cacheOnly);
		assertNull(revTag.info);
		assertNotEquals(0, revTag.hashCode);
	}
	
	@Test
	public void testConstructFromSerialization() {
		RevisionTag origTag = new RevisionTag(revTag.getRefTag(), 100, 200);
		RevisionTag cloneTag = new RevisionTag(config, origTag.getBytes(), true);
		assertEquals(origTag.getRefTag(), cloneTag.getRefTag());
		assertEquals(origTag.getParentHash(), cloneTag.getParentHash());
		assertEquals(origTag.getHeight(), cloneTag.getHeight());
		assertArrayEquals(origTag.getBytes(), cloneTag.serialized);
		assertFalse(cloneTag.cacheOnly);
		assertNull(cloneTag.info);
		assertNotEquals(0, cloneTag.hashCode);
	}
	
	@Test
	public void testConstructFromSerializationThrowsSecurityExceptionWhenTampered() {
		byte[] raw = revTag.getBytes().clone();
		for(int i = 0; i < 8*raw.length; i++) {
			int index = i/8;
			byte mask = (byte) (1 << (i & 0x07));
			
			raw[index] ^= mask;
			try {
				new RevisionTag(config, raw, true);
				fail("expected security exception");
			} catch(SecurityException exc) {
			}
			raw[index] ^= mask;
		}
	}
	
	@Test
	public void testMakeCacheOnlyReturnsCacheOnlyClone() throws IOException {
		RevisionTag cacheOnly = revTag.makeCacheOnly();
		assertFalse(revTag == cacheOnly);
		
		assertFalse(revTag.cacheOnly);
		assertTrue(cacheOnly.cacheOnly);
		
		assertFalse(revTag.getArchive().isCacheOnly());
		assertTrue(cacheOnly.getArchive().isCacheOnly());
	}
	
	@Test
	public void testGetFSReturnsAppropriateFilesystem() throws IOException {
		assertEquals(revTag, revTag.getFS().baseRevision);
	}
	
	@Test
	public void testCompareToComparesHeight() {
		RevisionTag a = new RevisionTag(revTag.getRefTag(), 100, 1);
		RevisionTag b = new RevisionTag(revTag.getRefTag(), 50, 2);
		assertTrue(a.compareTo(b) < 0);
		assertTrue(b.compareTo(a) > 0);
		assertTrue(a.compareTo(a) == 0);
		assertTrue(b.compareTo(b) == 0);
	}
	
	@Test
	public void testCompareToComparesHashes() {
		byte[] rawTagA = new byte[crypto.hashLength()];
		byte[] rawTagB = rawTagA.clone();
		rawTagB[0]++;
		
		RefTag aTag = new RefTag(config, rawTagA, RefTag.REF_TYPE_INDIRECT, 1);
		RefTag bTag = new RefTag(config, rawTagB, RefTag.REF_TYPE_INDIRECT, 1);
		RevisionTag a = new RevisionTag(aTag, 1, 1);
		RevisionTag b = new RevisionTag(bTag, 1, 1);
		
		assertTrue(a.compareTo(b) > 0);
		assertTrue(b.compareTo(a) < 0);
		assertTrue(a.compareTo(a) == 0);
		assertTrue(b.compareTo(b) == 0);
	}
	
	@Test
	public void testEquals() {
		RevisionTag clone = new RevisionTag(revTag.getRefTag(), revTag.getParentHash(), revTag.getHeight());
		assertTrue(clone.equals(revTag));
		assertTrue(revTag.equals(clone));
		assertTrue(clone.equals(clone));
		assertTrue(revTag.equals(revTag));
	}
	
	@Test
	public void testHashCode() {
		assertEquals(revTag.hashCode, revTag.hashCode());
		assertEquals(ByteBuffer.wrap(revTag.serialize()).getInt(), revTag.hashCode());
	}
	
	@Test
	public void testGetShortHash() {
		assertEquals(Util.shortTag(revTag.serialize()), revTag.getShortHash());
	}
	
	@Test
	public void testRevTagGetHeightUnpacks() throws IOException {
		setupPacked(true);
		assertFalse(packed.isUnpacked());
		assertEquals(revTag.getHeight(), packed.getHeight());
		assertTrue(packed.isUnpacked());
	}
	
	@Test
	public void testRevTagGetHeightReturnsNegativeOneIfPacked() throws IOException {
		setupPacked(false);
		assertEquals(-1, packed.getHeight());
		assertFalse(packed.isUnpacked());
	}

	@Test
	public void testRevTagGetParentHashUnpacks() throws IOException {
		setupPacked(true);
		assertFalse(packed.isUnpacked());
		assertEquals(revTag.getParentHash(), packed.getParentHash());
		assertTrue(packed.isUnpacked());
	}
	
	@Test
	public void testRevTagGetParentHashReturnsNegativeOneIfPacked() throws IOException {
		setupPacked(false);
		assertEquals(-1, packed.getParentHash());
		assertFalse(packed.isUnpacked());
	}

	@Test
	public void testRevTagGetRefTagUnpacks() throws IOException {
		setupPacked(true);
		assertFalse(packed.isUnpacked());
		assertArrayEquals(revTag.getRefTag().getBytes(), packed.getRefTag().getBytes());
		assertTrue(packed.isUnpacked());
	}
}
