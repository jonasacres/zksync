package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.utility.Util;

public class RefTagTest {
	ZKArchive archive;
	ZKMaster master;
	CryptoSupport crypto;
	RefTag tag;
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		crypto = new CryptoSupport();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		byte[] hash = crypto.hash("some data".getBytes());
		tag = new RefTag(archive, hash, RefTag.REF_TYPE_2INDIRECT, Long.MAX_VALUE);
	}
	
	@After
	public void afterEach() {
		master.close();
		archive.close();
		Util.setCurrentTimeMillis(-1);
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testConstructFromArchiveAndHash() throws IOException {
		byte[] hash = crypto.hash("some data".getBytes());
		RefTag newTag = new RefTag(archive, hash, RefTag.REF_TYPE_INDIRECT, 1234);
		
		assertArrayEquals(hash, newTag.getHash());
		assertEquals(RefTag.REF_TYPE_INDIRECT, newTag.getRefType());
		assertEquals(1234, newTag.getNumPages());
		assertEquals(archive, newTag.getArchive());
		assertEquals(archive.config, newTag.getConfig());
	}
	
	@Test
	public void testConstructFromConfigAndHash() throws IOException {
		byte[] hash = crypto.hash("some data".getBytes());
		RefTag newTag = new RefTag(archive.config, hash, RefTag.REF_TYPE_2INDIRECT, Long.MAX_VALUE);
		
		assertArrayEquals(hash, newTag.getHash());
		assertEquals(RefTag.REF_TYPE_2INDIRECT, newTag.getRefType());
		assertEquals(Long.MAX_VALUE, newTag.getNumPages());
		assertEquals(archive, newTag.getArchive());
		assertEquals(archive.config, newTag.getConfig());
	}
	
	@Test
	public void testConstructBlankFromArchive() throws IOException {
		RefTag tag = RefTag.blank(archive);
		assertTrue(tag.isBlank());
		assertEquals(archive, tag.getArchive());
		assertEquals(archive.config, tag.getConfig());
	}
	
	@Test
	public void testConstructBlankFromConfig() throws IOException {
		RefTag tag = RefTag.blank(archive.config);
		assertTrue(tag.isBlank());
		assertEquals(archive, tag.getArchive());
		assertEquals(archive.config, tag.getConfig());
	}
	
	@Test
	public void testSerialization() {
		tag.setFlag(RefTag.FLAG_PLACEHOLDER);
		
		byte[] serialized = tag.serialize();
		RefTag tag2 = new RefTag(archive, serialized);
		
		assertArrayEquals(tag.hash, tag2.hash);
		assertEquals(tag.archiveType, tag2.archiveType);
		assertEquals(tag.versionMajor, tag2.versionMajor);
		assertEquals(tag.versionMinor, tag2.versionMinor);
		assertEquals(tag.refType, tag2.refType);
		assertEquals(tag.numPages, tag2.numPages);
		assertEquals(tag.flags, tag2.flags);
		assertEquals(tag.config, tag2.config);
		assertArrayEquals(serialized, tag2.serialize());
	}
	
	@Test
	public void testEqualsMatchesForIdenticalTags() {
		RefTag clone = new RefTag(tag.config, tag.serialize());
		assertEquals(tag, clone);
	}
	
	@Test
	public void testEqualsToleratesNull() {
		assertNotEquals(null, tag);
		assertNotEquals(tag, null);
	}
	
	@Test
	public void testEqualsRegistersDifferenceForHash() {
		RefTag clone = new RefTag(tag.config, crypto.hash("other data".getBytes()), tag.getRefType(), tag.getNumPages());
		assertNotEquals(tag, clone);
	}
	
	@Test
	public void testEqualsRegistersDifferenceForRefType() {
		RefTag clone = new RefTag(tag.config, tag.getHash(), 1 + tag.getRefType(), tag.getNumPages());
		assertNotEquals(tag, clone);
	}

	@Test
	public void testEqualsRegistersDifferenceForNumPages() {
		RefTag clone = new RefTag(tag.config, tag.getHash(), tag.getRefType(), 1 + tag.getNumPages());
		assertNotEquals(tag, clone);
	}
	
	@Test
	public void testEqualsRegistersDifferenceForFlags() {
		RefTag clone = new RefTag(tag.config, tag.getHash(), tag.getRefType(), tag.getNumPages());
		tag.setFlag(RefTag.FLAG_PLACEHOLDER);
		assertNotEquals(tag, clone);
	}
	
	@Test
	public void testHasFlagReturnsTrueIfFlagSet() {
		tag.setFlag(RefTag.FLAG_PLACEHOLDER);
		assertTrue(tag.hasFlag(RefTag.FLAG_PLACEHOLDER));
	}
	
	@Test
	public void testHasFlagReturnsTrueIfFlagNotSet() {
		assertFalse(tag.hasFlag(RefTag.FLAG_PLACEHOLDER));
	}
	
	@Test
	public void testCompareToReturnsNegativeIfLeftHasLowerTagThanRight() {
		RefTag left = new RefTag(archive, tag.serialize());
		
		for(int i = 0; i < tag.hash.length; i++) {
			if(tag.hash[i] == 0xff) continue;
			tag.hash[i]++;
			break;
		}
		
		RefTag right = new RefTag(archive, tag.serialize());
		assertTrue(left.compareTo(right) < 0);
	}
	
	@Test
	public void testCompareToReturnsZeroIfLeftEqualsRight() {
		RefTag left = new RefTag(archive, tag.serialize());
		RefTag right = new RefTag(archive, tag.serialize());
		assertTrue(left.compareTo(right) == 0);
	}

	
	@Test
	public void testCompareToReturnsPositiveIfRightHasLowerTagThanLeft() {
		RefTag right = new RefTag(archive, tag.serialize());
		
		for(int i = 0; i < tag.hash.length; i++) {
			if(tag.hash[i] == 0xff) continue;
			tag.hash[i]++;
			break;
		}
		
		RefTag left = new RefTag(archive, tag.serialize());
		assertTrue(left.compareTo(right) > 0);
	}
}
