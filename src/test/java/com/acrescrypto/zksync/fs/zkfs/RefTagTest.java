package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
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
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		crypto = CryptoSupport.defaultCrypto();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		byte[] hash = crypto.hash("some data".getBytes());
		StorageTag storageTag = new StorageTag(crypto, hash);
		tag = new RefTag(archive, storageTag, RefTag.REF_TYPE_2INDIRECT, Long.MAX_VALUE);
	}
	
	@After
	public void afterEach() {
		master.close();
		archive.close();
		Util.setCurrentTimeMillis(-1);
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testConstructFromArchiveAndHash() throws IOException {
		byte[] hash = crypto.hash("some data".getBytes());
		StorageTag storageTag = new StorageTag(crypto, hash);
		RefTag newTag = new RefTag(archive, storageTag, RefTag.REF_TYPE_INDIRECT, 1234);
		
		assertEquals(storageTag, newTag.getStorageTag());
		assertEquals(RefTag.REF_TYPE_INDIRECT, newTag.getRefType());
		assertEquals(1234, newTag.getNumPages());
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
	public void testSerialization() {
		byte[] serialized = tag.serialize();
		RefTag tag2 = new RefTag(archive, serialized);
		
		assertEquals(tag.storageTag, tag2.storageTag);
		assertEquals(tag.archiveType, tag2.archiveType);
		assertEquals(tag.versionMajor, tag2.versionMajor);
		assertEquals(tag.versionMinor, tag2.versionMinor);
		assertEquals(tag.refType, tag2.refType);
		assertEquals(tag.numPages, tag2.numPages);
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
		byte[] hash = crypto.hash("other data".getBytes());
		StorageTag otherStorageTag = new StorageTag(crypto, hash);
		RefTag clone = new RefTag(tag.config,
				otherStorageTag,
				tag.getRefType(),
				tag.getNumPages());
		assertNotEquals(tag, clone);
	}
	
	@Test
	public void testEqualsRegistersDifferenceForRefType() {
		RefTag clone = new RefTag(tag.config, tag.getStorageTag(), 1 + tag.getRefType(), tag.getNumPages());
		assertNotEquals(tag, clone);
	}

	@Test
	public void testEqualsRegistersDifferenceForNumPages() {
		RefTag clone = new RefTag(tag.config, tag.getStorageTag(), tag.getRefType(), 1 + tag.getNumPages());
		assertNotEquals(tag, clone);
	}
	
	@Test
	public void testCompareToReturnsNegativeIfLeftHasLowerTagThanRight() {
		RefTag left = new RefTag(archive, tag.serialize());
		byte[] rightBytes = new byte[crypto.hashLength()];
		System.arraycopy(tag.getStorageTag().getTagBytes(), 0, rightBytes, 0, rightBytes.length);
		
		for(int i = 0; i < rightBytes.length; i++) {
			if(rightBytes[i] == 0xff) continue;
			rightBytes[i]++;
			break;
		}
		
		StorageTag rightStorageTag = new StorageTag(crypto, rightBytes);
		RefTag right = new RefTag(archive, rightStorageTag, tag.getRefType(), tag.getNumPages());
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
		byte[] leftBytes = new byte[crypto.hashLength()];
		System.arraycopy(tag.getStorageTag().getTagBytes(), 0, leftBytes, 0, leftBytes.length);
		
		for(int i = 0; i < leftBytes.length; i++) {
			if(leftBytes[i] == 0xff) continue;
			leftBytes[i]++;
			break;
		}
		
		StorageTag leftStorageTag = new StorageTag(crypto, leftBytes);
		RefTag left = new RefTag(archive, leftStorageTag, tag.getRefType(), tag.getNumPages());
		assertTrue(left.compareTo(right) > 0);
	}
}
