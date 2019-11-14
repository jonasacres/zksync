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

import org.apache.commons.lang3.mutable.MutableBoolean;
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
		TestUtils.startDebugMode();
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
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	public void setupPacked(boolean finish) throws IOException {
		ZKArchiveConfig config2 = ZKArchiveConfig.openExisting(config.accessor,
				config.getArchiveId(),
				false,
				config.writeRoot);
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
		byte[] storageTagBytes = crypto.hash(Util.serializeInt(0));
		StorageTag storageTag = new StorageTag(crypto, storageTagBytes);
		RefTag tag = new RefTag(archive, storageTag, RefTag.REF_TYPE_INDIRECT, 1);
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
		try(ZKFS fs = revTag.getFS()) {
			assertEquals(revTag, fs.baseRevision);
		}
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
		byte[] tagBytesA = new byte[crypto.hashLength()];
		byte[] tagBytesB = tagBytesA.clone();
		tagBytesB[0]++;
		StorageTag storageTagA = new StorageTag(crypto, tagBytesA);
		StorageTag storageTagB = new StorageTag(crypto, tagBytesB);
		
		RefTag aTag = new RefTag(config, storageTagA, RefTag.REF_TYPE_INDIRECT, 1);
		RefTag bTag = new RefTag(config, storageTagB, RefTag.REF_TYPE_INDIRECT, 1);
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
	
	@Test
	public void testHasStructureLocallyReturnsTrueIfInodeTableCachedForEmptyFS() throws IOException {
		assertTrue(revTag.hasStructureLocally());
	}

	@Test
	public void testHasStructureLocallyReturnsTrueIfInodeAndDirectoriesCached() throws IOException {
		fs.mkdir("dir");
		for(int i = 0; i < fs.inodeTable.numInodesForPage(1) + 1; i++) { // force the inode table and directory to each be multipage
			fs.write("dir/heresapathnamesamiam" + i, "foo".getBytes());
		}
		
		revTag = fs.commit();
		assertTrue(revTag.hasStructureLocally());
	}

	@Test
	public void testHasStructureLocallyReturnsFalseIfInodePageMissing() throws IOException {
		PageTree tree = new PageTree(revTag.getRefTag());
		String pagePath = tree.getPageTag(0).path();
		fs.getArchive().getStorage().unlink(pagePath);
		assertFalse(revTag.hasStructureLocally());
	}
	
	@Test
	public void testHasStructureLocallyReturnsFalseIfDirectoryPageMissing() throws IOException {
		fs.mkdir("dir");
		for(int i = 0; i < fs.inodeTable.numInodesForPage(1) + 1; i++) { // force the inode table and directory to each be multipage
			fs.write("dir/heresapathnamesamiam" + i, "foo".getBytes());
		}
		
		revTag = fs.commit();
		try(ZKDirectory dir = fs.opendir("dir")) {
			String path = dir.tree.getPageTag(0).path();
			fs.archive.getStorage().unlink(path);
		}
		
		assertFalse(revTag.hasStructureLocally());
	}
	
	@Test
	public void testWaitForStructureDoesNotBlockIfTimeoutIsZero() throws IOException {
		fs.mkdir("dir");
		for(int i = 0; i < fs.inodeTable.numInodesForPage(1) + 1; i++) { // force the inode table and directory to each be multipage
			fs.write("dir/heresapathnamesamiam" + i, "foo".getBytes());
		}
		
		revTag = fs.commit();
		try(ZKDirectory dir = fs.opendir("dir")) {
			String path = dir.tree.getPageTag(0).path();
			fs.archive.getStorage().unlink(path);
		}
		
		long now = System.currentTimeMillis();
		assertFalse(revTag.waitForStructure(0));
		
		// time shouldn't have moved more than 3ms (which could be a lot closer to 1ms depending on timing)
		assertEquals(now, System.currentTimeMillis(), 3);
	}
	
	@Test
	public void testWaitForStructureBlocksIfTimeoutIsNegative() throws IOException {
		fs.mkdir("dir");
		for(int i = 0; i < fs.inodeTable.numInodesForPage(1) + 1; i++) { // force the inode table and directory to each be multipage
			fs.write("dir/heresapathnamesamiam" + i, "foo".getBytes());
		}
		
		String path;
		revTag = fs.commit();
		try(ZKDirectory dir = fs.opendir("dir")) {
			path = dir.tree.getPageTag(0).path();
			fs.archive.getStorage().mv(path, path + ".moved");
		}
		
		MutableBoolean returned = new MutableBoolean();
		new Thread(()->{
			try {
				assertTrue(revTag.waitForStructure(-1));
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}
			returned.setTrue();
		}).start();
		
		Util.sleep(500);
		assertFalse(returned.booleanValue());
		
		fs.archive.getStorage().mv(path + ".moved", path);
		fs.archive.getConfig().getSwarm().notifyPageUpdate();
		assertTrue(Util.waitUntil(1000, ()->returned.booleanValue()));
	}
	
	@Test
	public void testWaitForStructureTimesOutAfterRequestedPositiveInterval() throws IOException {
		int delayMs = 100;
		fs.mkdir("dir");
		for(int i = 0; i < fs.inodeTable.numInodesForPage(1) + 1; i++) { // force the inode table and directory to each be multipage
			fs.write("dir/heresapathnamesamiam" + i, "foo".getBytes());
		}
		
		String path;
		revTag = fs.commit();
		try(ZKDirectory dir = fs.opendir("dir")) {
			path = dir.tree.getPageTag(0).path();
			fs.archive.getStorage().mv(path, path + ".moved");
		}
		
		MutableBoolean returned = new MutableBoolean();
		new Thread(()->{
			try {
				assertFalse(revTag.waitForStructure(delayMs));
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}
			returned.setTrue();
		}).start();
		
		long startTime = System.currentTimeMillis();
		assertTrue(Util.waitUntil(delayMs + 50, ()->returned.booleanValue()));
		assertTrue(System.currentTimeMillis() - startTime >= delayMs);
	}
	
	@Test
	public void testWaitForStructureReturnsFalseIfNotCached() throws IOException {
		fs.mkdir("dir");
		for(int i = 0; i < fs.inodeTable.numInodesForPage(1) + 1; i++) { // force the inode table and directory to each be multipage
			fs.write("dir/heresapathnamesamiam" + i, "foo".getBytes());
		}
		
		String path;
		revTag = fs.commit();
		try(ZKDirectory dir = fs.opendir("dir")) {
			path = dir.tree.getPageTag(0).path();
			fs.archive.getStorage().mv(path, path + ".moved");
		}
		
		assertFalse(revTag.waitForStructure(0));
	}
	
	@Test
	public void testWaitForStructureReturnsTrueIfPrecached() throws IOException {
		fs.mkdir("dir");
		for(int i = 0; i < fs.inodeTable.numInodesForPage(1) + 1; i++) { // force the inode table and directory to each be multipage
			fs.write("dir/heresapathnamesamiam" + i, "foo".getBytes());
		}
		
		revTag = fs.commit();
		assertTrue(revTag.waitForStructure(0));
	}
}
