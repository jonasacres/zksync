package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.zkfs.PageTree.PageTreeStats;
import com.acrescrypto.zksync.utility.Shuffler;
import com.acrescrypto.zksync.utility.Util;

public class PageTreeTest {
	CryptoSupport crypto;
	ZKMaster master;
	ZKArchive archive;
	ZKFS fs;
	PageTree tree;
	RefTag immediateTag, indirectTag, doubleIndirectTag;
	RevisionTag revTag;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		crypto = CryptoSupport.defaultCrypto();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		fs = archive.openBlank();
		
		fs.write("immediate", "a bit of data".getBytes());
		fs.write("indirect", crypto.rng(1 + crypto.hashLength()));
		fs.write("2indirect", crypto.rng(1 + archive.config.pageSize));
		
		immediateTag = fs.inodeForPath("immediate").getRefTag();
		indirectTag = fs.inodeForPath("indirect").getRefTag();
		doubleIndirectTag = fs.inodeForPath("2indirect").getRefTag();
		revTag = fs.commit();
		
		tree = new PageTree(fs.inodeForPath("2indirect"));
		tree.commit();
	}
	
	@After
	public void afterEach() throws IOException {
		fs.close();
		archive.close();
		master.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	public StorageTag makeTag(int i) {
		return new StorageTag(crypto, crypto.hash(Util.serializeInt(i)));
	}
	
	@Test
	public void testConstructFromRevTagSetsFields() throws IOException {
		PageTree fromRevTag = new PageTree(revTag.getRefTag());
		
		assertEquals(archive, fromRevTag.archive);
		assertEquals(revTag.getRefTag(), fromRevTag.refTag);
		assertEquals(InodeTable.INODE_ID_INODE_TABLE, fromRevTag.inodeId);
		assertEquals(0, fromRevTag.inodeIdentity);
		
		assertEquals(1, fromRevTag.numChunks);
		assertEquals(fromRevTag.tagsPerChunk(), fromRevTag.maxNumPages);
		assertEquals(1, fromRevTag.numPages);
		
		assertNotNull(fromRevTag.chunkCache);
	}
	
	@Test
	public void testConstructFromInodeSetsFields() throws IOException {
		Inode inode = fs.inodeForPath("2indirect");
		PageTree fromInode = new PageTree(inode);
		
		assertEquals(archive, fromInode.archive);
		assertEquals(inode.getRefTag(), fromInode.refTag);
		assertEquals(inode.getStat().getInodeId(), fromInode.inodeId);
		assertEquals(inode.getIdentity(), fromInode.inodeIdentity);
		
		assertEquals(1, fromInode.numChunks);
		assertEquals(fromInode.tagsPerChunk(), fromInode.maxNumPages);
		assertEquals(2, fromInode.numPages);
		
		assertNotNull(fromInode.chunkCache);
	}
	
	@Test
	public void testSetsImmediateInodeRefTagsAppropriately() {
		assertEquals(RefTag.REF_TYPE_IMMEDIATE, immediateTag.refType);
	}

	@Test
	public void testSetsIndirectInodeRefTagsAppropriately() {
		assertEquals(RefTag.REF_TYPE_INDIRECT, indirectTag.refType);
	}

	@Test
	public void testSets2IndirectInodeRefTagsAppropriately() {
		assertEquals(RefTag.REF_TYPE_2INDIRECT, doubleIndirectTag.refType);
	}

	@Test
	public void testExistsReturnsTrueIfRefTagIsImmediate() throws IOException {
		assertTrue(new PageTree(immediateTag).exists());
	}
	
	@Test
	public void testExistsReturnsTrueIfRefTagIsIndirectAndPageExists() throws IOException {
		assertTrue(new PageTree(indirectTag).exists());
	}
	
	@Test
	public void testExistsReturnsFalseIfRefTagIsIndirectAndPageDoesNotExist() throws IOException {
		archive.storage.unlink(indirectTag.getStorageTag().path());
		assertFalse(new PageTree(indirectTag).exists());
	}
	
	@Test
	public void testExistsReturnsTrueIfRefTagIs2IndirectAndChunksAndPagesExist() throws IOException {
		assertTrue(tree.exists());
	}
	
	@Test
	public void testExistsReturnsFalseIfRefTagIs2IndirectAndChunksExistButPageMissing() throws IOException {
		archive.storage.unlink(tree.getPageTag(1).path());
		assertFalse(tree.exists());
	}
	
	@Test
	public void testExistsReturnsFalseIfRefTagIs2IndirectAndsPagesExistButChunkMissing() throws IOException {
		archive.storage.unlink(tree.chunkAtIndex(0).chunkTag.path());
		assertFalse(tree.exists());
	}
	
	@Test(expected=ENOENTException.class)
	public void testAssertExistsThrowsExceptionIfExistsReturnsFalse() throws IOException {
		archive.storage.unlink(tree.chunkAtIndex(0).chunkTag.path());
		tree.assertExists();
	}
	
	@Test
	public void testPageExistsReturnsTrueIfImmediateAndPageIsZero() throws IOException {
		assertTrue(new PageTree(fs.inodeForPath("indirect")).pageExists(0));
	}
	
	@Test
	public void testPageExistsReturnsFalseIfImmediateAndPageIsNonzero() throws IOException {
		assertFalse(new PageTree(fs.inodeForPath("indirect")).pageExists(1));
	}
	
	@Test
	public void testPageExistsReturnsTrueIfIndirectAndPagePresent() throws IOException {
		assertTrue(new PageTree(fs.inodeForPath("indirect")).pageExists(0));
	}
	
	@Test
	public void testPageExistsReturnsFalseIfIndirectAndPageIsNonzero() throws IOException {
		assertFalse(new PageTree(fs.inodeForPath("indirect")).pageExists(1));
	}
	
	@Test
	public void testPageExistsReturnsFalseIfIndirectAndPageNotPresent() throws IOException {
		PageTree tree = new PageTree(fs.inodeForPath("indirect"));
		archive.getStorage().unlink(tree.getPageTag(0).path());
		assertFalse(tree.pageExists(0));
	}
	
	@Test
	public void testPageExistsReturnsFalseIfDoubleIndirectAndChunkNotPresent() throws IOException {
		archive.storage.unlink(tree.chunkForPageNum(0).chunkTag.path());
		for(int i = 0; i < tree.numPages; i++) {
			assertFalse(tree.pageExists(i));
		}
	}

	@Test
	public void testPageExistsReturnsFalseIfDoubleIndirectAndPageNotPresent() throws IOException {
		for(int i = 0; i < tree.numPages; i++) {
			assertTrue(tree.pageExists(i));
			archive.storage.unlink(tree.getPageTag(i).path());
			assertFalse(tree.pageExists(i));
		}
	}
	
	@Test
	public void testPageExistsReturnsFalseIfDoubleIndirectAndPageExcessive() throws IOException {
		assertFalse(tree.pageExists(tree.numPages + 1));
	}
	
	@Test
	public void testPageExistsReturnsFalseIfDoubleIndirectAndPageNegative() throws IOException {
		assertFalse(tree.pageExists(-1));
	}
	
	@Test
	public void testPageExistsReturnsTrueIfDoubleIndirectWithChunkAndPagePresent() throws IOException {
		for(int i = 0; i < tree.numPages; i++) {
			assertTrue(tree.pageExists(i));
		}
	}
	
	@Test
	public void testAssertExistsDoesNotThrowExceptionIfExistsReturnsTrue() throws IOException {
		tree.assertExists();
	}
	
	@Test
	public void testGetRefTagReturnsRefTag() {
		assertEquals(doubleIndirectTag, tree.getRefTag());
	}
	
	@Test
	public void testSetPageTagReplacesExistingPageTags() throws IOException {
		StorageTag randomTag = makeTag(1234);
		tree.setPageTag(1, randomTag);
		assertEquals(randomTag, tree.getPageTag(1));
	}
	
	@Test
	public void testSetPageTagReplacesBlankPageTags() throws IOException {
		StorageTag randomTag = makeTag(1234);
		tree.setPageTag(tree.numPages, randomTag);
		assertEquals(randomTag, tree.getPageTag(tree.numPages-1));
	}
	
	@Test
	public void testSetPageTagAutomaticallyResizesTreeIfNeeded() throws IOException {
		assertEquals(1, tree.numChunks());
		tree.setPageTag(tree.tagsPerChunk(), makeTag(1));
		assertEquals(3, tree.numChunks());
		assertEquals(tree.tagsPerChunk()*tree.tagsPerChunk(), tree.maxNumPages);
	}

	@Test
	public void testGetPageTagReturnsTag() throws IOException {
		StorageTag tag = makeTag(4321);
		tree.setPageTag((int) (tree.tagsPerChunk()*0.5), tag);
		assertEquals(tag, tree.getPageTag((int) (tree.tagsPerChunk()*0.5)));
	}
	
	@Test
	public void testGetPageTagReturnsBlankTagIfTagHasNotBeenSet() throws IOException {
		StorageTag nullTag = archive.getBlankStorageTag();
		assertEquals(nullTag, tree.getPageTag(2));
	}
	
	@Test
	public void testGetNumChunksReturnsNumberOfChunks() throws IOException {
		assertEquals(tree.numChunks, tree.numChunks());
		tree.setPageTag(tree.tagsPerChunk(), archive.getBlankStorageTag());
		assertEquals(tree.numChunks, tree.numChunks());
	}
	
	@Test
	public void testGetStatsReturnsNumChunksCachedForImmediates() throws IOException {
		PageTree immTree = new PageTree(immediateTag);
		assertEquals(1, immTree.getStats().numCachedChunks);
	}
	
	@Test
	public void testGetStatsReturnsNumPagesCachedForImmediates() throws IOException {
		PageTree immTree = new PageTree(immediateTag);
		assertEquals(1, immTree.getStats().numCachedPages);
	}
	
	@Test
	public void testGetStatsReturnsNumChunksCachedForIndirects() throws IOException {
		PageTree indTree = new PageTree(indirectTag);
		assertEquals(1, indTree.getStats().numCachedPages);
	}
	
	@Test
	public void testGetStatsReturnsNumPagesCachedForIndirects() throws IOException {
		PageTree indTree = new PageTree(indirectTag);
		assertEquals(1, indTree.getStats().numCachedPages);
		
		StorageTag tag = indTree.getPageTag(0);
		indirectTag.getConfig().getCacheStorage().unlink(tag.path());
		indirectTag.getArchive().allPageTags.remove(tag.shortTagPreserialized());
		assertEquals(0, indTree.getStats().numCachedPages);
	}
	
	@Test
	public void testGetStatsReturnsPagesCachedForDoubleIndirects() throws IOException {
		ZKArchive smallPageArchive = master.createArchive(RevisionInfo.FIXED_SIZE, "adopt a tinypage now!");
		ZKFS smallPageFs = smallPageArchive.openBlank();
		int size = 512*1024;
		smallPageFs.write("test", new byte[size]);
		assertArrayEquals(new byte[size], smallPageFs.read("test"));
		
		PageTree diTree = new PageTree(smallPageFs.inodeForPath("test"));
		PageTreeStats stats = diTree.getStats();
		
		assertEquals(diTree.numPages, stats.totalPages);
		assertEquals(diTree.numPages, stats.numCachedPages);
		long expected = diTree.numPages;
		
		for(long i = 0; i < diTree.numPages; i++) {
			if(i % 2 == 0) continue;
			try {
				String path = diTree.getPageTag(i).path();
				smallPageArchive.getConfig().getCacheStorage().unlink(path);
				expected--;
			} catch(ENOENTException exc) {
				// some chunks are blank so they don't really exist
			}
		}
		
		smallPageArchive.rescanPageTags();
		
		stats = diTree.getStats();
		assertEquals(diTree.numPages, stats.totalPages);
		assertEquals(expected, stats.numCachedPages);
		smallPageFs.close();
	}

	@Test
	public void testGetStatsReturnsChunksCachedForDoubleIndirects() throws IOException {
		ZKArchive smallPageArchive = master.createArchive(RevisionInfo.FIXED_SIZE, "adopt a tinypage now!");
		try(ZKFS smallPageFs = smallPageArchive.openBlank()) {
			int size = 512*1024;
			smallPageFs.write("test", new byte[size]);
			assertArrayEquals(new byte[size], smallPageFs.read("test"));
			
			PageTree diTree = new PageTree(smallPageFs.inodeForPath("test"));
			PageTreeStats stats = diTree.getStats();
			
			assertEquals(diTree.numChunks, stats.totalChunks);
			assertEquals(diTree.numChunks, stats.numCachedChunks);
			long expected = diTree.numChunks;
			long levelSize = (long) (Math.log(stats.totalChunks)/Math.log(smallPageArchive.getConfig().tagsPerChunk));
			
			for(long i = diTree.numChunks - levelSize + 1; i < diTree.numChunks; i++) {
				if(i % 2 == 0) continue;
				try {
					String path = diTree.tagForChunk(i).path();
					smallPageArchive.getConfig().getCacheStorage().unlink(path);
					expected--;
				} catch(ENOENTException exc) {
					// some chunks are blank so they don't really exist
				}
			}
			
			smallPageArchive.rescanPageTags();
			
			stats = diTree.getStats();
			assertEquals(diTree.numChunks, stats.totalChunks);
			assertEquals(expected, stats.numCachedChunks);
		}
	}
	
	@Test
	public void testCommitWritesDirtyChunksIfDoublyIndirect() throws IOException {
		ArrayList<StorageTag> oldTags = new ArrayList<>();
		
		for(int pageNum = 0; pageNum < tree.numPages(); pageNum++) {
			oldTags.add(tree.getPageTag(pageNum));
			assertTrue(archive.config.getCacheStorage().exists(tree.getPageTag(pageNum).path()));
		}
		
		ZKFile file = fs.open("2indirect", File.O_WRONLY);
		file.seek(archive.config.pageSize, File.SEEK_SET);
		file.write("bleep".getBytes());
		file.close();
		tree = new PageTree(file.inode);
		
		for(int pageNum = 0; pageNum < tree.numPages(); pageNum++) {
			if(pageNum == 1) {
				assertNotEquals(oldTags.get(pageNum), tree.getPageTag(pageNum));
			} else {
				assertEquals(oldTags.get(pageNum), tree.getPageTag(pageNum));
			}
			
			oldTags.add(tree.getPageTag(pageNum));
			assertTrue(archive.config.getCacheStorage().exists(oldTags.get(pageNum).path()));
			assertTrue(archive.config.getCacheStorage().exists(tree.getPageTag(pageNum).path()));
		}
	}
	
	@Test
	public void testCommitDoesNotWriteChunksIfIndirect() throws IOException {
		PageTree indirectTree = new PageTree(fs.inodeForPath("indirect"));
		StorageTag oldTag = indirectTree.getPageTag(0);
		fs.write("indirect", crypto.rng(archive.config.pageSize));
		
		indirectTree = new PageTree(fs.inodeForPath("indirect"));
		assertEquals(indirectTree.getRefTag().getStorageTag(), indirectTree.getPageTag(0));
		assertNotEquals(oldTag, indirectTree.getPageTag(0));
		assertTrue(archive.config.getCacheStorage().exists(indirectTag.getStorageTag().path()));
	}
	
	@Test
	public void testCommitDoesNotWriteChunksIfImmediate() throws IOException {
		PageTree immediateTree = new PageTree(fs.inodeForPath("immediate"));
		assertEquals(immediateTag, immediateTree.refTag);
		assertEquals(immediateTag.getStorageTag(), immediateTree.getPageTag(0));
		assertFalse(archive.config.getCacheStorage().exists(immediateTag.getStorageTag().path()));
	}
	
	@Test
	public void testResizeDownwardToSameTreeHeight() throws IOException {
		int max = (int) (1.5*tree.tagsPerChunk());
		int cutoff = tree.tagsPerChunk() + 1;
		StorageTag nullTag = archive.getBlankStorageTag();
		
		for(int i = 0; i < max; i++) {
			tree.setPageTag(i, makeTag(i));
		}
		
		tree.resize(cutoff);
		assertEquals(max, tree.numPages);
		
		for(int i = 0; i < cutoff; i++) {
			assertEquals(makeTag(i), tree.getPageTag(i));
		}
		
		for(int i = cutoff; i < max; i++) {
			assertEquals(nullTag, tree.getPageTag(i));
		}
	}
	
	@Test
	public void testResizeUpwardToSameTreeHeight() throws IOException {
		int max = (int) (1.5*tree.tagsPerChunk());
		int cutoff = tree.tagsPerChunk() + 1;
		StorageTag nullTag = archive.getBlankStorageTag();
		
		for(int i = 0; i < cutoff; i++) {
			tree.setPageTag(i, makeTag(i));
		}
		
		tree.resize(max);
		assertEquals(cutoff, tree.numPages);
		
		for(int i = 0; i < cutoff; i++) {
			assertEquals(makeTag(i), tree.getPageTag(i));
		}
		
		for(int i = cutoff; i < max; i++) {
			assertEquals(nullTag, tree.getPageTag(i));
		}
	}
	
	@Test
	public void testResizeDownwardInHeightOneLevel() throws IOException {
		int max = (int) (1.5*tree.tagsPerChunk()), contracted = tree.tagsPerChunk();
		for(int i = 0; i < max; i++) {
			tree.setPageTag(i, makeTag(i));
		}
		
		tree.resize(contracted);
		assertEquals(contracted, tree.numPages);
		
		for(int i = 0; i < contracted; i++) {
			assertEquals(makeTag(i), tree.getPageTag(i));
		}
	}
	
	@Test
	public void testResizeDownwardInHeightMultipleLevels() throws IOException {
		ZKArchive smallPageArchive = master.createArchive(RevisionInfo.FIXED_SIZE, "i have small, lovable pages!");
		try(ZKFS smallPageFs = smallPageArchive.openBlank()) {
			smallPageFs.write("test", new byte[0]);
			PageTree smallPageTree = new PageTree(smallPageFs.inodeForPath("test"));
			
			int contracted = smallPageTree.tagsPerChunk();
			int max = (int) (smallPageTree.tagsPerChunk()*smallPageTree.tagsPerChunk()+1);
			
			for(int i = 0; i < max; i++) {
				smallPageTree.setPageTag(i, makeTag(i));
			}
			
			smallPageTree.resize(contracted);
			assertEquals(contracted, smallPageTree.numPages);
			
			for(int i = 0; i < contracted; i++) {
				assertEquals(makeTag(i), smallPageTree.getPageTag(i));
			}
		}
	}

	@Test
	public void testResizeUpwardInHeightOneLevel() throws IOException {
		int original = tree.tagsPerChunk();
		int max = (int) (1.5*tree.tagsPerChunk());
		StorageTag blank = archive.getBlankStorageTag();
		
		for(int i = 0; i < original; i++) {
			tree.setPageTag(i, makeTag(i));
		}
		
		tree.resize(max);
		assertEquals(max, tree.numPages);
		
		for(int i = 0; i < original; i++) {
			assertEquals(makeTag(i), tree.getPageTag(i));
		}
		
		for(int i = original; i < max; i++) {
			assertEquals(blank, tree.getPageTag(i));
		}
	}

	@Test
	public void testResizeUpwardInHeightMultipleLevels() throws IOException {
		ZKArchive smallPageArchive = master.createArchive(RevisionInfo.FIXED_SIZE, "adopt a tinypage now!");
		try(ZKFS smallPageFs = smallPageArchive.openBlank()) {
			smallPageFs.write("test", new byte[0]);
			PageTree smallPageTree = new PageTree(smallPageFs.inodeForPath("test"));
			
			int original = smallPageTree.tagsPerChunk();
			int max = (int) (smallPageTree.tagsPerChunk()*smallPageTree.tagsPerChunk()+1);
			StorageTag blank = archive.getBlankStorageTag();
			
			for(int i = 0; i < original; i++) {
				smallPageTree.setPageTag(i, makeTag(i));
			}
			
			smallPageTree.resize(max);
			assertEquals(max, smallPageTree.numPages);
			
			for(int i = 0; i < original; i++) {
				assertEquals(makeTag(i), smallPageTree.getPageTag(i));
			}
			
			for(int i = original; i < max; i++) {
				assertEquals(blank, smallPageTree.getPageTag(i));
			}
		}
	}
	
	@Test @Ignore
	/* this is insanely slow now that we have a minimum page size (and therefore large trees have huge numbers of pages).
	 * The code is retained in case PageTree changes are made and it becomes worthwhile to test large scale tree scaling.
	 * Last run 2019-03-15
	 */
	public void testLargeScaleTree() throws IOException {
		/** A chunk is not in the cache, so we look it up
		 * In the process of looking it up, we evict one of its children
		 * That eviction causes another lookup of the same chunk
		 * We continue the original lookup operation, overwriting the in-memory copy (dirtied with the changes of the child) with a blank
		 */
		
		// TODO Someday: (bug) If page size is 512, chunk cache has a capacity of 8, and the tree depth is 6, this fails.
		;
		try(
				ZKArchive smallPageArchive = master.createArchive(RevisionInfo.FIXED_SIZE, "adopt a tinypage now!");
				ZKFS smallPageFs = smallPageArchive.openBlank())
		{
			smallPageFs.write("test", new byte[0]);
			PageTree smallPageTree = new PageTree(smallPageFs.inodeForPath("test"));
			
			int max = (int) Math.pow(smallPageTree.tagsPerChunk(), 4); // exponent is tree depth
			
			for(int i = 0; i < max; i++) {
				smallPageTree.setPageTag(i, makeTag(i));
			}
			
			for(int i = 0; i < max; i++) {
				assertEquals(makeTag(i), smallPageTree.getPageTag(i));
			}
		}
	}
	
	@Test
	public void testRandomAccess() throws IOException {
		int numPages = 16384;
		Shuffler shuffler = Shuffler.fixedShuffler(numPages);
		while(shuffler.hasNext()) {
			int pageNum = shuffler.next();
			tree.setPageTag(pageNum, makeTag(pageNum));
		}
		
		for(int i = 0; i < numPages; i++) {
			assertEquals(makeTag(i), tree.getPageTag(i));
		}
	}
	
	@Test
	public void testHasTagReturnsTrueIfTagNotBlank() throws IOException {
		tree.setPageTag(1, makeTag(31337));
		assertTrue(tree.hasTag(1));
	}
	
	@Test
	public void testHasTagReturnsTrueIfTagIsBlank() throws IOException {
		tree.setPageTag(1, archive.getBlankStorageTag());
		assertFalse(tree.hasTag(1));
	}
	
	@Test
	public void testHasTagReturnsTrueIfIdIsBeyondPageCount() throws IOException {
		assertFalse(tree.hasTag(tree.numPages+1));
		assertFalse(tree.hasTag(Long.MAX_VALUE));
	}
	
	@Test
	public void testHasTagReturnsTrueIfIdIsNegative() throws IOException {
		assertFalse(tree.hasTag(-1));
		assertFalse(tree.hasTag(Long.MIN_VALUE));
	}
	
	@Test
	public void testTagForChunkReturnsTagForRequestedChunk() throws IOException {
		// check root tag
		assertEquals(tree.getRefTag().getStorageTag(), tree.tagForChunk(0));
		
		// scale up to multiple chunks and check those
		for(int i = 0; i <= 1 + tree.tagsPerChunk(); i++) {
			tree.setPageTag(i, makeTag(i));
		}
		
		assertEquals(tree.chunkAtIndex(0).getTag(0), tree.tagForChunk(1));
		assertEquals(tree.chunkAtIndex(0).getTag(1), tree.tagForChunk(2));
	}
	
	@Test
	public void testGetArchiveReturnsArchive() throws IOException {
		assertEquals(archive, tree.getArchive());
	}

	@Test @Ignore // Disabled this test since we are not currently squashing timestamps. This had high performance cost, with dubious security benefits.
	public void testPageTreeChunkFilesHaveSquashedTimestamps() throws IOException {
		assertEquals(0,
			archive.config.getCacheStorage().stat(tree.tagForChunk(0).path()).getMtime());
	}
}
