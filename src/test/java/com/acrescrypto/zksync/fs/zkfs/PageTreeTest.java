package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.utility.Shuffler;
import com.acrescrypto.zksync.utility.Util;

public class PageTreeTest {
	CryptoSupport crypto;
	ZKMaster master;
	ZKArchive archive;
	ZKFS fs;
	PageTree tree;
	RefTag immediateTag, indirectTag, doubleIndirectTag, revTag;
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		crypto = new CryptoSupport();
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
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testConstructFromRevTagSetsFields() throws IOException {
		PageTree fromRevTag = new PageTree(revTag);
		
		assertEquals(archive, fromRevTag.archive);
		assertEquals(revTag, fromRevTag.refTag);
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
		assertEquals(inode.stat.getInodeId(), fromInode.inodeId);
		assertEquals(inode.identity, fromInode.inodeIdentity);
		
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
		archive.storage.unlink(Page.pathForTag(indirectTag.getHash()));
		assertFalse(new PageTree(indirectTag).exists());
	}
	
	@Test
	public void testExistsReturnsTrueIfRefTagIs2IndirectAndChunksAndPagesExist() throws IOException {
		assertTrue(tree.exists());
	}
	
	@Test
	public void testExistsReturnsFalseIfRefTagIs2IndirectAndChunksExistButPageMissing() throws IOException {
		archive.storage.unlink(Page.pathForTag(tree.getPageTag(1)));
		assertFalse(tree.exists());
	}
	
	@Test
	public void testExistsReturnsFalseIfRefTagIs2IndirectAndsPagesExistButChunkMissing() throws IOException {
		archive.storage.unlink(Page.pathForTag(tree.chunkAtIndex(0).chunkTag));
		assertFalse(tree.exists());
	}
	
	@Test(expected=ENOENTException.class)
	public void testAssertExistsThrowsExceptionIfExistsReturnsFalse() throws IOException {
		archive.storage.unlink(Page.pathForTag(tree.chunkAtIndex(0).chunkTag));
		tree.assertExists();
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
		byte[] randomTag = crypto.hash(Util.serializeInt(1234));
		tree.setPageTag(1, randomTag);
		assertArrayEquals(randomTag, tree.getPageTag(1));
	}
	
	@Test
	public void testSetPageTagReplacesBlankPageTags() throws IOException {
		byte[] randomTag = crypto.hash(Util.serializeInt(1234));
		tree.setPageTag(tree.numPages, randomTag);
		assertArrayEquals(randomTag, tree.getPageTag(tree.numPages-1));
	}
	
	@Test
	public void testSetPageTagAutomaticallyResizesTreeIfNeeded() throws IOException {
		assertEquals(1, tree.numChunks());
		tree.setPageTag(tree.tagsPerChunk(), crypto.hash(Util.serializeInt(1)));
		assertEquals(3, tree.numChunks());
		assertEquals(tree.tagsPerChunk()*tree.tagsPerChunk(), tree.maxNumPages);
	}

	@Test
	public void testGetPageTagReturnsTag() throws IOException {
		byte[] tag = crypto.rng(crypto.hashLength());
		tree.setPageTag((int) (tree.tagsPerChunk()*0.5), tag);
		assertEquals(tag, tree.getPageTag((int) (tree.tagsPerChunk()*0.5)));
	}
	
	@Test
	public void testGetPageTagReturnsBlankTagIfTagHasNotBeenSet() throws IOException {
		byte[] nullTag = new byte[crypto.hashLength()];
		assertArrayEquals(nullTag, tree.getPageTag(2));
	}
	
	@Test
	public void testGetNumChunksReturnsNumberOfChunks() throws IOException {
		assertEquals(tree.numChunks, tree.numChunks());
		tree.setPageTag(tree.tagsPerChunk(), new byte[crypto.hashLength()]);
		assertEquals(tree.numChunks, tree.numChunks());
	}
	
	@Test
	public void testCommitWritesDirtyChunksIfDoublyIndirect() throws IOException {
		ArrayList<byte[]> oldTags = new ArrayList<>();
		
		for(int pageNum = 0; pageNum < tree.numPages(); pageNum++) {
			oldTags.add(tree.getPageTag(pageNum));
			assertTrue(archive.config.getCacheStorage().exists(Page.pathForTag(tree.getPageTag(pageNum))));
		}
		
		ZKFile file = fs.open("2indirect", File.O_WRONLY);
		file.seek(archive.config.pageSize, File.SEEK_SET);
		file.write("bleep".getBytes());
		file.close();
		tree = new PageTree(file.inode);
		
		for(int pageNum = 0; pageNum < tree.numPages(); pageNum++) {
			if(pageNum == 1) {
				assertFalse(Arrays.equals(oldTags.get(pageNum), tree.getPageTag(pageNum)));
			} else {
				assertTrue(Arrays.equals(oldTags.get(pageNum), tree.getPageTag(pageNum)));
			}
			
			oldTags.add(tree.getPageTag(pageNum));
			assertTrue(archive.config.getCacheStorage().exists(Page.pathForTag(oldTags.get(pageNum))));
			assertTrue(archive.config.getCacheStorage().exists(Page.pathForTag(tree.getPageTag(pageNum))));
		}
	}
	
	@Test
	public void testCommitDoesNotWriteChunksIfIndirect() throws IOException {
		PageTree indirectTree = new PageTree(fs.inodeForPath("indirect"));
		byte[] oldTag = indirectTree.getPageTag(0);
		fs.write("indirect", crypto.rng(archive.config.pageSize));
		
		indirectTree = new PageTree(fs.inodeForPath("indirect"));
		assertArrayEquals(indirectTree.getRefTag().getHash(), indirectTree.getPageTag(0));
		assertFalse(Arrays.equals(oldTag, indirectTree.getPageTag(0)));
		assertTrue(archive.config.getCacheStorage().exists(Page.pathForTag(indirectTag.getHash())));
	}
	
	@Test
	public void testCommitDoesNotWriteChunksIfImmediate() throws IOException {
		PageTree immediateTree = new PageTree(fs.inodeForPath("immediate"));
		assertEquals(immediateTag, immediateTree.refTag);
		assertArrayEquals(immediateTag.getLiteral(), immediateTree.getPageTag(0));
		assertFalse(archive.config.getCacheStorage().exists(Page.pathForTag(immediateTag.getHash())));
	}
	
	@Test
	public void testChunkTagsPassedToArchive() throws IOException {
		for(int i = 0; i < tree.numChunks; i++) {
			boolean found = false;
			
			for(byte[] passed : archive.allPageTags()) {
				if(Arrays.equals(passed, tree.tagForChunk(i))) {
					found = true;
					break;
				}
			}
			
			assertTrue(found);
		}	
	}
	
	@Test
	public void testPageTagsPassedToArchive() throws IOException {
		for(int i = 0; i < tree.numChunks; i++) {
			boolean found = false;
			
			for(byte[] passed : archive.allPageTags()) {
				if(Arrays.equals(passed, tree.getPageTag(i))) {
					found = true;
					break;
				}
			}
			
			assertTrue(found);
		}	
	}
	
	@Test
	public void testImmediatesNotPassedToArchive() throws IOException {
		for(byte[] passed : archive.allPageTags()) {
			assertFalse(Arrays.equals(passed, immediateTag.getHash()));
			assertFalse(Arrays.equals(passed, immediateTag.getLiteral()));
		}
	}
	
	@Test
	public void testResizeDownwardToSameTreeHeight() throws IOException {
		int max = (int) (1.5*tree.tagsPerChunk());
		int cutoff = tree.tagsPerChunk() + 1;
		byte[] nullTag = new byte[crypto.hashLength()];
		
		for(int i = 0; i < max; i++) {
			tree.setPageTag(i, archive.crypto.hash(Util.serializeInt(i)));
		}
		
		tree.resize(cutoff);
		assertEquals(max, tree.numPages);
		
		for(int i = 0; i < cutoff; i++) {
			assertArrayEquals(archive.crypto.hash(Util.serializeInt(i)), tree.getPageTag(i));
		}
		
		for(int i = cutoff; i < max; i++) {
			assertArrayEquals(nullTag, tree.getPageTag(i));
		}
	}
	
	@Test
	public void testResizeUpwardToSameTreeHeight() throws IOException {
		int max = (int) (1.5*tree.tagsPerChunk());
		int cutoff = tree.tagsPerChunk() + 1;
		byte[] nullTag = new byte[crypto.hashLength()];
		
		for(int i = 0; i < cutoff; i++) {
			tree.setPageTag(i, archive.crypto.hash(Util.serializeInt(i)));
		}
		
		tree.resize(max);
		assertEquals(cutoff, tree.numPages);
		
		for(int i = 0; i < cutoff; i++) {
			assertArrayEquals(archive.crypto.hash(Util.serializeInt(i)), tree.getPageTag(i));
		}
		
		for(int i = cutoff; i < max; i++) {
			assertArrayEquals(nullTag, tree.getPageTag(i));
		}
	}
	
	@Test
	public void testResizeDownwardInHeightOneLevel() throws IOException {
		int max = (int) (1.5*tree.tagsPerChunk()), contracted = tree.tagsPerChunk();
		for(int i = 0; i < max; i++) {
			tree.setPageTag(i, archive.crypto.hash(Util.serializeInt(i)));
		}
		
		tree.resize(contracted);
		assertEquals(contracted, tree.numPages);
		
		for(int i = 0; i < contracted; i++) {
			assertArrayEquals(archive.crypto.hash(Util.serializeInt(i)), tree.getPageTag(i));
		}
	}
	
	@Test
	public void testResizeDownwardInHeightMultipleLevels() throws IOException {
		ZKArchive smallPageArchive = master.createArchive(1024, "i have small, lovable pages!");
		ZKFS smallPageFs = smallPageArchive.openBlank();
		smallPageFs.write("test", new byte[0]);
		PageTree smallPageTree = new PageTree(smallPageFs.inodeForPath("test"));
		
		int contracted = smallPageTree.tagsPerChunk();
		int max = (int) (smallPageTree.tagsPerChunk()*smallPageTree.tagsPerChunk()+1);
		
		for(int i = 0; i < max; i++) {
			smallPageTree.setPageTag(i, archive.crypto.hash(Util.serializeInt(i)));
		}
		
		smallPageTree.resize(contracted);
		assertEquals(contracted, smallPageTree.numPages);
		
		for(int i = 0; i < contracted; i++) {
			assertArrayEquals(archive.crypto.hash(Util.serializeInt(i)), smallPageTree.getPageTag(i));
		}
	}

	@Test
	public void testResizeUpwardInHeightOneLevel() throws IOException {
		int original = tree.tagsPerChunk();
		int max = (int) (1.5*tree.tagsPerChunk());
		byte[] blank = new byte[crypto.hashLength()];
		
		for(int i = 0; i < original; i++) {
			tree.setPageTag(i, archive.crypto.hash(Util.serializeInt(i)));
		}
		
		tree.resize(max);
		assertEquals(max, tree.numPages);
		
		for(int i = 0; i < original; i++) {
			assertArrayEquals(archive.crypto.hash(Util.serializeInt(i)), tree.getPageTag(i));
		}
		
		for(int i = original; i < max; i++) {
			assertArrayEquals(blank, tree.getPageTag(i));
		}
	}

	@Test
	public void testResizeUpwardInHeightMultipleLevels() throws IOException {
		ZKArchive smallPageArchive = master.createArchive(1024, "adopt a tinypage now!");
		ZKFS smallPageFs = smallPageArchive.openBlank();
		smallPageFs.write("test", new byte[0]);
		PageTree smallPageTree = new PageTree(smallPageFs.inodeForPath("test"));
		
		int original = smallPageTree.tagsPerChunk();
		int max = (int) (smallPageTree.tagsPerChunk()*smallPageTree.tagsPerChunk()+1);
		byte[] blank = new byte[crypto.hashLength()];
		
		for(int i = 0; i < original; i++) {
			smallPageTree.setPageTag(i, archive.crypto.hash(Util.serializeInt(i)));
		}
		
		smallPageTree.resize(max);
		assertEquals(max, smallPageTree.numPages);
		
		for(int i = 0; i < original; i++) {
			assertArrayEquals(archive.crypto.hash(Util.serializeInt(i)), smallPageTree.getPageTag(i));
		}
		
		for(int i = original; i < max; i++) {
			assertArrayEquals(blank, tree.getPageTag(i));
		}
	}
	
	@Test
	public void testLargeScaleTree() throws IOException {
		/** A chunk is not in the cache, so we look it up
		 * In the process of looking it up, we evict one of its children
		 * That eviction causes another lookup of the same chunk
		 * We continue the original lookup operation, overwriting the in-memory copy (dirtied with the changes of the child) with a blank
		 */
		
		// TODO DHT: (review) If page size is 512, chunk cache has a capacity of 8, and the tree depth is 6, this fails.
		ZKArchive smallPageArchive = master.createArchive(512, "adopt a tinypage now!");
		ZKFS smallPageFs = smallPageArchive.openBlank();
		smallPageFs.write("test", new byte[0]);
		PageTree smallPageTree = new PageTree(smallPageFs.inodeForPath("test"));
		
		int max = (int) Math.pow(smallPageTree.tagsPerChunk(), 4);
		
		for(int i = 0; i < max; i++) {
			smallPageTree.setPageTag(i, archive.crypto.hash(Util.serializeInt(i)));
		}
		
		for(int i = 0; i < max; i++) {
			assertArrayEquals(archive.crypto.hash(Util.serializeInt(i)), smallPageTree.getPageTag(i));
		}
	}
	
	@Test
	public void testRandomAccess() throws IOException {
		int numPages = 16384;
		Shuffler shuffler = Shuffler.fixedShuffler(numPages);
		while(shuffler.hasNext()) {
			int pageNum = shuffler.next();
			tree.setPageTag(pageNum, archive.crypto.hash(Util.serializeInt(pageNum)));
		}
		
		for(int i = 0; i < numPages; i++) {
			assertArrayEquals(archive.crypto.hash(Util.serializeInt(i)), tree.getPageTag(i));
		}
	}
	
	@Test
	public void testHasTagReturnsTrueIfTagNotBlank() throws IOException {
		assertTrue(tree.hasTag(0));
	}
	
	@Test
	public void testHasTagReturnsTrueIfTagIsBlank() throws IOException {
		tree.setPageTag(0, new byte[crypto.hashLength()]);
		assertFalse(tree.hasTag(0));
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
		assertArrayEquals(tree.getRefTag().getHash(), tree.tagForChunk(0));
		
		// scale up to multiple chunks and check those
		for(int i = 0; i <= 1 + tree.tagsPerChunk(); i++) {
			tree.setPageTag(i, crypto.hash(Util.serializeInt(i)));
		}
		
		assertArrayEquals(tree.chunkAtIndex(0).getTag(0), tree.tagForChunk(1));
		assertArrayEquals(tree.chunkAtIndex(0).getTag(1), tree.tagForChunk(2));
	}
	
	@Test
	public void testGetArchiveReturnsArchive() throws IOException {
		assertEquals(archive, tree.getArchive());
	}

	@Test
	public void testPageTreeChunkFilesHaveSquashedTimestamps() throws IOException {
		assertEquals(0, archive.config.getCacheStorage().stat(Page.pathForTag(tree.tagForChunk(0))).getMtime());
	}
}
