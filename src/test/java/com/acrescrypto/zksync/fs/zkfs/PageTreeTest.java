package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;

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
		assertEquals(1, fromRevTag.maxNumPages);
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
	// exists returns true if reftag is 2indirect and all chunks and pages present
	// exists returns false if reftag is 2indirect and all chunks exists but not all pages present
	// exists returns false if reftag is 2indirect and not all chunks present
	
	// assertExists throws exception if exists returns false
	// assertExists does now throw exception if exists returns true
	
	// getRefTag returns ref tag

	// setPageTag replaces existing page tags
	// setPageTag replaces blank page tags
	// setPageTag automatically resizes tree if needed
	// setPageTag calls archive.addPageTag
	
	// getPageTag returns tag
	// getPageTag returns blank tag if tag has not been set
	
	// numChunks returns numChunks
	
	// commit writes dirty chunks if numChunks greater than 1
	// commit does not write chunks if numChunks not greater than 1
	// chunk tags are passed to archive.addPageTag
	
	// resize downward to same tree height clears old tags and updates numPages
	// resize upward to same tree height updates numPages
	
	// resize downward in tree height by one level
	// resize downward in tree height by multiple levels
	
	// resize upward in tree height by one level
	// resize upward in tree height by multiple levels
	
	// hasTag returns true if tag not blank
	// hasTag returns false if tag blank
	// hasTag returns false if requested page beyond size
	// hasTag returns false if requested page is negative
	
	// tagForChunk returns tag for requested chunk
	
	// getArchive returns archive
}
