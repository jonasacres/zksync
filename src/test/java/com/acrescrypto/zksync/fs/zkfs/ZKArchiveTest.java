package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;

public class ZKArchiveTest {
	CryptoSupport crypto;
	ZKMaster master;
	ArchiveAccessor accessor;
	ZKArchiveConfig config;
	ZKArchive archive;
	ZKFS fs;
	
	ZKFS addMockData(ZKArchive archive) throws IOException {
		ZKFS fs = archive.openBlank();
		fs.write("file0", crypto.rng(crypto.hashLength()-1));
		fs.write("file1", crypto.rng(archive.getConfig().getPageSize()));
		fs.write("file2", crypto.rng(2*archive.getConfig().getPageSize()));
		fs.commit();
		
		return fs;
	}
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		crypto = new CryptoSupport();
		master = ZKMaster.openBlankTestVolume();
		accessor = master.makeAccessorForRoot(new Key(crypto), false);
		config = ZKArchiveConfig.create(accessor, "", ZKArchive.DEFAULT_PAGE_SIZE);
		archive = config.archive;
	}
	
	@After
	public void afterEach() {
		archive.close();
		master.close();
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testHasPageTagReturnsTrueIfPageTagInStorage() throws IOException {
		ZKFS fs = addMockData(archive);
		Inode inode = fs.inodeForPath("file1");
		PageTree tree = new PageTree(inode);
		
		assertTrue(archive.hasPageTag(tree.getPageTag(0)));
		fs.close();
	}
	
	@Test
	public void testHasPageTagReturnsTrueIfPageTagNotInStorage() throws IOException {
		assertFalse(archive.hasPageTag(crypto.rng(crypto.hashLength())));
	}
	
	@Test
	public void testHasInodeReturnsTrueIfAllPagesOfRefTagInStorage() throws IOException {
		for(int i = 0; i <= 2; i++) {
			ZKFS fs = addMockData(archive);
			Inode inode = fs.inodeForPath("file"+i);
			assertTrue(archive.hasInode(fs.baseRevision, inode.stat.getInodeId()));
			fs.close();
		}
	}
	
	@Test
	public void testHasInodeReturnsFalseIfAnyPagesMissingFromStorage() throws IOException {
		ZKFS fs = addMockData(archive);
		for(int i = 1; i <= 2; i++) {	
			Inode inode = fs.inodeForPath("file"+i);
			PageTree tree = new PageTree(inode);
			archive.storage.unlink(Page.pathForTag(tree.getPageTag(0)));
			assertFalse(archive.hasInode(fs.baseRevision, inode.stat.getInodeId()));
		}
		fs.close();
	}
	
	@Test
	public void testHasInodeReturnsFalseIfPageTreeChunkMissingFromStorage() throws IOException {
		ZKFS fs = addMockData(archive);
		Inode inode = fs.inodeForPath("file2");
		PageTree tree = new PageTree(inode);
		
		archive.storage.unlink(Page.pathForTag(tree.chunkAtIndex(0).chunkTag));
		assertFalse(archive.hasInode(fs.baseRevision, inode.getStat().getInodeId()));
		fs.close();
	}
	
	@Test
	public void testHasInodeReturnsFalseIfRevTagIsNonsense() throws IOException {
		ZKFS fs = addMockData(archive);
		RefTag revTag = new RefTag(archive, crypto.rng(archive.getConfig().refTagSize()));
		assertFalse(archive.hasInode(revTag, fs.stat("file2").getInodeId()));
	}
	
	@Test
	public void testHasInodeReturnsFalseIfInodeIsUnissued() throws IOException {
		ZKFS fs = addMockData(archive);
		assertFalse(archive.hasInode(fs.baseRevision, fs.getInodeTable().nextInodeId));
	}
	
	@Test
	public void testHasInodeReturnsFalseIfInodeIsDeleted() throws IOException {
		ZKFS fs = addMockData(archive);
		Inode inode = fs.inodeForPath("file2");
		fs.unlink("file2");
		fs.commit();
		assertFalse(archive.hasInode(fs.baseRevision, inode.stat.getInodeId()));
	}
	
	@Test
	public void testHasRevisionReturnsTrueIfRevisionContentsInStorage() throws IOException {
		ZKFS fs = addMockData(archive);
		assertTrue(archive.hasRevision(fs.baseRevision));
		fs.close();
	}
	
	@Test
	public void testHasRevisionReturnsFalseIfMissingAnyPages() throws IOException {
		for(int i = 1; i <= 2; i++) {
			ZKFS fs = addMockData(archive);
			Inode inode = fs.inodeForPath("file"+i);
			PageTree tree = new PageTree(inode);
			archive.storage.unlink(Page.pathForTag(tree.getPageTag(0)));
			assertFalse(archive.hasRevision(fs.baseRevision));
			fs.close();
		}
	}
	
	@Test
	public void testHasRevisionReturnsFalseIfMissingAnyFilePageTreeChunks() throws IOException {
		ZKFS fs = addMockData(archive);
		Inode inode = fs.inodeForPath("file2");
		PageTree tree = new PageTree(inode);
		archive.storage.unlink(Page.pathForTag(tree.chunkAtIndex(0).chunkTag));
		assertFalse(archive.hasRevision(fs.baseRevision));
		fs.close();
	}
	
	@Test
	public void testHasRevisionReturnsFalseIfPagesMissingFromInodeTable() throws IOException {
		ZKFS fs = addMockData(archive);
		PageTree tree = new PageTree(fs.baseRevision);
		archive.storage.unlink(Page.pathForTag(tree.getPageTag(0)));
		assertFalse(archive.hasRevision(fs.baseRevision));
		fs.close();
	}
	
	@Test
	public void testHasRevisionReturnsFalseIfPageTreeChunksMissingFromInodeTable() throws IOException {
		ZKFS fs = addMockData(archive);
		for(int i = 0; i < 1024; i++) fs.write(""+i, "".getBytes());
		RefTag revTag = fs.commit();
		assertTrue(revTag.numPages > 1);
		PageTree tree = new PageTree(revTag);
		archive.storage.unlink(Page.pathForTag(tree.chunkAtIndex(0).chunkTag));
		assertFalse(archive.hasRevision(revTag));
		fs.close();
	}
}
