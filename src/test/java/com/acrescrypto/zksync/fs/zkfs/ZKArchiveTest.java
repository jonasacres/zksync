package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
		config = new ZKArchiveConfig(accessor, "", ZKArchive.DEFAULT_PAGE_SIZE);
		archive = config.archive;
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Test
	public void testHasPageTagReturnsTrueIfPageTagInStorage() throws IOException {
		ZKFS fs = addMockData(archive);
		Inode inode = fs.inodeForPath("file1");
		PageMerkle merkle = new PageMerkle(inode.refTag);
		
		assertTrue(archive.hasPageTag(merkle.getPageTag(0)));
	}
	
	@Test
	public void testHasPageTagReturnsTrueIfPageTagNotInStorage() throws IOException {
		assertFalse(archive.hasPageTag(crypto.rng(crypto.hashLength())));
	}
	
	@Test
	public void testHasRefTagReturnsTrueIfAllPagesOfRefTagInStorage() throws IOException {
		for(int i = 0; i <= 2; i++) {
			ZKFS fs = addMockData(archive);
			Inode inode = fs.inodeForPath("file"+i);
			assertTrue(archive.hasRefTag(inode.refTag));
		}
	}
	
	@Test
	public void testHasRefTagReturnsFalseIfAnyPagesMissingFromStorage() throws IOException {
		ZKFS fs = addMockData(archive);
		for(int i = 1; i <= 2; i++) {	
			Inode inode = fs.inodeForPath("file"+i);
			PageMerkle merkle = new PageMerkle(inode.refTag);
			archive.storage.unlink(Page.pathForTag(merkle.getPageTag(0)));
			assertFalse(archive.hasRefTag(inode.refTag));
		}
	}
	
	@Test
	public void testHasRefTagReturnsFalseIfMerkleChunkMissingFromStorage() throws IOException {
		ZKFS fs = addMockData(archive);
		Inode inode = fs.inodeForPath("file2");
		archive.storage.unlink(Page.pathForTag(PageMerkle.tagForChunk(inode.refTag, 0)));
		assertFalse(archive.hasRefTag(inode.refTag));
	}
	
	@Test
	public void testHasRefTagReturnsFalseIfRefTagIsNonsense() throws IOException {
		RefTag refTag;
		do {
			refTag = new RefTag(archive, crypto.rng(archive.getConfig().refTagSize()));
		} while(refTag.getRefType() == RefTag.REF_TYPE_IMMEDIATE);
		assertFalse(archive.hasRefTag(refTag));
	}
	
	@Test
	public void testHasRevisionReturnsTrueIfRevisionContentsInStorage() throws IOException {
		ZKFS fs = addMockData(archive);
		assertTrue(archive.hasRevision(fs.baseRevision));
	}
	
	@Test
	public void testHasRevisionReturnsFalseIfMissingAnyPages() throws IOException {
		for(int i = 1; i <= 2; i++) {
			ZKFS fs = addMockData(archive);
			Inode inode = fs.inodeForPath("file"+i);
			PageMerkle merkle = new PageMerkle(inode.refTag);
			archive.storage.unlink(Page.pathForTag(merkle.getPageTag(0)));
			assertFalse(archive.hasRevision(fs.baseRevision));
		}
	}
	
	@Test
	public void testHasRevisionReturnsFalseIfMissingAnyFileMerkleChunks() throws IOException {
		ZKFS fs = addMockData(archive);
		Inode inode = fs.inodeForPath("file2");
		archive.storage.unlink(Page.pathForTag(PageMerkle.tagForChunk(inode.refTag, 0)));
		assertFalse(archive.hasRevision(fs.baseRevision));
	}
	
	@Test
	public void testHasRevisionReturnsFalseIfPagesMissingFromInodeTable() throws IOException {
		ZKFS fs = addMockData(archive);
		PageMerkle merkle = new PageMerkle(fs.baseRevision);
		archive.storage.unlink(Page.pathForTag(merkle.getPageTag(0)));
		assertFalse(archive.hasRevision(fs.baseRevision));
	}
	
	@Test
	public void testHasRevisionReturnsFalseIfMerkleChunksMissingFromInodeTable() throws IOException {
		ZKFS fs = addMockData(archive);
		for(int i = 0; i < 1024; i++) fs.write(""+i, "".getBytes());
		RefTag revTag = fs.commit();
		assertTrue(revTag.numPages > 1);
		archive.storage.unlink(Page.pathForTag(PageMerkle.tagForChunk(revTag, 0)));
		assertFalse(archive.hasRevision(revTag));
	}
}
