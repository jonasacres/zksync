package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
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
import com.acrescrypto.zksync.exceptions.EACCESException;

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
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		crypto = CryptoSupport.defaultCrypto();
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
		TestUtils.stopDebugMode();
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
		try(ZKFS fs = addMockData(archive)) {
			RevisionTag revTag = new RevisionTag(new RefTag(archive, crypto.rng(archive.getConfig().refTagSize())), 0, 0);
			assertFalse(archive.hasInode(revTag, fs.stat("file2").getInodeId()));
		}
	}
	
	@Test
	public void testHasInodeReturnsFalseIfInodeIsUnissued() throws IOException {
		try(ZKFS fs = addMockData(archive)) {
			assertFalse(archive.hasInode(fs.baseRevision, fs.getInodeTable().nextInodeId()));
		}
	}
	
	@Test
	public void testHasInodeReturnsFalseIfInodeIsDeleted() throws IOException {
		try(ZKFS fs = addMockData(archive)) {
			Inode inode = fs.inodeForPath("file2");
			fs.unlink("file2");
			fs.commit();
			assertFalse(archive.hasInode(fs.baseRevision, inode.stat.getInodeId()));
		}
	}
	
	@Test
	public void testHasRevisionReturnsTrueIfRevisionContentsInStorage() throws IOException {
		try(ZKFS fs = addMockData(archive)) {
			assertTrue(archive.hasRevision(fs.baseRevision));
		}
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
		try(ZKFS fs = addMockData(archive)) {
			Inode inode = fs.inodeForPath("file2");
			PageTree tree = new PageTree(inode);
			archive.storage.unlink(Page.pathForTag(tree.chunkAtIndex(0).chunkTag));
			assertFalse(archive.hasRevision(fs.baseRevision));
		}
	}
	
	@Test
	public void testHasRevisionReturnsFalseIfPagesMissingFromInodeTable() throws IOException {
		try(ZKFS fs = addMockData(archive)) {
			PageTree tree = new PageTree(fs.baseRevision.getRefTag());
			archive.storage.unlink(Page.pathForTag(tree.getPageTag(0)));
			assertFalse(archive.hasRevision(fs.baseRevision));
		}
	}
	
	@Test
	public void testHasRevisionReturnsFalseIfPageTreeChunksMissingFromInodeTable() throws IOException {
		try(ZKFS fs = addMockData(archive)) {
			for(int i = 0; i < 1024; i++) {
				fs.write(""+i, "".getBytes());
			}
			
			RevisionTag revTag = fs.commit();
			assertTrue(revTag.getRefTag().numPages > 1);
			
			PageTree tree = new PageTree(revTag.getRefTag());
			archive.storage.unlink(Page.pathForTag(tree.chunkAtIndex(0).chunkTag));
			assertFalse(archive.hasRevision(revTag));
		}
	}
	
	@Test
	public void testNondefaultArchive() throws IOException {
		Key writeKey = new Key(crypto);
		ZKArchiveConfig config = ZKArchiveConfig.create(accessor, "i'm unique and special", 4321, accessor.passphraseRoot, writeKey);
		ZKArchive archive = config.getArchive();
		
		try(ZKFS fs = archive.openBlank()) {
			String str = "contents!";
			fs.write("filename", str.getBytes());
			try(ZKFS fs2 = fs.commit().getFS()) {
				assertArrayEquals(str.getBytes(), fs2.read("filename"));
			}
		}
		
		config.close();
	}
	
	@Test
	public void testReadOnlyArchiveAllowsReads() throws IOException {
		Key writeKey = new Key(crypto);
		
		ZKArchiveConfig config = ZKArchiveConfig.create(accessor, "i'm unique and special", 4321, accessor.passphraseRoot, writeKey);
		ZKArchive archive = config.getArchive();
		RevisionTag revTag;
		
		byte[] data = "some data".getBytes();
		try(ZKFS fs = archive.openBlank()) {
			fs.write("filename", data);
			revTag = fs.commit();
		}
		
		config.close();
		ZKArchiveConfig ro = ZKArchiveConfig.openExisting(accessor, config.getArchiveId(), true, null);
		
		try(ZKFS fs = ro.getArchive().openRevision(revTag)) {
			assertArrayEquals(data, fs.read("filename"));
		}
	}
	
	@Test(expected=EACCESException.class)
	public void testReadOnlyArchiveThrowsExceptionOnWrites() throws IOException {
		Key writeKey = new Key(crypto);
		
		ZKArchiveConfig config = ZKArchiveConfig.create(accessor, "i'm unique and special", 4321, accessor.passphraseRoot, writeKey);
		ZKArchive archive = config.getArchive();
		try(ZKFS fs = archive.openBlank()) {
			byte[] data = "some data".getBytes();
			fs.write("filename", data);
			RevisionTag revTag = fs.commit();
			config.close();
			
			master.allConfigs.clear();
			master.accessors.clear();
			ZKArchiveConfig ro = ZKArchiveConfig.openExisting(accessor, config.getArchiveId(), true, null);
			archive = ro.getArchive();
			RevisionTag transplantTag = new RevisionTag(ro, revTag.serialize(), false);
			
			
			try(ZKFS fs2 = ro.getArchive().openRevision(transplantTag)) {
				fs2.write("filename", data);
				fs2.commit();
			}
		}
	}
	
	@Test(expected=EACCESException.class)
	public void testReadOnlyArchiveThrowsExceptionOnCommits() throws IOException {
		Key writeKey = new Key(crypto);
		
		ZKArchiveConfig config = ZKArchiveConfig.create(accessor, "i'm unique and special", 4321, accessor.passphraseRoot, writeKey);
		ZKArchive archive = config.getArchive();
		try(ZKFS fs = archive.openBlank()) {
			byte[] data = "some data".getBytes();
			fs.write("filename", data);
			RevisionTag revTag = fs.commit();
			config.close();
			
			master.allConfigs.clear();
			master.accessors.clear();
			ZKArchiveConfig ro = ZKArchiveConfig.openExisting(accessor, config.getArchiveId(), true, null);
			archive = ro.getArchive();
			RevisionTag transplantTag = new RevisionTag(ro, revTag.serialize(), false);
			
			ro.getArchive().openRevision(transplantTag).commitAndClose();
		}
	}

}
