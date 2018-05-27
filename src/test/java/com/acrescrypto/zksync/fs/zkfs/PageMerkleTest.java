package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.fs.File;

public class PageMerkleTest {
	ZKMaster master;
	ZKArchive archive;
	ZKFS fs;
	ZKFile file;
	PageMerkle merkle;
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").cacheOnlyArchive();
		fs = archive.openBlank();
		file = fs.open("test", File.O_RDWR|File.O_CREAT);
		merkle = file.merkle;
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Test
	public void testSerializedChunkMatchesConfigGetSerializedPageSize() throws IOException {
		file.write(new byte[2*archive.config.pageSize]);
		file.flush();
		
		String path = PageMerkle.pathForChunk(merkle.tag, 0);
		long size = archive.storage.stat(path).getSize();
		assertEquals(archive.config.getSerializedPageSize(), size);
	}
	
	@Test
	public void testExistsReturnsTrueIfRefTagIsImmediate() throws IOException {
		assertEquals(RefTag.REF_TYPE_IMMEDIATE, merkle.tag.getRefType());
		assertTrue(merkle.exists());
	}
	
	@Test
	public void testExistsReturnsTrueIfRefTagIsIndirectAndPageExists() throws IOException {
		file.write(new byte[archive.config.pageSize]);
		file.flush();
		assertEquals(RefTag.REF_TYPE_INDIRECT, merkle.tag.getRefType());
		assertTrue(merkle.exists());
	}

	@Test
	public void testExistsReturnsFalseIfRefTagIsIndirectAndPageExists() throws IOException {
		file.write(new byte[archive.config.pageSize]);
		file.flush();
		assertEquals(RefTag.REF_TYPE_INDIRECT, merkle.tag.getRefType());
		archive.storage.unlink(Page.pathForTag(merkle.getPageTag(0)));
		assertFalse(merkle.exists());
	}
	
	@Test
	public void testExistsReturnsTrueIfRefTagIsIndirectAndMerklePagesExist() throws IOException {
		file.write(new byte[10*archive.config.pageSize]);
		file.flush();
		assertEquals(RefTag.REF_TYPE_2INDIRECT, merkle.tag.getRefType());
		assertTrue(merkle.exists());
	}
	
	@Test
	public void testExistsReturnsTrueIfRefTagIsIndirectAndMerklePagesDontExist() throws IOException {
		int pagesNeeded = archive.config.pageSize/master.crypto.hashLength() + 1;
		file.write(new byte[pagesNeeded*archive.config.pageSize]);
		file.flush();
		assertEquals(RefTag.REF_TYPE_2INDIRECT, merkle.tag.getRefType());
		archive.storage.unlink(PageMerkle.pathForChunk(merkle.tag, 1));
		assertFalse(merkle.exists());
	}
}
