package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.File;

public class PageTest {
	ZKMaster master;
	ZKArchive archive;
	ZKFS fs;
	ZKFile file;
	Page page;
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		fs = archive.openBlank();
		file = fs.open("test", File.O_WRONLY|File.O_CREAT);
		file.write(new byte[2*archive.config.pageSize]);
		file.flush();
		page = new Page(file, 0);
	}
	
	@After
	public void afterEach() throws IOException {
		file.close();
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
	public void testSerializationLength() throws IOException {
		page.flush();
		byte[] tag = file.getPageTag(0);
		assertEquals(archive.config.getSerializedPageSize(), archive.storage.stat(Page.pathForTag(tag)).getSize());
	}
}
