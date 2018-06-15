package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.DirectoryTestBase;

public class ZKDirectoryTest extends DirectoryTestBase {
	ZKFS zkscratch;
	ZKMaster master;
	
	@Before
	public void beforeEach() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		master = ZKMaster.openBlankTestVolume();
		scratch = zkscratch = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").openBlank();
	}
	
	@After
	public void afterEach() throws IOException {
		ZKFSTest.restoreArgon2Costs();
		master.close();
		zkscratch.close();
		zkscratch.archive.close();
	}

	@BeforeClass
	public static void beforeClass() {
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}
	
	// TODO: test that invalid directories cause an appropriate exception
}
