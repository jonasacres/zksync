package com.acrescrypto.zksync.fs.ramfs;

import org.junit.AfterClass;
import org.junit.Before;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.DirectoryTestBase;

public class RAMDirectoryTest extends DirectoryTestBase {
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}

	@Before
	public void beforeEach() {
		scratch = new RAMFS();
	}
}
