package com.acrescrypto.zksync.fs.ramfs;

import org.junit.AfterClass;
import org.junit.Before;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.FileTestBase;

public class RAMFileTest extends FileTestBase {
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}
	
	@Before
	public void beforeEach() {
		scratch = new RAMFS();
	}
}
