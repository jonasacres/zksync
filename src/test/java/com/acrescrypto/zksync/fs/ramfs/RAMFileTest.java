package com.acrescrypto.zksync.fs.ramfs;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.FileTestBase;

public class RAMFileTest extends FileTestBase {
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Before
	public void beforeEach() {
		scratch = new RAMFS();
	}
}
