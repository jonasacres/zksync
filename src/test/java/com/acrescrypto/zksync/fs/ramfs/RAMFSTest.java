package com.acrescrypto.zksync.fs.ramfs;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.FSTestBase;

public class RAMFSTest extends FSTestBase {
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
	public void beforeEach() throws IOException {
		scratch = new RAMFS();
		prepareExamples();
	}
}
