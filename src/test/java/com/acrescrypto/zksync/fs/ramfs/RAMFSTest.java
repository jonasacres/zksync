package com.acrescrypto.zksync.fs.ramfs;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.FSTestBase;

public class RAMFSTest extends FSTestBase {
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}

	@Before
	public void beforeEach() throws IOException {
		scratch = new RAMFS();
		prepareExamples();
	}
}
