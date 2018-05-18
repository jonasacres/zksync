package com.acrescrypto.zksync.fs.ramfs;

import java.io.IOException;

import org.junit.Before;

import com.acrescrypto.zksync.fs.FSTestBase;

public class RAMFSTest extends FSTestBase {
	@Before
	public void beforeEach() throws IOException {
		scratch = new RAMFS();
		prepareExamples();
	}
}
