package com.acrescrypto.zksync.fs.ramfs;

import org.junit.Before;

import com.acrescrypto.zksync.fs.FileTestBase;

public class RAMFileTest extends FileTestBase {
	@Before
	public void beforeEach() {
		scratch = new RAMFS();
	}
}
