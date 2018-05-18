package com.acrescrypto.zksync.fs.ramfs;

import org.junit.Before;

import com.acrescrypto.zksync.fs.DirectoryTestBase;

public class RAMDirectoryTest extends DirectoryTestBase {
	@Before
	public void beforeEach() {
		scratch = new RAMFS();
	}
}
