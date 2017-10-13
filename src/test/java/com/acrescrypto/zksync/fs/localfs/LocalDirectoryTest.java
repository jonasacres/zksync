package com.acrescrypto.zksync.fs.localfs;

import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.BeforeClass;

import com.acrescrypto.zksync.fs.DirectoryTestBase;

public class LocalDirectoryTest extends DirectoryTestBase {
	
	@Before
	public void beforeEach() {
		scratch = new LocalFS(LocalFSTest.SCRATCH_DIR);
	}
	
	@BeforeClass
	public static void beforeClass() {
		java.io.File scratchDir = new java.io.File(LocalFSTest.SCRATCH_DIR);
		try {
			FileUtils.deleteDirectory(scratchDir);
		} catch (IOException e) {}
		scratchDir.mkdirs();
	}	
}
