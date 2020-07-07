package com.acrescrypto.zksync.fs.localfs;

import static org.junit.Assume.assumeTrue;

import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.FileTestBase;
import com.acrescrypto.zksync.utility.Util;

public class LocalFileTest extends FileTestBase {

	@Before
	public void beforeEach() {
		scratch = new LocalFS(LocalFSTest.SCRATCH_DIR);
	}

	@BeforeClass
	public static void beforeClass() {
		TestUtils.startDebugMode();
		java.io.File scratchDir = new java.io.File(LocalFSTest.SCRATCH_DIR);
		try {
			FileUtils.deleteDirectory(scratchDir);
		} catch (IOException e) {}
		scratchDir.mkdirs();
	}
	
	@AfterClass
	public static void afterAll() {
		LocalFSTest.deleteFiles();
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Override
	public void testOpenFollowsSymlinks() throws IOException {
		assumeTrue(!Util.isWindows());
		super.testOpenFollowsSymlinks();
	}

	@Override
	public void testONOFOLLOWDoesNotFollowSymlinks() throws IOException {
		assumeTrue(!Util.isWindows());
		super.testONOFOLLOWDoesNotFollowSymlinks();
	}
}
