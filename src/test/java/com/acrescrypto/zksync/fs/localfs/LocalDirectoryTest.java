package com.acrescrypto.zksync.fs.localfs;

import static org.junit.Assume.assumeFalse;

import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.DirectoryTestBase;
import com.acrescrypto.zksync.utility.Util;

public class LocalDirectoryTest extends DirectoryTestBase {
	
	@Before
	public void beforeEach() throws IOException {
		scratch = new LocalFS(LocalFSTest.SCRATCH_DIR);
		scratch.purge();
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
	
	@Test @Override
	public void testWalkFollowsSymlinksByDefault() throws IOException {
		assumeFalse("Symlinks not tested on Windows", Util.isWindows());
		super.testWalkFollowsSymlinksByDefault();
	}
	
	@Test @Override
	public void testWalkDoesNotFollowSymlinksWhenDontFollowSymlinksSpecified() throws IOException {
		assumeFalse("Symlinks not tested on Windows", Util.isWindows());
		super.testWalkDoesNotFollowSymlinksWhenDontFollowSymlinksSpecified();
	}
	
	@Test @Override
	public void testWalkSetsBrokenSymlinkFlagWhenAppropriate() throws IOException {
		assumeFalse("Symlinks not tested on Windows", Util.isWindows());
		super.testWalkSetsBrokenSymlinkFlagWhenAppropriate();
	}
}
