package com.acrescrypto.zksync.fs.localfs;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.*;

import org.apache.commons.io.FileUtils;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.FSTestBase;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.utility.Util;


public class LocalFSTest extends FSTestBase {

	// TODO Release: (test) this is going to break on Windows
	public final static String SCRATCH_DIR = "/tmp/zksync-test/localfs";

	@Before
	public void beforeEach() throws IOException {
		deleteFiles();
		(new java.io.File(SCRATCH_DIR)).mkdirs();
		scratch = new LocalFS(SCRATCH_DIR);
		prepareExamples();
	}

	@AfterClass
	public static void afterClass() {
		deleteFiles();
		TestUtils.assertTidy();
	}

	protected static void deleteFiles() {
		java.io.File scratchDir = new java.io.File(SCRATCH_DIR);
		try {
			FileUtils.deleteDirectory(scratchDir);
		} catch (IOException e) {}
	}

	@Test
	public void testStatIdentifiesDevices() throws IOException {
		if(Util.isWindows()) return;
		LocalFS root = new LocalFS("/");
		Stat devNull = root.stat("/dev/null");
		assertTrue(devNull.isCharacterDevice());
		if(Util.isLinux()) {
			assertEquals(devNull.getDevMajor(), 1);
			assertEquals(devNull.getDevMinor(), 3);
		} else if(Util.isOSX()) {
			assertEquals(devNull.getDevMajor(), 3);
			assertEquals(devNull.getDevMinor(), 2);
		}
	}
	
	@Test @Override
	public void testStatIdentifiesFifos() throws IOException {
		if(Util.isWindows()) return;
		super.testStatIdentifiesFifos();
	}

	@Test @Ignore @Override
	public void testMknodCharacterDevice() throws IOException {
		// TODO PrivilegedOperation: (test) mknod chardev. needs superuser privileges
	}

	@Test @Ignore @Override
	public void testMknodBlockDevice() throws IOException {
		// TODO PrivilegedOperation: (test) mknod blockdev. needs superuser privileges
	}

	@Test @Ignore @Override
	public void testChown() {
		// TODO PrivilegedOperation: (test) chown. needs superuser privileges
	}

	@Test @Ignore @Override
	public void testChgrp() {
		// TODO PrivilegedOperation: (test) chgrp. needs superuser privileges
	}
}
