package com.acrescrypto.zksync.fs.localfs;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.*;

import org.apache.commons.io.FileUtils;

import com.acrescrypto.zksync.fs.FSTestBase;
import com.acrescrypto.zksync.fs.Stat;


public class LocalFSTest extends FSTestBase {
	
	 // TODO: this is going to break on Windows
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
	}
	
	protected static void deleteFiles() {
		java.io.File scratchDir = new java.io.File(SCRATCH_DIR);
		try {
			FileUtils.deleteDirectory(scratchDir);
		} catch (IOException e) {}
	}
	
	@Test
	public void testStatIdentifiesDevices() throws IOException {
		// TODO: skip on windows
		LocalFS root = new LocalFS("/");
		Stat devNull = root.stat("/dev/null");
		assertTrue(devNull.isCharacterDevice());
		// TODO: OS X is 3,2 instead of 1,3 as on Linux
		assertEquals(devNull.getDevMajor(), 1);
		assertEquals(devNull.getDevMinor(), 3);
	}
	
	@Test @Ignore @Override
	public void testMknodCharacterDevice() throws IOException {
	  // TODO: Implement... but what to do about superuser privileges?
	}

	@Test @Ignore @Override
	public void testMknodBlockDevice() throws IOException {
	  // TODO: Implement... but what to do about superuser privileges?
	}

	@Test @Ignore @Override
	public void testChown() {
	  // TODO: Implement... still needs superuser though
	}

	@Test @Ignore @Override
	public void testChgrp() {
	  // TODO: Implement, needs super
	}
}
