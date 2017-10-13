package com.acrescrypto.zksync.fs.localfs;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.io.FileUtils;

import com.acrescrypto.zksync.fs.FSTestBase;
import com.acrescrypto.zksync.fs.Stat;


public class LocalFSTest extends FSTestBase {
	
	 // TODO: need a much better solution than hard-coded paths in my home directory!
	public final static String TEST_DIR = "/home/jonas/localfstest";
	public final static String SCRATCH_DIR = "/home/jonas/localfstest/scratch";
	
	@Before
	public void beforeEach() {
		fs = new LocalFS(TEST_DIR);
		scratch = new LocalFS(SCRATCH_DIR);
	}
	
	@BeforeClass
	public static void beforeClass() {		
		java.io.File scratchDir = new java.io.File(SCRATCH_DIR);
		try {
			FileUtils.deleteDirectory(scratchDir);
		} catch (IOException e) {}
		scratchDir.mkdirs();
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
	
	@Test
	public void testMknod() {
	  // TODO: Implement... but what to do about superuser privileges?
	}

	@Test
	public void testChown() {
	  // TODO: Implement... still needs superuser though
	}

	@Test
	public void testChgrp() {
	  // TODO: Implement
	}
}
