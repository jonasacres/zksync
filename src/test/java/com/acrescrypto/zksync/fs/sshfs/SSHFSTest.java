package com.acrescrypto.zksync.fs.sshfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.acrescrypto.zksync.fs.FSTestBase;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.localfs.LocalFS;

public class SSHFSTest extends FSTestBase {
	// TODO: this is going to break on Windows
	public final static String SCRATCH_DIR = "/tmp/zksync-test/sshfs";
	public final static String URL = "jonas@localhost:" + SCRATCH_DIR;
	public SSHFS sshscratch;
	
	public static SSHFS openFs() throws IOException { 
		return SSHFS.withPassphrase("zksync-test@localhost:", "voice tattoo behind school".toCharArray());
	}

	@Before
	public void beforeEach() throws IOException {
		deleteFiles();
		scratch = sshscratch = openFs();
		prepareExamples();
	}

	@AfterClass
	public static void afterClass() {
		deleteFiles();
	}

	protected static void deleteFiles() {
		try {
			openFs().rmrf("/");
		} catch(IOException exc) {}
	}

	@Test
	public void testStatIdentifiesDevices() throws IOException {
		if(sshscratch.hostType == SSHFS.HOST_TYPE_UNKNOWN) return;
		LocalFS root = new LocalFS("/");
		Stat devNull = root.stat("/dev/null");
		assertTrue(devNull.isCharacterDevice());
		if(sshscratch.getHostType() == SSHFS.HOST_TYPE_LINUX) {
			assertEquals(devNull.getDevMajor(), 1);
			assertEquals(devNull.getDevMinor(), 3);
		} else if(sshscratch.getHostType() == SSHFS.HOST_TYPE_OSX) {
			assertEquals(devNull.getDevMajor(), 3);
			assertEquals(devNull.getDevMinor(), 2);
		}
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
