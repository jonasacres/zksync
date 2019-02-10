package com.acrescrypto.zksync.fs.localfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;

import org.junit.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.FSTestBase;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.utility.Util;


public class LocalFSTest extends FSTestBase {
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
	
	protected static int getCurrentId(char type) {
		try {
		    String userName = System.getProperty("user.name");
		    String command = "id -" + type + " "+userName;
		    Process child = Runtime.getRuntime().exec(command);

		    // Get the input stream and read from it
		    InputStream in = child.getInputStream();
		    int uid = Integer.parseInt(new String(IOUtils.toByteArray(in)).replaceAll("\n", ""));
		    in.close();
		    return uid;
		} catch (IOException e) {
			return -1;
		}
	}

	protected static void deleteFiles() {
		try {
			java.io.File scratchDir = new java.io.File(SCRATCH_DIR);
			FileUtils.deleteDirectory(scratchDir);
			scratchDir.delete();
		} catch (IOException exc) {
			exc.printStackTrace();
		}
	}
	
	@Override
	public int expectedUid() {
		return getCurrentId('u');
	}

	@Override
	public int expectedGid() {
		return getCurrentId('g');
	}

	@Test
	public void testStatIdentifiesDevices() throws IOException {
		if(Util.isWindows()) return;
		try(LocalFS root = new LocalFS("/")) {
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
	}
	
	@Test @Override
	public void testStatIdentifiesFifos() throws IOException {
		if(Util.isWindows()) return;
		super.testStatIdentifiesFifos();
	}

	@Test @Override
	public void testMknodCharacterDevice() throws IOException {
		if(!Util.isSuperuser()) return;
		super.testMknodCharacterDevice();
	}

	@Test @Override
	public void testMknodBlockDevice() throws IOException {
		if(!Util.isSuperuser()) return;
		super.testMknodBlockDevice();
	}

	@Test @Override
	public void testChown() throws IOException {
		if(!Util.isSuperuser()) return;
		super.testChown();
	}

	@Test @Override
	public void testChgrp() throws IOException {
		if(!Util.isSuperuser()) return;
		super.testChgrp();
	}
}
