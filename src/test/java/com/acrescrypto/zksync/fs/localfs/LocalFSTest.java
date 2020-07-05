package com.acrescrypto.zksync.fs.localfs;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

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

	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}

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
		TestUtils.stopDebugMode();
	}
	
	@Override
	public void prepareExamples() throws IOException {
		if(examplesPrepared) return;
		
		scratch.write("regularfile", "just a regular ol file".getBytes());
		scratch.mkdir("directory");
		scratch.link("regularfile", "hardlink");
		if(!Util.isWindows()) {
			scratch.chmod  ("directory",   0755);
			scratch.chmod  ("regularfile", 0664);
			scratch.mkfifo ("fifo");
			scratch.symlink("regularfile", "symlink");
		}
		
		examplesPrepared = true;
	}
	
	@Test @Override
	public void testStatRegularFile() throws IOException {
		Stat stat = scratch.stat("regularfile");
		assertTrue(stat.isRegularFile());
		assertEquals(22, stat.getSize());
		assertTrue(stat.getCtime() >  0);
		assertTrue(stat.getAtime() >  0);
		assertTrue(stat.getMtime() >  0);
		if(!Util.isWindows()) {
			assertEquals(stat.getMode(),          0664);
			assertEquals(expectedUid (), stat.getUid());
			assertEquals(expectedGid (), stat.getGid());
		}
	}
	
	@Test @Override
	public void testStatDirectory() throws IOException {
		Stat stat = scratch.stat("directory");
		assertTrue(stat.isDirectory());
		assertTrue(stat.getCtime() > 0);
		assertTrue(stat.getAtime() > 0);
		assertTrue(stat.getMtime() > 0);
		if(!Util.isWindows()) {
			assertEquals(0755, stat.getMode());
			assertEquals(expectedUid(), stat.getUid());
			assertEquals(expectedGid(), stat.getGid());
		}
	}
	
	@Test @Override
	public void testStatGetsInodeId() throws IOException {
		// probably need to skip this on windows
		assumeTrue("Inode IDs not tested on Windows", !Util.isWindows());
		super.testStatGetsInodeId();
	}
	
	@Test @Override
	public void testStatFollowsSymlinks() throws IOException {
		assumeTrue("Symlinks not tested on Windows", !Util.isWindows());
		super.testStatFollowsSymlinks();
	}
	
	@Test @Override
	public void testLstatDoesntFollowSymlinks() throws IOException {
		assumeTrue("Symlinks not tested on Windows", !Util.isWindows());
		super.testLstatDoesntFollowSymlinks();
	}
	
	@Test @Override
	public void testSymlink() throws IOException {
	    assumeTrue("Symlinks not tested on Windows", !Util.isWindows());
	    super.testSymlink();
	}
	
	@Test @Override
	public void testSymlinkUnsafe() throws IOException {
		assumeTrue("Symlinks not tested on Windows", !Util.isWindows());
		super.testSymlinkUnsafe();
	}
	
	@Test @Override
	public void testReadlink() throws IOException {
		assumeTrue("symlinks not tested on windows", !Util.isWindows());
		super.testReadlink();
	}
	
	@Test @Override
	public void testReadlinkWhenSymlinkPointsToDirectory() throws IOException {
		assumeTrue("Symlinks not tested on Windows", !Util.isWindows());
		super.testReadlinkWhenSymlinkPointsToDirectory();
	}
	
	@Test @Override
	public void testMkfifo() throws IOException {
		assumeTrue("FIFOs not tested on Windows", !Util.isWindows());
		super.testMkfifo();
	}

	@Test @Override
	public void testChmod() throws IOException {
		assumeTrue("chmod not tested on Windows", !Util.isWindows());
		super.testChmod();
	}

	@Test @Override
	public void testMvMovesSymlinksIntoDirectoriesWithoutAlteringTarget() throws IOException {
		assumeTrue("Symlinks not tested on Windows", !Util.isWindows());
		super.testMvMovesSymlinksIntoDirectoriesWithoutAlteringTarget();
	}
	
	@Test @Override
	public void testMvMovesDirectoriesIntoSubdirectoriesWhenTargetIsSymlinkToDirectory() throws IOException {
		assumeTrue("Symlinks not tested on Windows", !Util.isWindows());
		super.testMvMovesDirectoriesIntoSubdirectoriesWhenTargetIsSymlinkToDirectory();
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
