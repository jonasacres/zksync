package com.acrescrypto.zksync.fs.localfs;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.commons.io.FileUtils;

import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;


public class LocalFSTest {
	
	 // TODO: need a much better solution than hard-coded paths in my home directory!
	public final static String TEST_DIR = "/home/jonas/localfstest";
	public final static String SCRATCH_DIR = "/home/jonas/localfstest/scratch";
	
	LocalFS fs = new LocalFS(TEST_DIR);
	LocalFS scratch = new LocalFS(SCRATCH_DIR);
	
	@BeforeClass
	public static void beforeClass() {
		java.io.File scratchDir = new java.io.File(SCRATCH_DIR);
		try {
			FileUtils.deleteDirectory(scratchDir);
		} catch (IOException e) {}
		scratchDir.mkdirs();
	}
	
	@Test
	public void testStatRegularFile() throws IOException {
		Stat stat = fs.stat("regularfile");
		assertTrue(stat.isRegularFile());
		assertEquals(stat.getMode(), 0664);
		assertEquals(stat.getSize(), 12);
		assertEquals(stat.getUid(), 0);
		assertEquals(stat.getGid(), 0);
		assertEquals(stat.getUser(), "root");
		assertTrue(stat.getGroup().equals("root") || stat.getGroup().equals("wheel"));
		assertTrue(stat.getCtime() > 0);
		assertTrue(stat.getAtime() > 0);
		assertTrue(stat.getMtime() > 0);
	}
	
	@Test
	public void testStatDirectory() throws IOException {
		Stat stat = fs.stat("directory");
		assertTrue(stat.isDirectory());
		assertEquals(stat.getMode(), 0775);
		assertEquals(stat.getUid(), 0);
		assertEquals(stat.getGid(), 0);
		assertEquals(stat.getUser(), "root");
		assertTrue(stat.getGroup().equals("root") || stat.getGroup().equals("wheel"));
		assertTrue(stat.getCtime() > 0);
		assertTrue(stat.getAtime() > 0);
		assertTrue(stat.getMtime() > 0);
	}
	
	@Test
	public void testStatGetsInodeId() throws IOException {
		// probably need to skip this on windows
		assertEquals(fs.stat("regularfile").getInodeId(), fs.stat("hardlink").getInodeId());
		assertFalse(fs.stat("regularfile").getInodeId() == fs.stat("directory").getInodeId());
	}
	
	@Test
	public void testStatFollowsSymlinks() throws IOException {
		Stat symStat = fs.stat("symlink"), fileStat = fs.stat("regularfile");
		assertEquals(symStat.getInodeId(), fileStat.getInodeId());
		assertFalse(symStat.isSymlink());
		assertTrue(symStat.isRegularFile());
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
	public void testStatIdentifiesFifos() throws IOException {
		// TODO: skip on windows
		// TODO: how the hell will i make this portable
		Stat fifo = fs.stat("fifo");
		assertTrue(fifo.isFifo());
	}
	
	@Test
	public void testStatGetsTimes() throws IOException {
		String filename = "timetest";
		scratch.write(filename, "some data".getBytes());
		Stat stat = scratch.stat(filename);
		Long now = System.currentTimeMillis();
		assertEquals(0, Math.abs(stat.getCtime()/(1000l*1000l) - now), 10);
		
		Long atime = 31337000000l, mtime = 80085000000l;
		scratch.setAtime(filename, atime);
		scratch.setMtime(filename, mtime);
		
		stat = scratch.stat(filename);
		assertTrue(stat.getAtime() == atime);
		assertTrue(stat.getMtime() == mtime);
	}

	@Test
	public void testLstatDoesntFollowSymlinks() throws IOException {
		Stat symlink = fs.lstat("symlink");
		assertTrue(symlink.isSymlink());
	}
	
	@Test
	public void testLstatWorksOnRegularFiles() throws IOException {
		Stat symlink = fs.lstat("regularfile");
		assertTrue(symlink.isRegularFile());
	}


	@Test
	public void testOpendir() throws IOException {
		LocalDirectory dir = fs.opendir("directory");
		assertEquals(dir.getPath(), "directory");
	}

	@Test
	public void testMkdir() throws IOException {
		scratch.mkdir("mkdirtest");
		Stat stat = scratch.stat("mkdirtest");
		assertTrue(stat.isDirectory());
	}

	@Test
	public void testRmdir() throws IOException {
		scratch.mkdir("rmdirtest");
		Stat stat = scratch.stat("rmdirtest");
		assertTrue(stat.isDirectory());
		scratch.rmdir("rmdirtest");
		assertFalse(scratch.exists("rmdirtest"));
	}

	@Test
	public void testUnlink() throws IOException {
		scratch.write("testunlink", "life is fleeting".getBytes());
		assertTrue(scratch.exists("testunlink"));
		scratch.unlink("testunlink");
		assertFalse(scratch.exists("testunlink"));
	}

	@Test
	public void testLink() throws IOException {
		scratch.write("testlink", "i'm here and there".getBytes());
		scratch.link("testlink", "testlink2");
		
		Stat srcStat = scratch.stat("testlink"), destStat = scratch.stat("testlink2");
		assertFalse(srcStat.isSymlink());
		assertFalse(destStat.isSymlink());
		assertEquals(srcStat.getInodeId(), destStat.getInodeId());
	}

	@Test
	public void testSymlink() throws IOException {
		scratch.write("symlink-target", "over here".getBytes());
		scratch.symlink("symlink-target", "symlink");
		byte[] a = scratch.read("symlink-target"), b = scratch.read("symlink");
		assertTrue(Arrays.equals(a, b));
	}
	
	@Test
	public void testReadlink() throws IOException {
		String target = "doesntexistbutthatsok";
		scratch.symlink(target, "readlink");
		assertEquals(target, scratch.readlink("readlink"));
	}

	@Test
	public void testMknod() {
	  // TODO: Implement... but what to do about superuser privileges?
	}

	@Test
	public void testMkfifo() throws IOException {
		scratch.mkfifo("fifo");
		assertTrue(scratch.stat("fifo").isFifo());
	}

	@Test
	public void testChmod() throws IOException {
		scratch.write("chmod", "contents".getBytes());
		scratch.chmod("chmod", 0777);
		assertEquals(0777, scratch.stat("chmod").getMode());
		scratch.chmod("chmod", 0642);
		assertEquals(0642, scratch.stat("chmod").getMode());
	}

	@Test
	public void testChown() {
	  // TODO: Implement... still needs superuser though
	}

	@Test
	public void testChgrp() {
	  // TODO: Implement
	}

	@Test
	public void testSetMtime() throws IOException {
		long ts = 12340000000l;
		scratch.write("mtime", "tick tock".getBytes());
		scratch.setMtime("mtime", ts);
		assertEquals(ts, scratch.stat("mtime").getMtime());
	}

	@Test
	public void testSetAtime() throws IOException {
		long ts = 4321000000l;
		scratch.write("atime", "clock rock".getBytes());
		scratch.setAtime("atime", ts);
		assertEquals(ts, scratch.stat("atime").getAtime());
	}

	@Test
	public void testWrite() throws IOException {
		byte[] text = "Hi! I have data in me!".getBytes();
		scratch.write("writetest", text);
		assertTrue(Arrays.equals(text, scratch.read("writetest")));
	}

	@Test
	public void testOpenWithoutCreateDoesntMakeFiles() throws IOException {
		try {
			scratch.open("shouldntexist", File.O_RDONLY);
			throw new RuntimeException("expected exception");
		} catch(FileNotFoundException e) {
			assertFalse(scratch.exists("shouldntexist"));
		}
	}
	
	@Test
	public void testOpenWithoutCreateWorksOnExistingFiles() throws IOException {
		fs.open("regularfile", File.O_RDONLY).close();
	}
	
	@Test
	public void testOpenWithCreateWorksOnExistingFiles() throws IOException {
		byte[] text = "some text".getBytes();
		scratch.write("O_CREAT_preexisting", text);
		scratch.open("O_CREAT_preexisting", File.O_RDONLY|File.O_CREAT).close();
		assertTrue(Arrays.equals(text, scratch.read("O_CREAT_preexisting")));
	}
	
	@Test
	public void testOpenWithCreateWorksOnNonexistingFiles() throws IOException {
		assertFalse(scratch.exists("O_CREAT_nonpreexisting"));
		scratch.open("O_CREAT_nonpreexisting", File.O_WRONLY|File.O_CREAT).close();
		assertTrue(scratch.exists("O_CREAT_nonpreexisting"));
	}
}
