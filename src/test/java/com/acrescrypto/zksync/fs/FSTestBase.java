package com.acrescrypto.zksync.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;

import com.acrescrypto.zksync.exceptions.EISNOTDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;

public class FSTestBase extends Object {
	protected FS scratch;
	protected boolean examplesPrepared = false;
	
	public void prepareExamples() throws IOException {
		if(examplesPrepared) return;
		
		scratch.write("regularfile", "just a regular ol file".getBytes());
		scratch.chmod("regularfile", 0664);
		scratch.mkdir("directory");
		scratch.chmod("directory", 0755);
		scratch.link("regularfile", "hardlink");
		scratch.mkfifo("fifo");
		scratch.symlink("regularfile", "symlink");
		
		examplesPrepared = true;
	}
	
	@Test
	public void testStatRegularFile() throws IOException {
		Stat stat = scratch.stat("regularfile");
		assertTrue(stat.isRegularFile());
		assertEquals(stat.getMode(), 0664);
		assertEquals(22, stat.getSize());
		// TODO: need a good way to test UID/GID stuff
		assertTrue(stat.getCtime() > 0);
		assertTrue(stat.getAtime() > 0);
		assertTrue(stat.getMtime() > 0);
	}
	
	@Test
	public void testStatDirectory() throws IOException {
		Stat stat = scratch.stat("directory");
		assertTrue(stat.isDirectory());
		assertEquals(0755, stat.getMode());
		// TODO: need a good way to test UID/GID stuff
		assertTrue(stat.getCtime() > 0);
		assertTrue(stat.getAtime() > 0);
		assertTrue(stat.getMtime() > 0);
	}
	
	@Test
	public void testStatGetsInodeId() throws IOException {
		// probably need to skip this on windows
		assertEquals(scratch.stat("regularfile").getInodeId(), scratch.stat("hardlink").getInodeId());
		assertFalse(scratch.stat("regularfile").getInodeId() == scratch.stat("directory").getInodeId());
	}
	
	@Test
	public void testStatFollowsSymlinks() throws IOException {
		Stat symStat = scratch.stat("symlink"), fileStat = scratch.stat("regularfile");
		assertEquals(symStat.getInodeId(), fileStat.getInodeId());
		assertFalse(symStat.isSymlink());
		assertTrue(symStat.isRegularFile());
	}
	
	@Test
	public void testStatIdentifiesFifos() throws IOException {
		// TODO: skip on windows for LocalFS.
		Stat fifo = scratch.stat("fifo");
		assertTrue(fifo.isFifo());
	}
	
	@Test
	public void testStatGetsTimes() throws IOException {
		String filename = "timetest";
		scratch.write(filename, "some data".getBytes());
		Stat stat = scratch.stat(filename);
		Long now = System.currentTimeMillis();
		assertEquals(0, Math.abs(stat.getCtime()/(1000l*1000l) - now), 1000);
		
		Long atime = 31337000000000l, mtime = 80085000000000l;
		scratch.setAtime(filename, atime);
		scratch.setMtime(filename, mtime);
		
		stat = scratch.stat(filename);
		assertEquals(0, Math.abs(stat.getAtime() - atime), 50);
		assertEquals(0, Math.abs(stat.getMtime() - mtime), 50);
	}

	@Test
	public void testLstatDoesntFollowSymlinks() throws IOException {
		Stat symlink = scratch.lstat("symlink");
		assertTrue(symlink.isSymlink());
	}
	
	@Test
	public void testLstatWorksOnRegularFiles() throws IOException {
		Stat symlink = scratch.lstat("regularfile");
		assertTrue(symlink.isRegularFile());
	}


	@Test
	public void testOpendir() throws IOException {
		Directory dir = scratch.opendir("directory");
		assertEquals(dir.getPath(), "/directory");
	}
	
	@Test(expected=ENOENTException.class)
	public void testOpendirThrowsFileNotFound() throws IOException {
		scratch.opendir("doesntexist");
	}
	
	@Test(expected=EISNOTDIRException.class)
	public void testOpendirThrowsEISNOTDIR() throws IOException {
		scratch.opendir("regularfile");
	}

	@Test
	public void testMkdir() throws IOException {
		scratch.mkdir("mkdirtest");
		Stat stat = scratch.stat("mkdirtest");
		assertTrue(stat.isDirectory());
	}
	
	@Test
	public void testMkdirp() throws IOException {
		scratch.mkdirp("mkdirptest/1/2/3");
		Stat stat = scratch.stat("mkdirptest/1/2/3");
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
		scratch.symlink("symlink-target", "symlink-link");
		byte[] a = scratch.read("symlink-target");
		byte[] b = scratch.read("symlink-link");
		assertTrue(Arrays.equals(a, b));
	}
	
	@Test
	public void testReadlink() throws IOException {
		String target = "doesntexistbutthatsok";
		scratch.symlink(target, "readlink");
		assertEquals(target, scratch.readlink("readlink"));
	}

	@Test
	public void testMknodCharacterDevice() throws IOException {
		scratch.mknod("devnull", Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		Stat stat = scratch.stat("devnull");
		assertTrue(stat.isCharacterDevice());
		assertEquals(1, stat.getDevMajor());
		assertEquals(3, stat.getDevMinor());
	}

	@Test
	public void testMknodBlockDevice() throws IOException {
		scratch.mknod("blockdev", Stat.TYPE_BLOCK_DEVICE, 1, 3);
		Stat stat = scratch.stat("blockdev");
		assertTrue(stat.isBlockDevice());
		assertEquals(1, stat.getDevMajor());
		assertEquals(3, stat.getDevMinor());
	}

	@Test
	public void testMkfifo() throws IOException {
		scratch.mkfifo("mkfifo");
		assertTrue(scratch.stat("mkfifo").isFifo());
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
	public void testChown() throws IOException {
		scratch.write("chown", "contents".getBytes());
		scratch.chown("chown", "root");
		assertEquals("root", scratch.stat("chown").getUser());
		scratch.chown("chown", "jonas"); // TODO: needs another user
		assertEquals("jonas", scratch.stat("chown").getUser());
	}

	@Test
	public void testChgrp() throws IOException {
		scratch.write("chgrp", "contents".getBytes());
		scratch.chgrp("chgrp", "root");
		assertEquals("root", scratch.stat("chgrp").getGroup());
		scratch.chgrp("chgrp", "jonas"); // TODO: needs another user
		assertEquals("jonas", scratch.stat("chgrp").getGroup());
	}

	@Test
	public void testSetMtime() throws IOException {
		long ts = 12340000000000l;
		scratch.write("mtime", "tick tock".getBytes());
		scratch.setMtime("mtime", ts);
		assertEquals(ts, scratch.stat("mtime").getMtime());
	}

	@Test
	public void testSetAtime() throws IOException {
		long ts = 4321000000000l;
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
	public void testWriteCreatesDirectories() throws IOException {
		byte[] text = "Hi! I have data in me!".getBytes();
		scratch.write("write-creates-directories/1/2/3/afile", text);
		assertTrue(Arrays.equals(text, scratch.read("write-creates-directories/1/2/3/afile")));
	}
	
	@Test
	public void testTruncateToZero() throws IOException {
		byte[] text = "Die, monster! You don't belong in this world!".getBytes();
		scratch.write("truncatetest", text);
		assertEquals(text.length, scratch.stat("truncatetest").getSize());
		scratch.truncate("truncatetest", 0);
		assertEquals(0, scratch.stat("truncatetest").getSize());
	}
	
	@Test
	public void testTruncateShorter() throws IOException {
		byte[] text = "It was not by my hand that I am once more given flesh".getBytes();
		scratch.write("truncate-short-test", text);
		assertEquals(text.length, scratch.stat("truncate-short-test").getSize());
		scratch.truncate("truncate-short-test", 21);
		assertEquals(21, scratch.stat("truncate-short-test").getSize());
		
		ByteBuffer buf = ByteBuffer.allocate(21);
		buf.put(text, 0, 21);
		assertTrue(Arrays.equals(buf.array(), scratch.read("truncate-short-test")));
	}
	
	@Test
	public void testTruncateLonger() throws IOException {
		byte[] text = "What is a man? A miserable little pile of secrets!".getBytes();
		scratch.write("truncate-longer-test", text);
		assertEquals(text.length, scratch.stat("truncate-longer-test").getSize());
		scratch.truncate("truncate-longer-test", 2*text.length);
		assertEquals(2*text.length, scratch.stat("truncate-longer-test").getSize());
		
		ByteBuffer buf = ByteBuffer.allocate(2*text.length);
		buf.put(text);
		assertTrue(Arrays.equals(buf.array(), scratch.read("truncate-longer-test")));
	}

	@Test
	public void testOpenWithoutCreateDoesntMakeFiles() throws IOException {
		try {
			scratch.open("shouldntexist", File.O_RDONLY);
			throw new RuntimeException("expected exception");
		} catch(ENOENTException e) {
			assertFalse(scratch.exists("shouldntexist"));
		}
	}
	
	@Test
	public void testOpenWithoutCreateWorksOnExistingFiles() throws IOException {
		scratch.open("regularfile", File.O_RDONLY).close();
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
