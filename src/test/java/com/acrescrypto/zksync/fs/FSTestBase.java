package com.acrescrypto.zksync.fs;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;

import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.exceptions.EISDIRException;
import com.acrescrypto.zksync.exceptions.EISNOTDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.ENOTEMPTYException;
import com.acrescrypto.zksync.exceptions.FileTypeNotSupportedException;
import com.acrescrypto.zksync.utility.Util;

public abstract class FSTestBase {
	protected FS scratch;
	protected boolean examplesPrepared = false;

	interface LambdaOp { void op() throws IOException; }
	void expectENOENT(LambdaOp op) throws IOException {
		try {
			op.op();
			fail();
		}
		catch(ENOENTException exc) {}
		catch(UnsupportedOperationException exc) {}
		catch(FileTypeNotSupportedException exc) {}
	}
	
	void expectIOException(LambdaOp op) throws IOException {
		try {
			op.op();
			fail();
		}
		catch(IOException exc) {}
		catch(UnsupportedOperationException exc) {}
	}
	
	void expectInScope(FS scoped, String path, LambdaOp op) throws IOException {
		assertFalse(scoped.exists(path));
		assertFalse(scratch.exists(path));
		try {
			op.op();
		} catch(UnsupportedOperationException exc) {
			return;
		}
		assertTrue(scoped.exists(path, false));
		assertFalse(scratch.exists(path));
		if(scoped.lstat(path).isDirectory()) {
			scoped.rmdir(path);
		} else {
			scoped.unlink(path);
		}
	}
	
	void expectCantCreate(String path) throws IOException {
		expectIOException(()->scratch.write(path, "test".getBytes()));
		expectIOException(()->scratch.open(path, File.O_RDONLY));
		expectIOException(()->scratch.open(path, File.O_WRONLY|File.O_CREAT));
		expectIOException(()->scratch.mkdir(path));
		expectIOException(()->scratch.mkfifo(path));
		expectIOException(()->scratch.mknod(path, Stat.TYPE_BLOCK_DEVICE, 0, 0));
		expectIOException(()->scratch.mknod(path, Stat.TYPE_CHARACTER_DEVICE, 0, 0));
		expectIOException(()->scratch.link("test", path));
		expectIOException(()->scratch.symlink("test", path));
	}

	public void prepareExamples() throws IOException {
		if(examplesPrepared) return;
		
		scratch.write("regularfile", "just a regular ol file".getBytes());
		scratch.mkdir("directory");
		scratch.link("regularfile", "hardlink");
		scratch.chmod  ("directory",   0755);
		scratch.chmod  ("regularfile", 0664);
		scratch.mkfifo ("fifo");
		scratch.symlink("regularfile", "symlink");
		
		examplesPrepared = true;
	}
	
	public int expectedUid() {
		return 0;
	}
	
	public int expectedGid() {
		return 0;
	}
	
	@Test
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
	
	@Test
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
	
	@Test
	public void testStatGetsInodeId() throws IOException {
		// probably need to skip this on windows
		assertEquals(scratch.stat("regularfile").getInodeId(),   scratch.stat("hardlink") .getInodeId());
		assertFalse (scratch.stat("regularfile").getInodeId() == scratch.stat("directory").getInodeId());
	}
	
	@Test
	public void testStatFollowsSymlinks() throws IOException {
		Stat symStat = scratch.stat("symlink"), fileStat = scratch.stat("regularfile");
		assertEquals(symStat.getInodeId(), fileStat.getInodeId());
		assertFalse (symStat.isSymlink());
		assertTrue  (symStat.isRegularFile());
	}
	
	@Test
	public void testStatIdentifiesFifos() throws IOException {
		Stat fifo = scratch.stat("fifo");
		assertTrue(fifo.isFifo());
	}
	
	@Test
	public void testStatGetsTimes() throws IOException {
		String filename = "timetest";
		scratch.write(filename, "some data".getBytes());
		Stat stat = scratch.stat(filename);
		Long now = System.currentTimeMillis();
		assertEquals(0, Math.abs(stat.getCtime()/(1000l*1000l) - now), 2000);
		
		Long atime = 31337000000000l, mtime = 80085000000000l;
		scratch.setAtime(filename, atime);
		scratch.setMtime(filename, mtime);
		
		stat = scratch.stat(filename);
		assertEquals(0, Math.abs(stat.getAtime() - atime), 50);
		assertEquals(0, Math.abs(stat.getMtime() - mtime), 50);
	}

	@Test
	public void testLstatDoesntFollowSymlinks() throws IOException {
		assumeTrue("Symlinks not tested on Windows", !Util.isWindows());
		Stat symlink = scratch.lstat("symlink");
		assertTrue(symlink.isSymlink());
	}
	
	@Test
	public void testLstatWorksOnRegularFiles() throws IOException {
		Stat stat = scratch.lstat("regularfile");
		assertTrue(stat.isRegularFile());
	}


	@Test
	public void testOpendir() throws IOException {
		Directory dir = scratch.opendir("directory");
		assertEquals(FSPath.with("/directory").standardize(), dir.getPath());
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
	public void testSymlinkUnsafe() throws IOException {
		String target = "/tmp/symlink-target";
		FS unscopedFs = scratch.unscopedFS();
		
		try {
			if(!unscopedFs.exists(unscopedFs.dirname(target))) {
				unscopedFs.mkdirp(unscopedFs.dirname(target));
			}
			unscopedFs.write(target, "over here".getBytes());
			scratch.symlink_unsafe(target, "symlink-link");
			byte[] a = unscopedFs.read(target);
			byte[] b = scratch.read("symlink-link");
			assertTrue(Arrays.equals(a, b));
		} finally {
			unscopedFs.unlink(target);
		}
	}
	
	@Test
	public void testReadlink() throws IOException {
		String target = "doesntexistbutthatsok";
		scratch.symlink(target, "readlink");
		assertEquals(target, scratch.readlink("readlink"));
	}
	
	@Test
	public void testReadlinkWhenSymlinkPointsToDirectory() throws IOException {
		scratch.mkdir("dir");
		scratch.symlink("dir", "readlink");
		assertEquals("dir", scratch.readlink("readlink"));
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
		assumeTrue("FIFOs not tested on Windows", !Util.isWindows());
		scratch.mkfifo("mkfifo");
		assertTrue(scratch.stat("mkfifo").isFifo());
	}

	@Test
	public void testChmod() throws IOException {
		assumeTrue("chmod not tested on Windows", !Util.isWindows());
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
		scratch.chown("chown", System.getProperty("user.name"));
		assertEquals(System.getProperty("user.name"), scratch.stat("chown").getUser());
	}

	@Test
	public void testChgrp() throws IOException {
		scratch.write("chgrp", "contents".getBytes());
		scratch.chgrp("chgrp", "root");
		assertEquals("root", scratch.stat("chgrp").getGroup());
		scratch.chgrp("chgrp", System.getProperty("user.name"));
		assertEquals(System.getProperty("user.name"), scratch.stat("chgrp").getGroup());
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
	public void testMvLinksFileInNewLocation() throws IOException {
		scratch.write("file", "test".getBytes());
		scratch.mv("file", "new");
		assertTrue(scratch.exists("new"));
		assertFalse(scratch.exists("file"));
		assertArrayEquals("test".getBytes(), scratch.read("new"));
	}
	
	@Test
	public void testMvOverwritesIfNewLocationTaken() throws IOException {
		scratch.write("file1", "test1".getBytes());
		scratch.write("file2", "test2".getBytes());
		
		scratch.mv("file1", "file2");
		
		assertFalse(scratch.exists("file1"));
		assertTrue(scratch.exists("file2"));
		assertArrayEquals("test1".getBytes(), scratch.read("file2"));
	}
	
	@Test
	public void testMvMovesFileIntoDirectoryIfDestIsDirectory() throws IOException {
		scratch.write("file", "test".getBytes());
		scratch.mkdir("dir");
		scratch.mv("file", "dir");
		assertTrue(scratch.exists("dir/file"));
		assertFalse(scratch.exists("file"));
		assertArrayEquals("test".getBytes(), scratch.read("dir/file"));
	}
	
	@Test
	public void testMvThrowsEEXISTSIfDestIsDirectoryContainingFilename() throws IOException {
		scratch.write("file", "test".getBytes());
		scratch.mkdir("dir");
		scratch.mkdir("dir/file");
		try {
			scratch.mv("file", "dir");
			fail("expected EEXISTSException");
		} catch(EEXISTSException exc) {
		}
		
		assertTrue(scratch.exists("dir/file"));
		assertTrue(scratch.stat("dir/file").isDirectory());
		assertTrue(scratch.exists("file"));
		assertArrayEquals("test".getBytes(), scratch.read("file"));
	}
	
	@Test
	public void testMvRenamesDirectories() throws IOException {
		scratch.mkdir("dir");
		scratch.mv("dir", "dir2");
		assertTrue(scratch.exists("dir2"));
		assertFalse(scratch.exists("dir"));
	}
	
	@Test
	public void testMvMovesDirectoriesIntoSubdirectoriesWhenTargetIsDirectory() throws IOException {
		scratch.mkdir("dir");
		scratch.mkdir("dest");
		scratch.mv("dir", "dest");
		assertTrue(scratch.exists("dest/dir"));
		assertFalse(scratch.exists("dir"));
	}
	
	@Test
	public void testMvMovesSymlinksIntoDirectoriesWithoutAlteringTarget() throws IOException {
		assumeTrue("Symlinks not tested on Windows", !Util.isWindows());
		scratch.purge();
		scratch.mkdir("dir");
		scratch.write("target", "foo".getBytes());
		scratch.symlink("/target", "symlink");
		scratch.mv("symlink", "dir");
		assertTrue(scratch.lstat("dir/symlink").isSymlink());
		assertTrue(scratch.stat("target").isRegularFile());
		assertEquals("/target", scratch.readlink("dir/symlink"));
	}
	
	@Test
	public void testMvMovesDirectoriesIntoSubdirectoriesWhenTargetIsSymlinkToDirectory() throws IOException {
		assumeTrue("Symlinks not tested on Windows", !Util.isWindows());
		scratch.mkdir("dir");
		scratch.mkdir("destdir");
		scratch.symlink("destdir", "dest");
		scratch.mv("dir", "dest");
		assertTrue(scratch.exists("dest/dir"));
		assertTrue(scratch.exists("destdir/dir"));
		assertFalse(scratch.exists("dir"));
	}
	
	@Test(expected=EEXISTSException.class)
	public void testMvDoesNotMovesDirectoriesWhenTargetIsFile() throws IOException {
		scratch.mkdir("dir");
		scratch.write("dest", "foo".getBytes());
		scratch.mv("dir", "dest");
	}
	
	@Test(expected=EEXISTSException.class)
	public void testMvDoesNotMovesDirectoriesWhenTargetIsDirectoryAndContainsFileWithName() throws IOException {
		scratch.mkdir("dir");
		scratch.write("dest/dir", "foo".getBytes());
		scratch.mv("dir", "dest");
	}
	
	@Test(expected=ENOTEMPTYException.class)
	public void testMvDoesNotMovesDirectoriesWhenTargetIsDirectoryAndContainsNonemptyDirectoryWithName() throws IOException {
		scratch.mkdir("dir");
		scratch.write("dest/dir/file", "foo".getBytes());
		scratch.mv("dir", "dest");
	}
	
	@Test
	public void testMvOverwritesDirectoriesWhenTargetIsDirectoryAndContainsEmptySubdirectoryWithName() throws IOException {
		scratch.mkdir("dir");
		scratch.write("dir/file", "foo".getBytes());
		scratch.mkdirp("dest/dir");
		scratch.mv("dir", "dest");
		assertTrue(scratch.exists("dest/dir/file"));
	}
	
	@Test
	public void testCpCopiesFileToNewLocation() throws IOException {
		scratch.write("file", "test".getBytes());
		scratch.cp("file", "new");
		assertTrue(scratch.exists("new"));
		assertTrue(scratch.exists("file"));
		assertArrayEquals("test".getBytes(), scratch.read("new"));
		assertArrayEquals("test".getBytes(), scratch.read("file"));
	}
	
	@Test
	public void testCpDoesNotCreateHardlink() throws IOException {
		scratch.write("file", "test".getBytes());
		scratch.cp("file", "new");
		scratch.write("file", "changed".getBytes());
		assertArrayEquals("changed".getBytes(), scratch.read("file"));
		assertArrayEquals("test".getBytes(), scratch.read("new"));
	}
	
	@Test
	public void testCpOverwritesIfNewLocationTaken() throws IOException {
		scratch.write("file1", "test1".getBytes());
		scratch.write("file2", "test2".getBytes());
		
		scratch.cp("file1", "file2");
		
		assertTrue(scratch.exists("file1"));
		assertTrue(scratch.exists("file2"));
		assertArrayEquals("test1".getBytes(), scratch.read("file2"));
	}
	
	@Test
	public void testCpCopiesFileIntoDirectoryIfDestIsDirectory() throws IOException {
		scratch.write("file", "test".getBytes());
		scratch.mkdir("dir");
		scratch.cp("file", "dir");
		assertTrue(scratch.exists("dir/file"));
		assertTrue(scratch.exists("file"));
		assertArrayEquals("test".getBytes(), scratch.read("dir/file"));
	}
	
	@Test
	public void testCpThrowsEISDIRIfDestIsDirectoryContainingFIlename() throws IOException {
		scratch.write("file", "test".getBytes());
		scratch.mkdir("dir");
		scratch.mkdir("dir/file");
		try {
			scratch.cp("file", "dir");
			fail("expected EISDIRException");
		} catch(EISDIRException exc) {
		}
		
		assertTrue(scratch.exists("dir/file"));
		assertTrue(scratch.stat("dir/file").isDirectory());
		assertTrue(scratch.exists("file"));
		assertArrayEquals("test".getBytes(), scratch.read("file"));
	}

	@Test
	public void testOpenWithoutCreateDoesntMakeFiles() throws IOException {
		try {
			try(File file = scratch.open("shouldntexist", File.O_RDONLY)) {}
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
	
	@Test
	public void testOpenRootDir() throws IOException {
		scratch.opendir("/");
	}
	
	@Test
	public void testScopedFS() throws IOException {
		scratch.mkdir("scoped");
		scratch.write("basefile",    "data".getBytes());
		scratch.write("scoped/file", "data".getBytes());
		
		FS scoped = scratch.scopedFS("scoped");
		assertFalse(scoped.exists("scoped/file"));
		assertFalse(scoped.exists("basefile"));
		assertTrue (scoped.exists("file"));
		
		scoped.write("file2", "data".getBytes());
		assertFalse(scoped .exists("scoped/file2"));
		assertTrue (scoped .exists("file2"));
		assertFalse(scratch.exists("file2"));
		assertTrue (scratch.exists("scoped/file2"));
		
		assertTrue(scoped.opendir("/").contains("file2"));
	}
	
	@Test
	public void testPurge() throws IOException {
		scratch.write("a/b/c/d", "test data".getBytes());
		scratch.write("a/b/e",   "test data".getBytes());
		scratch.write("a/b/g",   "test data".getBytes());
		try {
			scratch.mkfifo("special");
		} catch(UnsupportedOperationException exc) {};
		scratch.purge();
		
		assertTrue (scratch.exists("/"));
		assertFalse(scratch.exists("a/b/c/d"));
		assertFalse(scratch.exists("a/b/e"));
		assertFalse(scratch.exists("a/b/g"));
		assertFalse(scratch.exists("a/b"));
		assertFalse(scratch.exists("a"));
		assertFalse(scratch.exists("special"));
	}
	
	@Test
	public void testScopedPurge() throws IOException {
		scratch.mkdir("scoped");
		FS scoped = scratch.scopedFS("scoped");
		
		scoped.write("a/b/c/d", "test data".getBytes());
		scoped.write("a/b/e", "test data".getBytes());
		scoped.write("a/b/g", "test data".getBytes());
		try {
			scoped.mkfifo("fifo");
		} catch(UnsupportedOperationException exc) {}
		scoped.purge();
		
		assertTrue(scoped.exists("/"));
		assertFalse(scoped.exists("a/b/c/d"));
		assertFalse(scoped.exists("a/b/e"));
		assertFalse(scoped.exists("a/b/g"));
		assertFalse(scoped.exists("a/b"));
		assertFalse(scoped.exists("a"));
		assertFalse(scoped.exists("fifo"));
	}
	
	@Test
	public void testScopedMakesDirectory() throws IOException {
		assertFalse(scratch.exists("a/b/c/d"));
		FS scoped = scratch.scopedFS("a/b/c/d");
		assertTrue(scoped.exists("/"));
		assertTrue(scratch.exists("a/b/c/d"));
	}
	
	@Test
	public void testWriteBlankFiles() throws IOException {
		scratch.write("blank", new byte[0]);
		assertTrue(scratch.exists("blank"));
		assertEquals(0, scratch.read("blank").length);
	}
	
	@Test
	public void testWriteWithOffset() throws IOException {
		byte[] buf = "hello world".getBytes();
		scratch.write("test", buf, 1, 9);
		assertTrue(Arrays.equals("ello worl".getBytes(), scratch.read("test")));
	}
	
	@Test
	public void testCantEscapeScopedFSUsingDotDot() throws IOException {
		FS scoped;
		try {
			scoped = scratch.scopedFS("scoped");
		} catch(UnsupportedOperationException exc) {
			return;
		}
		
		scratch.write("shouldntexist", "".getBytes());
		scratch.mkdir("empty");
		scoped.write("exists", "exists".getBytes());
		
		assertFalse(scoped.exists("../shouldntexist"));
		expectENOENT(()->scoped.stat("../shouldntexist"));
		expectENOENT(()->scoped.lstat("../shouldntexist"));
//		expectENOENT(()->scoped.open("../shouldntexist", File.O_RDONLY).close());
		expectENOENT(()->scoped.open("../cantexist", File.O_WRONLY|File.O_CREAT).close());
//		expectENOENT(()->scoped.opendir("../empty"));
//		expectENOENT(()->scoped.write("../cantexist", "test".getBytes()));
//		expectENOENT(()->scoped.mkdir("../cantexist"));
//		expectENOENT(()->scoped.mkdirp("../cantexist"));
//		expectENOENT(()->scoped.rmdir("../empty"));
//		expectENOENT(()->scoped.unlink("../shouldntexist"));
//		expectENOENT(()->scoped.link("exists", "../cantexist"));
//		expectENOENT(()->scoped.link("../shouldntexist", "cantexist2"));
//		expectENOENT(()->scoped.symlink("../shouldntexist", "cantexist2"));
//		expectENOENT(()->scoped.symlink("exists", "../cantexist"));
//		expectENOENT(()->scoped.readlink("../symlink"));
//		expectENOENT(()->scoped.mknod("../cantexist", Stat.TYPE_BLOCK_DEVICE, 0, 0));
//		expectENOENT(()->scoped.mknod("../cantexist", Stat.TYPE_CHARACTER_DEVICE, 0, 0));
//		expectENOENT(()->scoped.mkfifo("../cantexist"));
//		expectENOENT(()->scoped.chmod("../shouldntexist", 0777));
//		expectENOENT(()->scoped.chown("../shouldntexist", 0));
//		expectENOENT(()->scoped.chown("../shouldntexist", "root"));
//		expectENOENT(()->scoped.chgrp("../shouldntexist", 0));
//		expectENOENT(()->scoped.chgrp("../shouldntexist", Util.isOSX() ? "wheel" : "root"));
//		expectENOENT(()->scoped.setMtime("../shouldntexist", 12345));
//		expectENOENT(()->scoped.setCtime("../shouldntexist", 12345));
//		expectENOENT(()->scoped.setAtime("../shouldntexist", 12345));
//		expectENOENT(()->scoped.truncate("../shouldntexist", 0));
//		expectENOENT(()->scoped.scopedFS("../empty"));
	}

	@Test
	public void testCantEscapeScopedFSUsingSlash() throws IOException {
		FS scoped;
		try {
			scoped = scratch.scopedFS("scoped");
		} catch(UnsupportedOperationException exc) {
			return;
		}
		
		scratch.write("shouldntexist", "".getBytes());
		
		assertFalse(scoped.exists("/shouldntexist"));
		expectENOENT(()->scoped.stat("/shouldntexist"));
		expectENOENT(()->scoped.lstat("/shouldntexist"));
		expectENOENT(()->scoped.open("/shouldntexist", File.O_RDONLY));
		expectENOENT(()->scoped.opendir("/empty"));
		expectENOENT(()->scoped.rmdir("/empty"));
		expectENOENT(()->scoped.unlink("/shouldntexist"));
		expectENOENT(()->scoped.link("exists", "/cantexist"));
		expectENOENT(()->scoped.link("/shouldntexist", "cantexist2"));
		expectENOENT(()->scoped.readlink("/symlink"));
		expectENOENT(()->scoped.chmod("/shouldntexist", 0777));
		expectENOENT(()->scoped.chown("/shouldntexist", 0));
		expectENOENT(()->scoped.chown("/shouldntexist", "root"));
		expectENOENT(()->scoped.chgrp("/shouldntexist", 0));
		expectENOENT(()->scoped.chgrp("/shouldntexist", Util.isOSX() ? "wheel" : "root"));
		expectENOENT(()->scoped.setMtime("/shouldntexist", 12345));
		expectENOENT(()->scoped.setCtime("/shouldntexist", 12345));
		expectENOENT(()->scoped.setAtime("/shouldntexist", 12345));
		
		expectInScope(scoped, "mustbescoped", ()->scoped.open    ("/mustbescoped",  File.O_WRONLY|File.O_CREAT).close());
		expectInScope(scoped, "mustbescoped", ()->scoped.open    ("/mustbescoped",  File.O_WRONLY|File.O_CREAT).close());
		expectInScope(scoped, "mustbescoped", ()->scoped.write   ("/mustbescoped",  "test".getBytes()));
		expectInScope(scoped, "mustbescoped", ()->scoped.mkdir   ("/mustbescoped"));
		expectInScope(scoped, "mustbescoped", ()->scoped.mkdirp  ("/mustbescoped"));
		expectInScope(scoped, "mustbescoped", ()->scoped.symlink ("/shouldntexist", "mustbescoped"));
		expectInScope(scoped, "mustbescoped", ()->scoped.symlink ("exists",         "/mustbescoped"));
		expectInScope(scoped, "mustbescoped", ()->scoped.mknod   ("/mustbescoped",  Stat.TYPE_BLOCK_DEVICE, 0, 0));
		expectInScope(scoped, "mustbescoped", ()->scoped.mknod   ("/mustbescoped",  Stat.TYPE_CHARACTER_DEVICE, 0, 0));
		expectInScope(scoped, "mustbescoped", ()->scoped.mkfifo  ("/mustbescoped"));
		expectInScope(scoped, "mustbescoped", ()->scoped.truncate("/mustbescoped",   0));
		expectInScope(scoped, "mustbescoped", ()->scoped.scopedFS("/mustbescoped"));
	}
	
	@Test
	public void testCantCreateFileNamedDotDot() throws IOException {
		expectCantCreate("..");
	}
	
	@Test
	public void testCantCreateFileNamedDot() throws IOException {
		expectCantCreate(".");
	}
	
	@Test
	public void testCantCreateFileWithBlankName() throws IOException {
		expectCantCreate("");
	}
	
	@Test
	public void testCantCreateFileNamedSlash() throws IOException {
		expectCantCreate("/");
		expectCantCreate("directory/");
	}
}
