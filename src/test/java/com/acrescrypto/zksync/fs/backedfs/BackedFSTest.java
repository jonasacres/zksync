package com.acrescrypto.zksync.fs.backedfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.FSTestBase;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;

public class BackedFSTest extends FSTestBase {
	class DummyFS extends FS {
		HashMap<String,Integer> stattedPaths = new HashMap<String,Integer>();
		HashMap<String,Integer> lstattedPaths = new HashMap<String,Integer>();
		HashMap<String,Integer> readPaths = new HashMap<String,Integer>();
		String scope;
		int delay;
		
		boolean accessed;
		
		public DummyFS() {
			this("/");
		}
		
		public DummyFS(String scope) {
			this.scope = scope;
		}
		
		protected byte[] defaultContent() {
			return UNCACHED_DATA;
		}
		
		protected Stat defaultStat() {
			Stat stat = new Stat();
			stat.setGid(0);
			stat.setUid(0);
			stat.setMode(0664);
			stat.setType(Stat.TYPE_REGULAR_FILE);
			stat.setDevMinor(0);
			stat.setDevMajor(0);
			stat.setGroup("root");
			stat.setUser("root");
			stat.setAtime(0);
			stat.setMtime(0);
			stat.setCtime(0);
			stat.setSize(defaultContent().length);
			stat.setInodeId(0);
			
			return stat;
		}
		
		protected void delay() {
			long targetTime = System.currentTimeMillis() + delay;
			while(System.currentTimeMillis() < targetTime) {
				try {
					Thread.sleep(delay);
				} catch(InterruptedException exc) {
				}
			}
		}
		
		@Override
		public Stat stat(String path) throws IOException {
			delay();
			accessed = true;
			stattedPaths.put(path, stattedPaths.getOrDefault(path, 0)+1);
			if(path.equals(UNCACHED_FILE)) {
				return defaultStat();
			}
			
			throw new ENOENTException(path);
		}

		@Override
		public Stat lstat(String path) throws IOException {
			delay();
			accessed = true;
			lstattedPaths.put(path, lstattedPaths.getOrDefault(path, 0)+1);
			if(path.equals(UNCACHED_FILE)) {
				return defaultStat();
			}
			
			throw new ENOENTException(path);
		}
		
		@Override
		public FS scopedFS(String path) throws IOException {
			return new DummyFS(Paths.get(scope, path).toString());
		}
		
		@Override
		public byte[] read(String path) throws IOException {
			delay();
			accessed = true;
			readPaths.put(path, readPaths.getOrDefault(path, 0)+1);
			if(path.equals(UNCACHED_FILE)) {
				return defaultContent();
			}
			
			throw new ENOENTException(path);
		}

		public Directory opendir(String path) throws IOException { throw new RuntimeException("attempted to opendir on backup"); }
		public void mkdir(String path) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void mkdirp(String path) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void rmdir(String path) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void unlink(String path) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void link(String target, String link) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void symlink(String target, String link) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public String readlink(String link) throws IOException { throw new RuntimeException("attempted to read link on backup"); }
		public void mknod(String path, int type, int major, int minor) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void mkfifo(String path) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void chmod(String path, int mode) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void chown(String path, int uid) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void chown(String path, String user) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void chgrp(String path, int gid) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void chgrp(String path, String group) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void setMtime(String path, long mtime) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void setCtime(String path, long ctime) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void setAtime(String path, long atime) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public void write(String path, byte[] contents, int offset, int length) throws IOException { throw new RuntimeException("attempted to write to backup"); }
		public File open(String path, int mode) throws IOException {  throw new RuntimeException("attempted to open file handle from backup");  }
		public void truncate(String path, long size) throws IOException { throw new RuntimeException("attempted to write to backup"); }
	}
	
	final static byte[] CACHED_DATA = { 0, 1, 1, 2, 3, 5, 8, 13 };
	final static String CACHED_DIR = "testdir";
	final static String CACHED_FILE = CACHED_DIR + "/exists";
	final static byte[] UNCACHED_DATA = { 2, 3, 5, 7, 11, 13, 17, 19 };
	final static String UNCACHED_DIR = "testdir-uncached";
	final static String UNCACHED_FILE = UNCACHED_DIR + "/exists-in-backup";
	final static String NONEXISTENT_DIR = "testdir-nonexistent";
	final static String NONEXISTENT_FILE = NONEXISTENT_DIR + "/doesnt-exist";
	BackedFS backedFS;
	RAMFS cacheFS;
	DummyFS backupFS;
	
	public interface Operation {
		void operation() throws IOException;
	}
	
	public void expectPathStat(String path, Operation op) throws IOException {
		assertFalse(backupFS.stattedPaths.getOrDefault(path, 0) > 0);
		op.operation();
		assertTrue(backupFS.stattedPaths.getOrDefault(path, 0) > 0);
		assertFalse(backupFS.readPaths.getOrDefault(path, 0) > 0);
	}
	
	public void expectPathLstat(String path, Operation op) throws IOException {
		assertFalse(backupFS.lstattedPaths.getOrDefault(path, 0) > 0);
		op.operation();
		assertTrue(backupFS.lstattedPaths.getOrDefault(path, 0) > 0);
		assertFalse(backupFS.readPaths.getOrDefault(path, 0) > 0);
	}
	
	
	public void expectRead(String path, Operation op) throws IOException {
		assertFalse(backupFS.stattedPaths.getOrDefault(path, 0) > 0);
		assertFalse(backupFS.readPaths.getOrDefault(path, 0) > 0);
		op.operation();
		assertTrue(backupFS.stattedPaths.getOrDefault(path,	0) > 0);
		assertTrue(backupFS.readPaths.getOrDefault(path, 0) > 0);
	}
	
	public void expectNoRead(String path, Operation op) throws IOException {
		assertFalse(backupFS.stattedPaths.getOrDefault(path, 0) > 0);
		assertFalse(backupFS.readPaths.getOrDefault(path, 0) > 0);
		op.operation();
		assertFalse(backupFS.stattedPaths.getOrDefault(path,	0) > 0);
		assertFalse(backupFS.readPaths.getOrDefault(path, 0) > 0);
	}
	
	public void expectENOENT(Operation op) throws IOException {
		try {
			op.operation();
			fail();
		} catch(ENOENTException exc) {}
	}
	
	public void runThreads(int numThreads, Operation op) {
		Thread[] threads = new Thread[numThreads];
		for(int i = 0; i < numThreads; i++) {
			threads[i] = new Thread(()->{
				try {
					op.operation();
				} catch (IOException e) {
					fail();
				}
			});
			threads[i].start();
		}
		
		for(int i = 0; i < numThreads; i++) {
			while(threads[i].isAlive()) {
				try {
					threads[i].join();
				} catch (InterruptedException e) {}
			}
		}
	}

	public void assertNoBackupAccess() {
		assertFalse(backupFS.accessed);
	}

	public void assertNoBackupAccess(String path) throws IOException {
		assertFalse(backupFS.lstattedPaths.getOrDefault(path, 0) > 0);
		assertFalse(backupFS.stattedPaths.getOrDefault(path, 0) > 0);
		assertFalse(backupFS.readPaths.getOrDefault(path, 0) > 0);
	}

	@Before
	public void beforeEach() throws IOException {
		cacheFS = new RAMFS();
		if(cacheFS.exists("/")) cacheFS.rmrf("/");
		backupFS = new DummyFS();
		cacheFS.write(CACHED_FILE, CACHED_DATA);
		scratch = backedFS = new BackedFS(cacheFS, backupFS);
		prepareExamples();
	}
	
	@After
	public void afterEach() throws IOException {
		if(cacheFS.exists("/")) cacheFS.rmrf("/");
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}
	
	@Test
	public void testStatHitsCacheFirst() throws IOException {
		assertEquals(cacheFS.stat(CACHED_FILE), backedFS.stat(CACHED_FILE));
		assertNoBackupAccess();
	}
	
	@Test
	public void testStatFallsBackOnBackup() throws IOException {
		expectPathStat(UNCACHED_FILE, ()->{
			assertEquals(backupFS.stat(UNCACHED_FILE), backedFS.stat(UNCACHED_FILE));
		});
	}
	
	@Test
	public void testStatThrowsENOENT() throws IOException {
		expectENOENT(()->{
			backedFS.stat(NONEXISTENT_FILE);
		});
	}
	
	@Test
	public void testLstatHitsCacheFirst() throws IOException {
		assertEquals(cacheFS.lstat(CACHED_FILE), backedFS.lstat(CACHED_FILE));
		assertNoBackupAccess();
	}
	
	@Test
	public void testLstatFallsBackOnBackup() throws IOException {
		expectPathLstat(UNCACHED_FILE, ()->{
			assertEquals(backupFS.lstat(UNCACHED_FILE), backedFS.lstat(UNCACHED_FILE));
		});
	}
	
	@Test
	public void testLstatThrowsENOENT() throws IOException {
		expectENOENT(()->{
			backedFS.lstat(NONEXISTENT_FILE);
		});
	}
	
	@Test
	public void testOpendirChecksCache() throws IOException {
		Directory dir = backedFS.opendir(CACHED_DIR);
		Directory ref = cacheFS.opendir(CACHED_DIR);
		assertTrue(dir.getClass().equals(ref.getClass()));
		assertNoBackupAccess();
	}
	
	@Test
	public void testMkdirCreatesDirectoryInCache() throws IOException {
		assertFalse(cacheFS.exists(NONEXISTENT_DIR));
		backedFS.mkdir(NONEXISTENT_DIR);
		assertTrue(cacheFS.exists(NONEXISTENT_DIR));
		assertNoBackupAccess();
	}
	
	@Test
	public void testMkdirpCreatesDirectoryInCache() throws IOException {
		String path = "a/b/c/d";
		assertFalse(cacheFS.exists(path));
		backedFS.mkdirp(path);
		assertTrue(cacheFS.exists(path));
		assertNoBackupAccess();
	}
	
	@Test
	public void testRmdirRemoveDirectoryInCache() throws IOException {
		assertFalse(cacheFS.exists(NONEXISTENT_DIR));
		backedFS.mkdir(NONEXISTENT_DIR);
		assertTrue(cacheFS.exists(NONEXISTENT_DIR));
		backedFS.rmdir(NONEXISTENT_DIR);
		assertFalse(cacheFS.exists(NONEXISTENT_DIR));
		assertNoBackupAccess();
	}
	
	@Test
	public void testUnlinkRemovesFileInCache() throws IOException {
		assertTrue(cacheFS.exists(CACHED_FILE));
		backedFS.unlink(CACHED_FILE);
		assertFalse(cacheFS.exists(CACHED_FILE));
		assertNoBackupAccess();
	}
	
	@Test
	public void testLinkCreatesHardlinks() throws IOException {
		String linkfile = "link";
		assertFalse(cacheFS.exists(linkfile));
		backedFS.link(CACHED_FILE, linkfile);
		assertTrue(cacheFS.exists(linkfile));
		assertEquals(cacheFS.stat(CACHED_FILE).getInodeId(), cacheFS.stat(linkfile).getInodeId());
		assertNoBackupAccess();
	}
	
	@Test
	public void testLinkAcquiresTargetFromBackup() throws IOException {
		String linkfile = "link";
		assertFalse(cacheFS.exists(UNCACHED_FILE));
		expectRead(UNCACHED_FILE, ()->{
			backedFS.link(UNCACHED_FILE, linkfile);
		});
		
		assertTrue(cacheFS.exists(UNCACHED_FILE));
	}
	
	@Test
	public void testLinkCreatesParent() throws IOException {
		String parent = "a", linkfile = parent + "/link";
		assertFalse(cacheFS.exists(parent));
		backedFS.link(CACHED_FILE, linkfile);
		assertTrue(cacheFS.exists(parent));
		assertNoBackupAccess();
	}
	
	@Test
	public void testLinkThrowsENOENT() throws IOException {
		String linkfile = "link";
		expectENOENT(()->{
			backedFS.link(NONEXISTENT_FILE, linkfile);
		});
	}
	
	@Test
	public void testSymlinkCreatesSymlinks() throws IOException {
		String symlink = "symlink-test";
		assertFalse(cacheFS.exists(symlink));
		backedFS.symlink(CACHED_FILE, symlink);
		assertTrue(cacheFS.exists(symlink));
		assertEquals(cacheFS.readlink(symlink), CACHED_FILE);
		assertNoBackupAccess();
	}
	
	@Test
	public void testSymlinkCreatesParents() throws IOException {
		String parent = "a", symlink = parent + "/symlink";
		assertFalse(cacheFS.exists(parent));
		backedFS.symlink(CACHED_FILE, symlink);
		assertTrue(cacheFS.exists(parent));
		assertTrue(cacheFS.exists(symlink));
		assertEquals(cacheFS.readlink(symlink), CACHED_FILE);
		assertNoBackupAccess();
	}
	
	@Test
	public void testChmodChangesMode() throws IOException {
		assertFalse((cacheFS.stat(CACHED_FILE).getMode() & 0777) == 0777);
		backedFS.chmod(CACHED_FILE, 0777);
		assertTrue((cacheFS.stat(CACHED_FILE).getMode() & 0777) == 0777);
		assertNoBackupAccess();
	}
	
	@Test
	public void testChmodAcquiresFile() throws IOException {
		expectRead(UNCACHED_FILE, ()->{
			assertFalse((backupFS.stat(UNCACHED_FILE).getMode() & 0777) == 0777);
			backedFS.chmod(UNCACHED_FILE, 0777);
			assertTrue((cacheFS.stat(UNCACHED_FILE).getMode() & 0777) == 0777);
		});
	}
	
	@Test
	public void testChmodThrowsENOENT() throws IOException {
		expectENOENT(()->{
			backedFS.chmod(NONEXISTENT_FILE, 0777);
		});
	}
	
	@Test
	public void testSetMtimeAcquiresFile() throws IOException {
		expectRead(UNCACHED_FILE, ()->{
			backedFS.setMtime(UNCACHED_FILE, 1234);
		});
	}
	
	@Test
	public void testSetMtimeSetsTimestamp() throws IOException {
		int timestamp = 3000;
		assertFalse(cacheFS.stat(CACHED_FILE).getMtime() == timestamp);
		backedFS.setMtime(CACHED_FILE, timestamp);
		assertTrue(cacheFS.stat(CACHED_FILE).getMtime() == timestamp);
		assertNoBackupAccess();
	}
	
	@Test
	public void testSetMtimeThrowsENOENT() throws IOException {
		expectENOENT(()-> {
			backedFS.setMtime(NONEXISTENT_FILE, 1234);
		});
	}
	
	@Test
	public void testSetCtimeAcquiresFile() throws IOException {
		expectRead(UNCACHED_FILE, ()->{
			backedFS.setCtime(UNCACHED_FILE, 1234);
		});
	}
	
	@Test
	public void testSetCtimeSetsTimestamp() throws IOException {
		int timestamp = 3000;
		assertFalse(cacheFS.stat(CACHED_FILE).getCtime() == timestamp);
		backedFS.setCtime(CACHED_FILE, timestamp);
		assertTrue(cacheFS.stat(CACHED_FILE).getCtime() == timestamp);
		assertNoBackupAccess();
	}
	
	@Test
	public void testSetCtimeThrowsENOENT() throws IOException {
		expectENOENT(()-> {
			backedFS.setCtime(NONEXISTENT_FILE, 1234);
		});
	}
	
	@Test
	public void testSetAtimeAcquiresFile() throws IOException {
		expectRead(UNCACHED_FILE, ()->{
			backedFS.setAtime(UNCACHED_FILE, 1234);
		});
	}

	@Test
	public void testSetAtimeSetsTimestamp() throws IOException {
		int timestamp = 3000;
		assertFalse(cacheFS.stat(CACHED_FILE).getAtime() == timestamp);
		backedFS.setAtime(CACHED_FILE, timestamp);
		assertTrue(cacheFS.stat(CACHED_FILE).getAtime() == timestamp);
		assertNoBackupAccess();
	}
	
	@Test
	public void testSetAtimeThrowsENOENT() throws IOException {
		expectENOENT(()-> {
			backedFS.setAtime(NONEXISTENT_FILE, 1234);
		});
	}
	
	@Test
	public void testWriteCreatesParent() throws IOException {
		assertFalse(cacheFS.exists(NONEXISTENT_DIR));
		backedFS.write(NONEXISTENT_FILE, "test".getBytes());
		assertTrue(cacheFS.exists(NONEXISTENT_DIR));
		assertNoBackupAccess();
	}
	
	@Test
	public void testWriteCreatesFileInCache() throws IOException {
		byte[] data = "mary had a little lamb".getBytes();
		assertFalse(cacheFS.exists(NONEXISTENT_FILE));
		backedFS.write(NONEXISTENT_FILE, data);
		assertTrue(Arrays.equals(data, cacheFS.read(NONEXISTENT_FILE)));
		assertNoBackupAccess();
	}
	
	@Test
	public void testWriteReplacesFileInCache() throws IOException {
		byte[] data = "mary had a little lamb".getBytes();
		assertFalse(Arrays.equals(data, cacheFS.read(CACHED_FILE)));
		backedFS.write(CACHED_FILE, data);
		assertTrue(Arrays.equals(data, cacheFS.read(CACHED_FILE)));
		assertNoBackupAccess();
	}
	
	@Test
	public void testReadUsesCacheCopy() throws IOException {
		assertTrue(Arrays.equals(cacheFS.read(CACHED_FILE), backedFS.read(CACHED_FILE)));
		assertNoBackupAccess();
	}
	
	@Test
	public void testReadCachesFromBackup() throws IOException {
		expectRead(UNCACHED_FILE, ()->{
			assertFalse(cacheFS.exists(UNCACHED_FILE));
			byte[] data = backedFS.read(UNCACHED_FILE);
			assertTrue(cacheFS.exists(UNCACHED_FILE));
			assertTrue(Arrays.equals(data, cacheFS.read(UNCACHED_FILE)));
		});
	}
	
	@Test
	public void testReadThrowsENOENT() throws IOException {
		expectENOENT(()-> {
			backedFS.read(NONEXISTENT_FILE);
		});
	}
	
	@Test
	public void testOpenReturnsCacheFileHandleForCachedFiles() throws IOException {
		File file = backedFS.open(CACHED_FILE, File.O_RDONLY);
		File ref = cacheFS.open(CACHED_FILE, File.O_RDONLY);
		assertEquals(ref.getClass(), file.getClass());
		assertNoBackupAccess();
	}
	
	@Test
	public void testOpenReturnsCacheFileHandleForUncachedFiles() throws IOException {
		expectRead(UNCACHED_FILE, ()->{
			assertFalse(cacheFS.exists(UNCACHED_FILE));
			File file = backedFS.open(UNCACHED_FILE, File.O_RDONLY);
			File ref = cacheFS.open(UNCACHED_FILE, File.O_RDONLY);
			assertEquals(ref.getClass(), file.getClass());
		});
	}
	
	@Test
	public void testOpenThrowsENOENT() throws IOException {
		expectENOENT(()->{
			backedFS.open(NONEXISTENT_FILE, File.O_RDONLY);
		});
	}
	
	@Test
	public void testOpenReadsCachedContents() throws IOException {
		byte[] data = backedFS.open(CACHED_FILE, File.O_RDONLY).read();
		assertTrue(Arrays.equals(cacheFS.read(CACHED_FILE), data));
		assertNoBackupAccess();
	}
	
	@Test
	public void testOpenReadsUncachedContents() throws IOException {
		expectRead(UNCACHED_FILE, ()->{
			byte[] data = backedFS.open(UNCACHED_FILE, File.O_RDONLY).read();
			assertTrue(Arrays.equals(cacheFS.read(UNCACHED_FILE), data));
		});
	}
	
	@Test
	public void testOpenAllowsCreation() throws IOException {
		expectPathStat(NONEXISTENT_FILE, ()->{
			byte[] data = { 1,2,3,4 };
			backedFS.mkdirp(backedFS.dirname(NONEXISTENT_FILE));
			File file = backedFS.open(NONEXISTENT_FILE, File.O_WRONLY|File.O_CREAT|File.O_APPEND);
			file.write(data);
			file.close();
			assertTrue(Arrays.equals(data, cacheFS.read(NONEXISTENT_FILE)));
		});
	}
	
	@Test
	public void testOpenAllowsModificationOfCached() throws IOException {
		byte[] data = { 1,2,3,4 };
		assertFalse(Arrays.equals(data, cacheFS.read(CACHED_FILE)));
		if(!backedFS.exists(backedFS.dirname(CACHED_FILE))) {
			backedFS.mkdirp(backedFS.dirname(CACHED_FILE));
		}
		File file = backedFS.open(CACHED_FILE, File.O_WRONLY|File.O_CREAT|File.O_TRUNC);
		file.write(data);
		file.close();
		assertTrue(Arrays.equals(data, cacheFS.read(CACHED_FILE)));
		assertNoBackupAccess();
	}
	
	@Test
	public void testOpenAllowsModificationOfUncached() throws IOException {
		expectRead(UNCACHED_FILE, ()->{
			byte[] data = { 1,2,3,4 };
			ByteBuffer expected = ByteBuffer.allocate(UNCACHED_DATA.length + data.length);
			expected.put(UNCACHED_DATA);
			expected.put(data);
			
			File file = backedFS.open(UNCACHED_FILE, File.O_WRONLY|File.O_CREAT|File.O_APPEND);
			file.write(data);
			file.close();
			assertTrue(Arrays.equals(expected.array(), cacheFS.read(UNCACHED_FILE)));
		});
	}
	
	@Test
	public void testOpenAllowsOverwriteOfUncached() throws IOException {
		byte[] data = { 1,2,3,4 };
		File file = backedFS.open(UNCACHED_FILE, File.O_WRONLY|File.O_CREAT|File.O_TRUNC);
		file.write(data);
		file.close();
		assertTrue(Arrays.equals(data, cacheFS.read(UNCACHED_FILE)));
		assertNoBackupAccess();
	}
	
	@Test
	public void testTruncateReducesCachedFileSize() throws IOException {
		ByteBuffer expected = ByteBuffer.allocate(CACHED_DATA.length-1);
		expected.put(CACHED_DATA, 0, expected.remaining());
		assertFalse(Arrays.equals(expected.array(), cacheFS.read(CACHED_FILE)));
		backedFS.truncate(CACHED_FILE, expected.capacity());
		assertTrue(Arrays.equals(expected.array(), cacheFS.read(CACHED_FILE)));
		assertNoBackupAccess();
	}
	
	@Test
	public void testTruncateExtendsCachedFileSize() throws IOException {
		ByteBuffer expected = ByteBuffer.allocate(CACHED_DATA.length+1);
		expected.put(CACHED_DATA);
		assertFalse(Arrays.equals(expected.array(), cacheFS.read(CACHED_FILE)));
		backedFS.truncate(CACHED_FILE, expected.capacity());
		assertTrue(Arrays.equals(expected.array(), cacheFS.read(CACHED_FILE)));
		assertNoBackupAccess();
	}
	
	@Test
	public void testTruncateReducesUncachedFileSize() throws IOException {
		expectRead(UNCACHED_FILE, ()->{
			ByteBuffer expected = ByteBuffer.allocate(UNCACHED_DATA.length-1);
			expected.put(UNCACHED_DATA, 0, expected.remaining());
			backedFS.truncate(UNCACHED_FILE, expected.capacity());
			assertTrue(Arrays.equals(expected.array(), cacheFS.read(UNCACHED_FILE)));
		});
	}
	
	@Test
	public void testTruncateExtendsUncachedFileSize() throws IOException {
		expectRead(UNCACHED_FILE, ()->{
			ByteBuffer expected = ByteBuffer.allocate(UNCACHED_DATA.length+1);
			expected.put(UNCACHED_DATA);
			backedFS.truncate(UNCACHED_FILE, expected.capacity());
			assertTrue(Arrays.equals(expected.array(), cacheFS.read(UNCACHED_FILE)));
		});
	}
	
	@Test
	public void testTruncateSkipsReadOnZero() throws IOException {
		expectNoRead(UNCACHED_FILE, ()->{
			backedFS.truncate(UNCACHED_FILE, 0);
			assertEquals(0, cacheFS.stat(UNCACHED_FILE).getSize());
		});
	}
	
	@Test
	public void testScopedFS() throws IOException {
		String scope = "scope";
		String expectedCacheRoot = Paths.get(((RAMFS) backedFS.cacheFS).getRoot(), scope).toString();
		String expectedBackupRoot = Paths.get(((DummyFS) backedFS.backupFS).scope, scope).toString();
		BackedFS scoped = backedFS.scopedFS("scope");
		assertEquals(expectedCacheRoot, ((RAMFS) scoped.cacheFS).getRoot());
		assertEquals(expectedBackupRoot, ((DummyFS) scoped.backupFS).scope);
	}
	
	@Test
	public void testMultipleThreadsTriggerOneReadRequest() throws IOException {
		backupFS.delay = 20;
		
		runThreads(20, ()->{
			backedFS.read(UNCACHED_FILE);
		});
		
		assertEquals(1, backupFS.readPaths.getOrDefault(UNCACHED_FILE, 0).intValue());
	}
	
	@Test @Ignore @Override
	public void testMknodCharacterDevice() throws IOException {
		// can't mknod on a localfs without superuser
	}

	@Test @Ignore @Override
	public void testMknodBlockDevice() throws IOException {
		// can't mknod on a localfs without superuser
	}

	@Test @Ignore @Override
	public void testChown() {
		// can't chown on a localfs without superuser
	}

	@Test @Ignore @Override
	public void testChgrp() {
		// can't chgrp on a localfs without superuser
	}
}
