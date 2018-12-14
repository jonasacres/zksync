package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;

public class FSMirrorTest {
	ZKMaster master;
	ZKArchive archive;
	ZKFS zkfs;
	RAMFS target;
	FSMirror mirror;
	
	interface PathChangeTest {
		void test(String path) throws IOException;
	}
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createDefaultArchive();
		zkfs = archive.openBlank();
		target = new RAMFS();
		mirror = new FSMirror(zkfs, target);
	}
	
	@After
	public void afterEach() throws IOException {
		zkfs.close();
		target.close();
		archive.close();
		master.close();
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	void checkPathMatch(FS src, FS modified, Stat expected, String path) throws IOException {
		if(expected == null) {
			assertFalse(modified.exists(path));
			return;
		}
		
		Stat current = src.lstat(path);
		assertEquals(expected, current);
		
		Stat stat = modified.lstat(path);
		assertEquals(expected.getType(), stat.getType());
		
		if(!expected.isSymlink()) {
			assertEquals(expected.getAtime(), stat.getAtime());
			assertEquals(expected.getCtime(), stat.getCtime());
			assertEquals(expected.getMtime(), stat.getMtime());
			
			assertEquals(expected.getUid(), stat.getUid());
			assertEquals(expected.getUser(), stat.getUser());
			assertEquals(expected.getGid(), stat.getGid());
			assertEquals(expected.getGroup(), stat.getGroup());
			
			assertEquals(expected.getMode(), stat.getMode());
		}
		
		if(expected.isSymlink()) {
			assertEquals(src.readlink(path), modified.readlink(path));
		} else if(expected.isDevice()) {
			assertEquals(expected.getDevMajor(), stat.getDevMajor());
			assertEquals(expected.getDevMinor(), stat.getDevMinor());
		} else if(expected.isRegularFile()) {
			assertArrayEquals(src.read(path), modified.read(path));
		}
	}
	
	void observeFileChangeTest(String path, PathChangeTest test) throws IOException {
		test.test(path);
		
		Stat expected = null;
		try {
			expected = target.lstat(path);
		} catch(ENOENTException exc) {};
		
		mirror.observedTargetPathChange(path);
		checkPathMatch(target, zkfs, expected, path);
	}
	
	@Test
	public void testObservedFileChangeForRegularFileCreatesFile() throws IOException {
		observeFileChangeTest("foo", (p)->target.write(p, "bar".getBytes()));
	}

	@Test
	public void testObservedFileChangeForRegularFileOverwritesExisting() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.write(p, "baz".getBytes());
			zkfs.chmod(p, 0123);
			target.write(p, "bar".getBytes());
		});
	}

	@Test
	public void testObservedFileChangeForRegularFileCreatesParentDirectoriesInArchive() throws IOException {
		observeFileChangeTest("/path/to/foo", (p)->{
			target.write(p, "bar".getBytes());
		});
	}
	
	@Test
	public void testObservedFileChangeForRegularFileReplacesDirectories() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.write(p + "/contents", "".getBytes());
			target.write(p, "bar".getBytes());
		});
	}

	@Test
	public void testObservedFileChangeForRegularFileReplacesNonFiles() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mkfifo(p);
			target.write(p, "bar".getBytes());
		});
	}
	
	
	
	
	@Test
	public void testObservedFileChangeForDirectoryTriggersMkdir() throws IOException{
		observeFileChangeTest("foo", (p)->{
			target.mkdir(p);
		});
	}
	
	@Test
	public void testObservedFileChangeForDirectoryRefreshesExistingStat() throws IOException{
		observeFileChangeTest("foo", (p)->{
			zkfs.mkdir(p);
			zkfs.chmod(p, 0123);
			target.mkdir(p);
		});
	}

	@Test
	public void testObservedFileChangeForDirectoryCreatesParentDirectoriesInArchive() throws IOException {
		observeFileChangeTest("/path/to/foo", (p)->{
			target.mkdirp(p);
		});
	}
	
	@Test
	public void testObservedFileChangeForDirectoryReplacesExistingFile() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.write(p, "".getBytes());
			target.mkdir(p);
		});
	}
	
	
	
	@Test
	public void testObservedFileChangeForFifoCreatesFifo() throws IOException {
		observeFileChangeTest("foo", (p)->{
			target.mkfifo(p);
		});
	}
	
	@Test
	public void testObservedFileChangeForFifoRefreshesExistingStat() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mkfifo(p);
			zkfs.chmod(p, 0123);
			target.mkfifo(p);
		});
	}
	
	@Test
	public void testObservedFileChangeForFifoCreatesParentDirectoriesInArchive() throws IOException {
		observeFileChangeTest("/path/to/foo", (p)->{
			target.mkdirp(target.dirname(p));
			target.mkfifo(p);
		});
	}
	
	@Test
	public void testObservedFileChangeForFifoReplacesDirectories() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mkdir(p);
			target.mkfifo(p);
		});
	}

	@Test
	public void testObservedFileChangeForFifoReplacesNonfiles() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
			target.mkfifo(p);
		});
	}
	
	
	
	@Test
	public void testObservedFileChangeForBlockDeviceCreatesDevice() throws IOException {
		observeFileChangeTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
		});
	}

	@Test
	public void testObservedFileChangeForBlockDeviceOverwritesExisting() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 4, 3);
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForBlockDeviceCreatesParentDirectoriesInArchive() throws IOException {
		observeFileChangeTest("/path/to/foo", (p)->{
			target.mkdirp(target.dirname(p));
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForBlockDeviceReplacesDirectories() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mkdirp(p);
			target.mkdirp(target.dirname(p));
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForBlockDeviceReplacesExistingFiles() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.write(p, "data".getBytes());
			target.mkdirp(target.dirname(p));
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
		});
	}



	@Test
	public void testObservedFileChangeForCharacterDeviceCreatesDevice() throws IOException {
		observeFileChangeTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
		});
	}

	@Test
	public void testObservedFileChangeForCharacterDeviceOverwritesExisting() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 4, 3);
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForCharacterDeviceCreatesParentDirectoriesInArchive() throws IOException {
		observeFileChangeTest("/path/to/foo", (p)->{
			target.mkdirp(target.dirname(p));
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForCharacterDeviceReplacesDirectories() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mkdirp(p);
			target.mkdirp(target.dirname(p));
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForCharacterDeviceReplacesExistingFiles() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.write(p, "data".getBytes());
			target.mkdirp(target.dirname(p));
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
		});
	}



	@Test
	public void testObservedFileChangeForSymlinkCreatesSymlink() throws IOException {
		observeFileChangeTest("foo", (p)->{
			target.symlink("bar", p);
		});
	}

	@Test
	public void testObservedFileChangeForSymlinkOverwritesExisting() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.symlink("baz", p);
			target.symlink("bar", p);
		});
	}

	@Test
	public void testObservedFileChangeForSymlinkCreatesParentDirectoriesInArchive() throws IOException {
		observeFileChangeTest("/path/to/foo", (p)->{
			target.mkdirp(target.dirname(p));
			target.symlink("bar", p);
		});
	}
	
	@Test
	public void testObservedFileChangeForSymlinkReplacesDirectories() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mkdirp(p);
			target.symlink("bar", p);
		});
	}

	@Test
	public void testObservedFileChangeForSymlinkReplacesNonFiles() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.write(p, "bytes".getBytes());
			target.symlink("bar", p);
		});
	}
	
	
	
	@Test
	public void testObservedFileChangeForUnlinkToleratesNonexistent() throws IOException {
		observeFileChangeTest("foo", (p)->{});
	}
	
	@Test
	public void testObservedFileChangeForUnlinkRemovesRegularFileFromArchive() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.write(p, "bytes".getBytes());
		});
	}
	
	@Test
	public void testObservedFileChangeForUnlinkRemovesDirectoryFromArchive() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.write(p + "/file", "some data".getBytes());
		});
	}
	
	@Test
	public void testObservedFileChangeForUnlinkRemovesBrokenSymlinkFromArchive() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.symlink("bar", p);
		});
	}
	
	@Test
	public void testObservedFileChangeForUnlinkRemovesUnbrokenSymlinkFromArchive() throws IOException {
		zkfs.write("bar", "bytes".getBytes());
		
		observeFileChangeTest("foo", (p)->{
			zkfs.symlink("bar", p);
		});
		
		assertArrayEquals("bytes".getBytes(), zkfs.read("bar"));
	}
}
