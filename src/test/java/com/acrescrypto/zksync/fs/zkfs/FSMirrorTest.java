package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;

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
import com.acrescrypto.zksync.utility.Util;

public class FSMirrorTest {
	/* This class is perhaps an exercise in over-testing, with many tests exercising the same
	 * code and same branches... but filesystem operations have a way of being troublesome.
	 * Best to be cautious here.
	 */
	
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
			if(!expected.isDirectory()) {
				assertEquals(expected.getMtime(), stat.getMtime());
			}
			
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
	
	void syncArchiveToTargetTest(String path, PathChangeTest test) throws IOException {
		test.test(path);
		zkfs.commit();
		
		Stat expected = null;
		try {
			expected = zkfs.lstat(path);
		} catch(ENOENTException exc) {};
		
		mirror.syncArchiveToTarget();
		checkPathMatch(zkfs, target, expected, path);
	}
	
	LinkedList<String> makeDummyFs(FS fs) throws IOException {
		LinkedList<String> paths = new LinkedList<>();
		ByteBuffer prng = ByteBuffer.wrap(master.getCrypto().expand(new byte[0], 1024*64, new byte[0], new byte[0]));
		
		for(int i = 0; i < 128; i++) {
			int depth = 1 + (Util.unsignByte(prng.get()) % 4);
			String p = "";
			for(int j = 0; j < depth; j++) {
				p = String.format("%s/%016x", p, prng.getLong());
			}
			
			fs.mkdirp(fs.dirname(p));
			switch(prng.get() % 8) {
				case 1:
					p += "-dir";
					fs.mkdir(p);
					break;
				case 2:
					p += "-fifo";
					fs.mkfifo(p);
					break;
				case 3:
					p += "-bdev";
					fs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
					break;
				case 4:
					p += "-cdev";
					fs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
					break;
				case 5:
					p += "-brokensym";
					fs.symlink(String.format("%08x", prng.getInt()), p);
					break;
				case 6:
					if(!paths.isEmpty()) {
						p += "-validsym";
						String linkTarget = null;
						while(linkTarget == null || fs.lstat(linkTarget).isSymlink()) {
							linkTarget = paths.get(Util.unsignShort(prng.getShort()) % paths.size());
						}
						
						fs.symlink(linkTarget, p);
						break;
					} // if we draw this one first, fall through to file case
				default:
					p += "-file";
					fs.write(p, master.getCrypto().expand(
							master.crypto.symNonce(prng.getInt()),
							Util.unsignShort(prng.getShort()),
							new byte[0],
							new byte[0]));
			}

			paths.add(p);
		}
		
		return paths;
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
	public void testObservedFileChangeForBlockDeviceRefreshesStat() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
			zkfs.chown(p, 1234);
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
	public void testObservedFileChangeForCharacterDeviceRefreshesStat() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
			zkfs.chown(p, 1234);
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
	public void testObservedFileChangeForSymlinkToleratesExactMatch() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.symlink("bar", p);
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
	
	@Test
	public void testObservedFileChangeForUnlinkRemovesFifoFromArchive() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mkfifo(p);
		});
	}
	
	@Test
	public void testObservedFileChangeForUnlinkRemovesCharacterDeviceFromArchive() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForUnlinkRemovesBlockDeviceFromArchive() throws IOException {
		observeFileChangeTest("foo", (p)->{
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
		});
	}
	
	
	
	
	@Test
	public void testSyncArchiveToTargetCreatesFiles() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			zkfs.write(p, "data".getBytes());
		});
	}
	
	@Test
	public void testSyncArchiveToTargetOverwritesFiles() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
			zkfs.write(p, "and now this".getBytes());
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUpdatesFileStat() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
			target.chmod(p, 0123);
			zkfs.write(p, "data".getBytes());
		});
	}
	
	@Test
	public void testSyncArchiveToTargetDeletesFiles() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesDirectoriesWithFiles() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkdir(p);
			zkfs.write(p, "data".getBytes());
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesSpecialFilesWithFiles() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkfifo(p);
			zkfs.write(p, "data".getBytes());
		});
	}

	
	
	@Test
	public void testSyncArchiveToTargetCreatesDirectories() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			zkfs.mkdir(p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetDeletesDirectories() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkdir(p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUpdatesDirectoryStat() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkdir(p);
			target.chmod(p, 1234);
			zkfs.mkdir(p);
		});
	}

	@Test
	public void testSyncArchiveToTargetReplacesFilesWithDirectories() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
			zkfs.mkdir(p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesSpecialFilesWithDirectories() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkfifo(p);
			zkfs.mkdir(p);
		});
	}



	@Test
	public void testSyncArchiveToTargetCreatesFifos() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			zkfs.mkfifo(p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetDeletesFifos() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkfifo(p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUpdatesFifoStat() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkfifo(p);
			target.chmod(p, 0123);
			zkfs.mkfifo(p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesDirectoriesWithFifos() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkdir(p);
			zkfs.mkfifo(p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesFilesWithFifos() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
			zkfs.mkfifo(p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesSpecialFilesWithFifos() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
			zkfs.mkfifo(p);
		});
	}

	
	
	
	@Test
	public void testSyncArchiveToTargetCreatesBlockDevices() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetDeletesBlockDevices() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUpdatesBlockDeviceStat() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
			target.chmod(p, 0123);
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}

	@Test
	public void testSyncArchiveToTargetReplacesDirectoriesWithBlockDevices() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkdir(p);
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesFilesWithBlockDevices() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesSpecialFilesWithBlockDevices() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkfifo(p);
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}

	
	
	@Test
	public void testSyncArchiveToTargetCreatesCharacterDevices() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetDeletesCharacterDevices() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUpdatesCharacterDeviceStat() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
			target.chmod(p, 0123);
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesDirectoriesWithCharacterDevices() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkdir(p);
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesFilesWithCharacterDevices() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesSpecialFilesWithCharacterDevices() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkfifo(p);
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		});
	}
	
	
	
	@Test
	public void testSyncArchiveToTargetCreatesSymlinks() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			zkfs.symlink("bar", p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetDeletesBrokenSymlinks() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.symlink("bar", p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetDeletesUnbrokenSymlinks() throws IOException {
		zkfs.write("bar", "data".getBytes());
		target.write("bar", "data".getBytes());
		zkfs.commit();
		
		syncArchiveToTargetTest("foo", (p)->{
			target.symlink("bar", p);
		});
		
		assertArrayEquals("data".getBytes(), target.read("bar"));
	}
	
	@Test
	public void testSyncArchiveToTargetUpdatesSymlinks() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.symlink("baz", p);
			zkfs.symlink("bar", p);
		});
	}

	@Test
	public void testSyncArchiveToTargetReplacesDirectoriesWithSymlinks() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkdir(p);
			zkfs.symlink("bar", p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesFilesWithSymlinks() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
			zkfs.symlink("bar", p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesSpecialFilesWithSymlinks() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkfifo(p);
			zkfs.symlink("bar", p);
		});
	}
	
	
	
	@Test
	public void testSyncArchiveToTargetUnlinksFiles() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUnlinksDirectories() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkdir(p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUnlinksFifos() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mkfifo(p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUnlinksBlockDevices() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUnlinksCharacterDevices() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUnlinksUnbrokenSymlinks() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.write("bar", "data".getBytes());
			target.symlink("bar", p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUnlinksBrokenSymlinks() throws IOException {
		syncArchiveToTargetTest("foo", (p)->{
			target.symlink("bar", p);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetHandlesTree() throws IOException {
		LinkedList<String> paths = makeDummyFs(zkfs);		
		zkfs.commit();
		HashMap<String,Stat> expectations = new HashMap<>();
		for(String path : paths) {
			expectations.put(path, zkfs.lstat(path));
		}
		
		mirror.syncArchiveToTarget();
		
		for(String path : paths) {
			checkPathMatch(zkfs, target, expectations.get(path), path);
		}
	}

	@Test
	public void testSyncTargetToArchiveHandlesTree() throws IOException {
		LinkedList<String> paths = makeDummyFs(target);
		HashMap<String,Stat> expectations = new HashMap<>();
		for(String path : paths) {
			expectations.put(path, target.lstat(path));
		}
		
		mirror.syncTargetToArchive();
		
		for(String path : paths) {
			checkPathMatch(target, zkfs, expectations.get(path), path);
		}
	}
}
