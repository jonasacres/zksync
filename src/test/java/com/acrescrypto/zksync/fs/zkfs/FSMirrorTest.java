package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.utility.Util;

public class FSMirrorTest {
	/* This class is perhaps an exercise in over-testing, with many tests exercising the same
	 * code and same branches... but filesystem operations have a way of being troublesome.
	 * Best to be cautious here.
	 */
	
	ZKMaster master;
	ZKArchive archive;
	ZKFS zkfs;
	FS target;
	FSMirror mirror;
	
	interface PathChangeTest {
		void test(String path) throws IOException;
	}
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createDefaultArchive();
		zkfs = archive.openBlank();
		target = new LocalFS("/tmp/zksync-test/fsmirrortest");
		target.purge();
		mirror = new FSMirror(zkfs, target);
	}
	
	@After
	public void afterEach() throws IOException {
		mirror.stopWatch();
		target.purge();
		zkfs.close();
		target.close();
		archive.close();
		master.close();
		TestUtils.assertTidy();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	boolean isSuperuser() {
		// leave the door open for a per-filesystem check
		return Util.isSuperuser();
	}
	
	boolean quickCheck(FS src, FS modified, Stat expected, String path) {
		try {
			Stat stat = zkfs.lstat(path);
			boolean check = true;
			check &= stat.getType() == expected.getType();
			if(stat.isSymlink()) {
				check &= src.readlink(path).equals(modified.readlink(path));
			} else if(stat.isRegularFile()) {
				check &= stat.getSize() == expected.getSize();
				check &= stat.getMode() == expected.getMode();
			} else if(stat.isDevice()) {
				check &= stat.getDevMajor() == expected.getDevMajor();
				check &= stat.getDevMinor() == expected.getDevMinor();
			}
			return check;
		} catch(IOException exc) { return false; }
	}
	
	void checkPathMatch(FS src, FS modified, Stat expected, String path) throws IOException {
		if(expected == null) {
			assertFalse(modified.exists(path));
			return;
		}
		
		Stat current = src.lstat(path);
		current.setAtime(expected.getAtime()); // let's take a pass on this one; we're on LocalFS
		if(!expected.equals(current)) {
			Util.hexdump("current", current.serialize());
			Util.hexdump("expected", expected.serialize());
			Util.hexdump("expected", CryptoSupport.xor(current.serialize(), expected.serialize()));
		}
		assertEquals(expected, current);
		
		Stat stat = modified.lstat(path);
		assertEquals(expected.getType(), stat.getType());
		
		final int conversion = 1000*1000*1000;
		final int timeTolerance = 20; // timestamps are in nanoseconds. allow some slop.
		
		if(!expected.isSymlink()) {
			assertEquals(expected.getAtime()/conversion, stat.getAtime()/conversion, timeTolerance);
			assertEquals(expected.getCtime()/conversion, stat.getCtime()/conversion, timeTolerance);
			assertEquals(expected.getMtime()/conversion, stat.getMtime()/conversion, timeTolerance);
			
			if(Util.isSuperuser()) {
				assertEquals(expected.getUid(), stat.getUid());
				assertEquals(expected.getUser(), stat.getUser());
				assertEquals(expected.getGid(), stat.getGid());
				assertEquals(expected.getGroup(), stat.getGroup());
			}
			
			assertEquals(expected.getMode(), stat.getMode(), timeTolerance);
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
	
	void watcherTest(String path, PathChangeTest test) throws IOException {
		mirror.syncTargetToArchive();
		mirror.startWatch();
		test.test(path);
		
		if(target.exists(path)) {
			assertTrue(Util.waitUntil(100, ()->zkfs.exists(path)));
			Stat existing = target.stat(path);
			assertTrue(Util.waitUntil(100, ()->quickCheck(target, zkfs, existing, path)));
			checkPathMatch(target, zkfs, existing, path);
		} else {
			assertTrue(Util.waitUntil(100, ()->!zkfs.exists(path)));
		}
	}
	
	LinkedList<String> makeDummyFs(FS fs) throws IOException {
		LinkedList<String> paths = new LinkedList<>();
		ByteBuffer prng = ByteBuffer.wrap(master.getCrypto().expand(new byte[0], 1024*64, new byte[0], new byte[0]));
		
		for(int i = 0; i < 128; i++) {
			int depth = 1 + (Util.unsignByte(prng.get()) % 4);
			String p = "";
			for(int j = 0; j < depth; j++) {
				p = String.format("%s/%08x", p, prng.getLong());
			}
			
			fs.mkdirp(fs.dirname(p));
			switch(prng.get() % 8) {
				case 3:
					if(isSuperuser()) {
						p += "-bdev";
						fs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
						break;
					}
				case 1:
					p += "-dir";
					fs.mkdir(p);
					break;
				case 4:
					if(isSuperuser()) {
						p += "-cdev";
						fs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
						break;
					}
				case 2:
					p += "-fifo";
					fs.mkfifo(p);
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
			zkfs.chmod(p, 0714);
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
			zkfs.chmod(p, 0714);
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
			zkfs.chmod(p, 0714);
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
		if(!isSuperuser()) return;
		observeFileChangeTest("foo", (p)->{
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
			target.mkfifo(p);
		});
	}
	
	
	
	@Test
	public void testObservedFileChangeForBlockDeviceCreatesDevice() throws IOException {
		if(!isSuperuser()) return;
		observeFileChangeTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
		});
	}

	@Test
	public void testObservedFileChangeForBlockDeviceOverwritesExisting() throws IOException {
		if(!isSuperuser()) return;
		observeFileChangeTest("foo", (p)->{
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 4, 3);
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForBlockDeviceRefreshesStat() throws IOException {
		if(!isSuperuser()) return;
		observeFileChangeTest("foo", (p)->{
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
			zkfs.chown(p, 1234);
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForBlockDeviceCreatesParentDirectoriesInArchive() throws IOException {
		if(!isSuperuser()) return;
		observeFileChangeTest("/path/to/foo", (p)->{
			target.mkdirp(target.dirname(p));
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForBlockDeviceReplacesDirectories() throws IOException {
		if(!isSuperuser()) return;
		observeFileChangeTest("foo", (p)->{
			zkfs.mkdirp(p);
			target.mkdirp(target.dirname(p));
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForBlockDeviceReplacesExistingFiles() throws IOException {
		if(!isSuperuser()) return;
		observeFileChangeTest("foo", (p)->{
			zkfs.write(p, "data".getBytes());
			target.mkdirp(target.dirname(p));
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 2);
		});
	}



	@Test
	public void testObservedFileChangeForCharacterDeviceCreatesDevice() throws IOException {
		if(!isSuperuser()) return;
		observeFileChangeTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
		});
	}

	@Test
	public void testObservedFileChangeForCharacterDeviceOverwritesExisting() throws IOException {
		if(!isSuperuser()) return;
		observeFileChangeTest("foo", (p)->{
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 4, 3);
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForCharacterDeviceRefreshesStat() throws IOException {
		if(!isSuperuser()) return;
		observeFileChangeTest("foo", (p)->{
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
			zkfs.chown(p, 1234);
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForCharacterDeviceCreatesParentDirectoriesInArchive() throws IOException {
		if(!isSuperuser()) return;
		observeFileChangeTest("/path/to/foo", (p)->{
			target.mkdirp(target.dirname(p));
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForCharacterDeviceReplacesDirectories() throws IOException {
		if(!isSuperuser()) return;
		observeFileChangeTest("foo", (p)->{
			zkfs.mkdirp(p);
			target.mkdirp(target.dirname(p));
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 2);
		});
	}
	
	@Test
	public void testObservedFileChangeForCharacterDeviceReplacesExistingFiles() throws IOException {
		if(!isSuperuser()) return;
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
			target.chmod(p, 0714);
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
			target.chmod(p, 0714);
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
		if(!isSuperuser()) return;
		syncArchiveToTargetTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
			zkfs.mkfifo(p);
		});
	}

	
	
	
	@Test
	public void testSyncArchiveToTargetCreatesBlockDevices() throws IOException {
		if(!isSuperuser()) return;
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetDeletesBlockDevices() throws IOException {
		if(!isSuperuser()) return;
		syncArchiveToTargetTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUpdatesBlockDeviceStat() throws IOException {
		if(!isSuperuser()) return;
		syncArchiveToTargetTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
			target.chmod(p, 0714);
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}

	@Test
	public void testSyncArchiveToTargetReplacesDirectoriesWithBlockDevices() throws IOException {
		if(!isSuperuser()) return;
		syncArchiveToTargetTest("foo", (p)->{
			target.mkdir(p);
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesFilesWithBlockDevices() throws IOException {
		if(!isSuperuser()) return;
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesSpecialFilesWithBlockDevices() throws IOException {
		if(!isSuperuser()) return;
		syncArchiveToTargetTest("foo", (p)->{
			target.mkfifo(p);
			zkfs.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}

	
	
	@Test
	public void testSyncArchiveToTargetCreatesCharacterDevices() throws IOException {
		if(!isSuperuser()) return;
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetDeletesCharacterDevices() throws IOException {
		if(!isSuperuser()) return;
		syncArchiveToTargetTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUpdatesCharacterDeviceStat() throws IOException {
		if(!isSuperuser()) return;
		syncArchiveToTargetTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
			target.chmod(p, 0714);
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesDirectoriesWithCharacterDevices() throws IOException {
		if(!isSuperuser()) return;
		syncArchiveToTargetTest("foo", (p)->{
			target.mkdir(p);
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesFilesWithCharacterDevices() throws IOException {
		if(!isSuperuser()) return;
		syncArchiveToTargetTest("foo", (p)->{
			target.write(p, "data".getBytes());
			zkfs.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetReplacesSpecialFilesWithCharacterDevices() throws IOException {
		if(!isSuperuser()) return;
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
		if(!isSuperuser()) return;
		syncArchiveToTargetTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testSyncArchiveToTargetUnlinksCharacterDevices() throws IOException {
		if(!isSuperuser()) return;
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
	public void testSyncArchiveToTargetDoesNotTriggerWatch() throws IOException {
		mirror.startWatch();
		syncArchiveToTargetTest("foo", (p)->{
			zkfs.write(p, "foo".getBytes());
		});
		
		assertFalse(zkfs.dirty);
	}

	@Test
	public void testSyncArchiveToTargetResumesWatchWhenDone() throws IOException {
		mirror.startWatch();
		syncArchiveToTargetTest("foo", (p)->{
			zkfs.write(p, "foo".getBytes());
		});
		
		target.write("foo", "bar".getBytes());
		assertTrue(Util.waitUntil(100, ()->zkfs.dirty));
	}
	
	@Test
	public void testWatchMonitorsFileCreations() throws IOException {
		watcherTest("foo", (p)->{
			target.write(p, "data".getBytes());
		});
	}
	
	@Test
	public void testWatchMonitorsEmptyFileCreations() throws IOException {
		watcherTest("foo", (p)->{
			target.write(p, new byte[0]);
		});
	}
	
	@Test
	public void testWatchMonitorsFileCreationsInSubdirectory() throws IOException {
		target.mkdirp("a/b/c");
		watcherTest("a/b/c/foo", (p)->{
			target.write(p, "data".getBytes());
		});
	}
	
	@Test
	public void testWatchMonitorsFileExtensions() throws IOException {
		target.write("foo", "data".getBytes());
		
		watcherTest("foo", (p)->{
			target.write(p, "bar".getBytes());
		});
	}
	
	@Test
	public void testWatchMonitorsFileTruncations() throws IOException {
		target.write("foo", "here's a really long string that we can truncate".getBytes());
		
		watcherTest("foo", (p)->{
			target.truncate(p, 8);
		});
	}
	
	@Test
	public void testWatchMonitorsFileDeletions() throws IOException {
		target.write("foo", "data".getBytes());
		
		watcherTest("foo", (p)->{
			target.unlink(p);
		});
	}
	
	@Test
	public void testWatchMonitorsFileModeChanges() throws IOException {
		target.write("foo", "data".getBytes());
		
		watcherTest("foo", (p)->{
			target.chmod(p, 0600);
		});
	}
	
	@Test
	public void testWatchMonitorsFileUserChanges() throws IOException {
		if(!isSuperuser()) return;
		target.write("foo", "data".getBytes());
		
		watcherTest("foo", (p)->{
			target.chown(p, 1234);
		});
	}
	
	@Test
	public void testWatchMonitorsFileGroupChanges() throws IOException {
		if(!isSuperuser()) return;
		target.write("foo", "data".getBytes());
		
		watcherTest("foo", (p)->{
			target.chgrp(p, 1234);
		});
	}

	/* This test does not pass.
	** It is left here as a reminder/demonstration that mtime changes absent other changes are NOT detected.
	** (at least, so it seems on JDK 1.8.0_171 / Linux / Ubuntu 18.04 / btrfs)
	*/
	//	@Test
	//	public void testWatchMonitorsFileTimestampChanges() throws IOException {
	//		target.write("foo", "data".getBytes());
	//		mirror.syncTargetToArchive();
	//		
	//		watcherTest("foo", (p)->{
	//			target.setMtime(p, 1000*1000*1000*1000);
	//		});
	//	}
	
	@Test
	public void testWatchMonitorsDirectoryCreation() throws IOException {
		watcherTest("foo", (p)->{
			target.mkdir(p);
		});
	}
	
	@Test
	public void testWatchMonitorsDirectoryRemoval() throws IOException {
		target.mkdir("foo");
		watcherTest("foo", (p)->{
			target.rmdir(p);
		});
	}
	
	@Test
	public void testWatchMonitorsSymlinkCreation() throws IOException {
		watcherTest("foo", (p)->{
			target.symlink("fake", p);
		});
	}
	
	@Test
	public void testWatchMonitorsSymlinkRemoval() throws IOException {
		target.symlink("fake", "foo");
		watcherTest("foo", (p)->{
			target.unlink(p);
		});
	}
	
	@Test
	public void testWatchMonitorsFifoCreation() throws IOException {
		watcherTest("foo", (p)->{
			target.mkfifo(p);
		});
	}
	
	@Test
	public void testWatchMonitorsFifoRemoval() throws IOException {
		target.mkfifo("foo");
		watcherTest("foo", (p)->{
			target.unlink(p);
		});
	}
	
	@Test
	public void testWatchMonitorsCharacterDeviceCreation() throws IOException {
		if(!isSuperuser()) return;
		watcherTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testWatchMonitorsCharacterDeviceRemoval() throws IOException {
		if(!isSuperuser()) return;
		target.mknod("foo", Stat.TYPE_CHARACTER_DEVICE, 1, 3);
		watcherTest("foo", (p)->{
			target.unlink(p);
		});
	}

	@Test
	public void testWatchMonitorsBlockDeviceCreation() throws IOException {
		if(!isSuperuser()) return;
		watcherTest("foo", (p)->{
			target.mknod(p, Stat.TYPE_BLOCK_DEVICE, 1, 3);
		});
	}
	
	@Test
	public void testWatchMonitorsBlockDeviceRemoval() throws IOException {
		if(!isSuperuser()) return;
		target.mknod("foo", Stat.TYPE_BLOCK_DEVICE, 1, 3);
		watcherTest("foo", (p)->{
			target.unlink(p);
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

	@Test
	public void testWatchHandlesTree() throws IOException {
		mirror.startWatch();
		LinkedList<String> paths = makeDummyFs(target);
		HashMap<String,Stat> expectations = new HashMap<>();
		for(String path : paths) {
			expectations.put(path, target.lstat(path));
		}
		
		for(String path : paths) {
			Stat expected = expectations.get(path);
			assertTrue(Util.waitUntil(100, ()->quickCheck(target, zkfs, expected, path)));
			checkPathMatch(target, zkfs, expected, path);
		}
	}
	
	@Test
	public void testAbsoluteSymlinksCanCopy() throws IOException {
		String linkTarget = "/dev/null", link = "test-symlink";
		zkfs.symlink(linkTarget, link);
		mirror.syncArchiveToTarget();
		assertEquals(linkTarget, target.readlink_unsafe(link));
	}
	
	@Test
	public void testDeleteDoesNotCauseDeafening() throws IOException {
		target.write("somefile", "just here to make the data directory".getBytes());
		mirror.syncTargetToArchive();
		mirror.startWatch();
		
		String path = "testdir/dir2";
		target.mkdirp(path);
		assertTrue(Util.waitUntil(100, ()->zkfs.exists(path)));
		
		target.rmrf(path);
		assertTrue(Util.waitUntil(100, ()->!zkfs.exists(path)));
		
		String path2 = "testfile";
		target.write(path2, "somebytes".getBytes());
		assertTrue(Util.waitUntil(100, ()->zkfs.exists(path2)));
	}
}
