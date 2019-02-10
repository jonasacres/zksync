package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Security;
import java.util.Arrays;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.ENOTEMPTYException;
import com.acrescrypto.zksync.fs.FSTestBase;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.ZKFS.ZKFSDirtyMonitor;
import com.acrescrypto.zksync.utility.Util;

public class ZKFSTest extends FSTestBase {
	int oldDefaultTimeCost, oldDefaultParallelism, oldDefaultMemoryCost;
	ZKFS zkscratch;
	ZKMaster master;
	
	@Before
	public void beforeEach() throws IOException {
		cheapenArgon2Costs();
		master = ZKMaster.openBlankTestVolume();
		scratch = zkscratch = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").openBlank();
		prepareExamples();
	}
	
	@After
	public void afterEach() throws IOException {
		master.close();
		zkscratch.close();
		zkscratch.archive.close();
		restoreArgon2Costs();
	}
	
	@BeforeClass
	public static void beforeClass() {
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@AfterClass
	public static void afterClass() throws IOException {
		TestUtils.assertTidy();
	}
	
	@Override @Test
	public void testSymlinkUnsafe() throws IOException {
		scratch.write("symlink-target", "over here".getBytes());
		scratch.symlink_unsafe("symlink-target", "symlink-link");
		byte[] a = scratch.read("symlink-target");
		byte[] b = scratch.read("symlink-link");
		assertTrue(Arrays.equals(a, b));
	}	
	
	@Test
	public void testMultipageWrite() throws IOException {
		byte[] text = new byte[10*zkscratch.getArchive().config.pageSize];
		for(int i = 0; i < text.length; i++) text[i] = (byte) (i % 256);
		scratch.write("multipage-write", text);
		assertTrue(Arrays.equals(text, scratch.read("multipage-write")));
	}
	
	@Test
	public void testMultipageModify() throws IOException {
		byte[] text = new byte[10*zkscratch.getArchive().config.pageSize];
		ByteBuffer buf = ByteBuffer.wrap(text);
		for(int i = 0; i < text.length; i++) text[i] = (byte) (i % 256);
		scratch.write("multipage-modify", text);
		ZKFile file = zkscratch.open("multipage-modify", ZKFile.O_RDWR);
		assertTrue(Arrays.equals(text, scratch.read("multipage-modify")));
		
		int modOffset = 1000;
		byte[] mod = "The doubts gnaw us,\nI am not happy about these doubts,\nThis vile load in the chest breaks love.\nAnd while we're sitting and suffering,\nwhining at the closed gates,\nwe are already getting the hit in the eye or brow.".getBytes();
		buf.position(modOffset);
		buf.put(mod);
		assertFalse(Arrays.equals(text, scratch.read("multipage-modify")));
		
		file.seek(modOffset, File.SEEK_SET);
		file.write(text, modOffset, mod.length);
		file.close();
		assertTrue(Arrays.equals(text, scratch.read("multipage-modify")));
	}
	
	@Test
	public void testMultipageTruncateSmaller() throws IOException {
		byte[] text = new byte[10*zkscratch.getArchive().config.pageSize];
		for(int i = 0; i < text.length; i++) text[i] = (byte) (i % 256);
		scratch.write("multipage-truncate", text);
		
		byte[] truncated = new byte[150000];
		for(int i = 0; i < truncated.length; i++) truncated[i] = (byte) (i % 256);
		scratch.truncate("multipage-truncate", truncated.length);
		assertTrue(Arrays.equals(truncated, scratch.read("multipage-truncate")));
	}
	
	@Test
	public void testFilesCreateWithNlink1() throws IOException {
		scratch.write("files-create-with-nlink-1", "yadda yadda".getBytes());
		ZKFile file = zkscratch.open("files-create-with-nlink-1", File.O_RDONLY);
		int nlink = file.inode.getNlink();
		file.close();
		assertEquals(1, nlink);
	}

	@Test
	public void testHardlinksIncreaseNlinkBy1() throws IOException {
		scratch.write("hardlinks-increase-nlink-by-1", "yadda yadda".getBytes());
		scratch.link("hardlinks-increase-nlink-by-1", "hardlinks-increase-nlink-by-1-link");
		ZKFile file = zkscratch.open("hardlinks-increase-nlink-by-1", File.O_RDONLY);
		int nlink = file.inode.getNlink();
		file.close();
		assertEquals(2, nlink);
	}

	@Test
	public void testUnlinkReducesNlinkBy1() throws IOException {
		scratch.write("hardlinks-increase-nlink-by-1", "yadda yadda".getBytes());
		scratch.link("hardlinks-increase-nlink-by-1", "hardlinks-increase-nlink-by-1-link");
		scratch.unlink("hardlinks-increase-nlink-by-1");
		ZKFile file = zkscratch.open("hardlinks-increase-nlink-by-1-link", File.O_RDONLY);
		int nlink = file.inode.getNlink();
		file.close();
		assertEquals(1, nlink);
	}

	@Test
	public void testSymlinksDontAffectNlink() throws IOException {
		scratch.write("hardlinks-increase-nlink-by-1", "yadda yadda".getBytes());
		scratch.symlink("hardlinks-increase-nlink-by-1", "hardlinks-increase-nlink-by-1-link");
		ZKFile file = zkscratch.open("hardlinks-increase-nlink-by-1", File.O_RDONLY);
		int nlink = file.inode.getNlink();
		file.close();
		assertEquals(1, nlink);
	}
	
	@Test
	public void testPageBoundaryWrites() throws IOException {
		for(int pageCount = 1; pageCount <= 3; pageCount++) {
			for(int mod = -1; mod <= 1; mod++) {
				byte[] buf = generateFileData("page-boundary-test", pageCount*zkscratch.getArchive().config.pageSize + mod);
				for(int i = 0; i < buf.length; i++) buf[i] = (byte) (i & 0xff);
				zkscratch.write("page-boundary-test", buf);
				
				byte[] contents = zkscratch.read("page-boundary-test");
				assertTrue(Arrays.equals(buf, contents));
			}
		}
	}
	
	@Test
	public void testPageBoundaryExtensions() throws IOException {
		int pageCount = 5;
		byte[] testData = generateFileData("page-boundary", pageCount*zkscratch.getArchive().config.pageSize);
		ByteBuffer testBuf = ByteBuffer.wrap(testData);
		
		try(ZKFile testFile = zkscratch.open("page-boundary-extension-test", File.O_RDWR|File.O_CREAT)) {
			for(int page = 0; page < pageCount; page++) {
				int[] writeLengths = { 1, zkscratch.getArchive().config.pageSize-2, 1 };
				for(int writeLength: writeLengths) {
					byte[] writeData = new byte[writeLength];
					testBuf.get(writeData);
					testFile.write(writeData, 0, writeLength);
					testFile.flush();
					
					assertEquals(testFile.pos(), testFile.getInode().getStat().getSize());
					assertEquals(testFile.pos(), testBuf.position());
					testFile.seek(0, File.SEEK_SET);
					byte[] readData = testFile.read(testBuf.position());
					for(int i = 0; i < testBuf.position(); i++) {
						if(readData[i] != testData[i]) throw new IllegalStateException("read data doesn't match written data at index " + i);
					}
					assertEquals(testFile.pos(), testBuf.position());
				}
			}
		}
	}
	
	@Test
	public void testPageBoundaryTruncations() throws IOException {
		int pageCount = 5;
		int pageSize = zkscratch.getArchive().config.pageSize;
		try(ZKFile testFile = zkscratch.open("page-boundary-extension-test", File.O_RDWR|File.O_CREAT)) {
			byte[] testData = generateFileData("page-boundary", pageCount*pageSize);
			
			testFile.write(testData);
			for(int page = pageCount - 1; page >= 0; page--) {
				for(int i = 1; i >= -1; i--) {
					int size = page*pageSize + i;
					if(size < 0) continue;
					
					byte[] expectedData = new byte[size];
					System.arraycopy(testData, 0, expectedData, 0, size);
					testFile.truncate(size);
					assertEquals(size, testFile.getInode().getStat().getSize());
					testFile.seek(0, File.SEEK_SET);
					assertArrayEquals(expectedData, testFile.read());
				}
			}
		}
	}
	
	@Test
	public void testBasicArchiveWrite() throws IOException {
		byte[] content = "there is a house down in new orleans they call the rising sun".getBytes();
		zkscratch.write("basic-archive-test", content);
		assertTrue(Arrays.equals(content, zkscratch.read("basic-archive-test")));
		RevisionTag rev = zkscratch.commit();
		
		ZKFS readFs = rev.readOnlyFS();
		assertTrue(Arrays.equals(content, readFs.read("basic-archive-test")));
	}
	
	// ---- cut here ----
	// TODO Someday: (refactor) everything after this should probably go into its own test class (revision, inode table) 
	
	@Test
	public void testSuccessiveRevisions() throws IOException {
		int numRevisions = 8;
		RevisionTag[] revisions = new RevisionTag[numRevisions+1];
		revisions[0] = RevisionTag.blank(zkscratch.archive.config);
		
		for(int i = 0; i < numRevisions; i++) {
			try(ZKFS revFs = revisions[i].getFS()) {
				revFs.write("successive-revisions", ("Version " + i).getBytes());
				revisions[i+1] = revFs.commit();
			}
		}
		
		for(int i = 1; i <= numRevisions; i++) {
			try(ZKFS revFs = revisions[i].getFS()) {
				byte[] text = ("Version " + (i-1)).getBytes();
				assertTrue(Arrays.equals(text, revFs.read("successive-revisions")));
				assertEquals(1, revFs.getRevisionInfo().getNumParents());
				if(i > 1) {
					assertEquals(revisions[i-1], revFs.getRevisionInfo().getParents().get(0));
				}
			}
		}
	}
	
	@Test
	public void testIntensiveRevisions() throws IOException {
		int numRevisions = 31;
		RevisionTag[] revisions = new RevisionTag[numRevisions];
		
		for(int i = 0; i < numRevisions; i++) {
			RevisionTag parent = null;
			if(i == 0) parent = RevisionTag.blank(zkscratch.archive.config);
			else parent = revisions[(i-1)/2]; 
			try(ZKFS revFs = parent.getFS()) {
				revFs.write("intensive-revisions", ("Version " + i).getBytes());
				revisions[i] = revFs.commit();
			}
		}

		for(int i = 0; i < numRevisions; i++) {
			try(ZKFS revFs = revisions[i].getFS()) {
				byte[] text = ("Version " + i).getBytes();
				assertTrue(Arrays.equals(text, revFs.read("intensive-revisions")));
				assertEquals(1, revFs.getRevisionInfo().getNumParents());
				if(i > 0) {
					assertEquals(revisions[(i-1)/2], revFs.getRevisionInfo().getParents().get(0));
				}
			}
		}
	}
	
	@Test
	public void testSinglePageInodeTable() throws IOException {
		RevisionTag rev = zkscratch.commit();
		assertTrue(zkscratch.inodeTable.getStat().getSize() >= zkscratch.archive.crypto.hashLength());
		assertTrue(zkscratch.inodeTable.getStat().getSize() <= zkscratch.archive.config.pageSize);
		assertEquals(1, zkscratch.inodeTable.inode.refTag.numPages);
		try(ZKFS revFs = rev.getFS()) {
			assertTrue(Arrays.equals(revFs.inodeTable.tree.getRefTag().getBytes(), zkscratch.inodeTable.tree.getRefTag().getBytes()));
			assertTrue(revFs.inodeTable.tree.maxNumPages <= revFs.inodeTable.tree.tagsPerChunk());
			assertEquals(zkscratch.inodeTable.getStat().getSize(), revFs.inodeTable.getStat().getSize());
		}
	}
	
	@Test
	public void testMultiPageInodeTable() throws IOException {
		for(int i = 0; i < 1000; i++) {
			String path = String.format("multipage-inode-table-padding-%04d", i);
			zkscratch.write(path, "foo".getBytes());
		}
		
		RevisionTag rev = zkscratch.commit();
		assertTrue(zkscratch.inodeTable.getStat().getSize() > zkscratch.archive.config.pageSize);
		ZKFS revFs = rev.readOnlyFS();
		
		assertTrue(Arrays.equals(revFs.inodeTable.tree.getRefTag().getBytes(), zkscratch.inodeTable.tree.getRefTag().getBytes()));
		assertEquals(zkscratch.inodeTable.tree.maxNumPages, revFs.inodeTable.tree.maxNumPages);
		assertEquals(zkscratch.inodeTable.getStat().getSize(), revFs.inodeTable.getStat().getSize());
	}
	
	@Test
	public void testDistinctFileWrites() throws IOException {
		byte[][] texts = new byte[10][];
		for(int i = 0; i < texts.length; i++) {
			texts[i] = ("data" + i).getBytes();
			zkscratch.write("file" + i, texts[i]);
		}
		
		for(int i = 0; i < texts.length; i++) {
			assertTrue(Arrays.equals(texts[i], zkscratch.read("file" + i)));
		}
	}
	
	@Test
	public void testDistinctDirectoryWrites() throws IOException {
		byte[][] texts = new byte[10][];
		for(int i = 0; i < texts.length; i++) {
			texts[i] = ("data" + i).getBytes();
			zkscratch.write("directory" + i + "/file", texts[i]);
		}
		
		for(int i = 0; i < texts.length; i++) {
			assertTrue(Arrays.equals(texts[i], zkscratch.read("directory" + i + "/file")));
		}
	}
	
	@Test
	public void testAssertPathExistsPositive() throws IOException {
		zkscratch.write("exists", "i sync, therefore i am".getBytes());
		zkscratch.assertPathExists("exists");
	}
	
	@Test(expected=ENOENTException.class)
	public void testAssertPathExistsNegative() throws IOException {
		zkscratch.assertPathExists("nonexistent");
	}
	
	@Test
	public void testAssertPathDoesntExistPositive() throws IOException {
		zkscratch.assertPathDoesntExist("nonexistent");
	}
	
	@Test(expected=EEXISTSException.class)
	public void testAssertPathDoesntExistNegative() throws IOException {
		zkscratch.write("exists", "i sync, therefore i am".getBytes());
		zkscratch.assertPathDoesntExist("exists");
	}
	
	@Test
	public void testAssertDirectoryIsEmptyPositive() throws IOException {
		zkscratch.mkdir("emptydir");
		zkscratch.assertDirectoryIsEmpty("emptydir");
	}
	
	@Test(expected=ENOTEMPTYException.class)
	public void testAssertDirectoryIsEmptyNegative() throws IOException {
		zkscratch.write("nonemptydir/a", "some data".getBytes());
		zkscratch.assertDirectoryIsEmpty("nonemptydir");
	}
	
	// no scoping on ZKFS, so disable these tests
	@Override @Test public void testScopedFS() { }
	@Override @Test public void testScopedMakesDirectory() { }
	@Override @Test public void testScopedPurge() { }
	
	@Test
	public void testAlternativePageSizes() throws IOException {
		// let's try some different page sizes to see if we can gum things up.
		CryptoSupport crypto = CryptoSupport.defaultCrypto();
		
		int[] pageSizes = {
				RevisionInfo.FIXED_SIZE >> 1,
				RevisionInfo.FIXED_SIZE,
				RevisionInfo.FIXED_SIZE << 1,
				59049, // power of 3
				50625, // power of 3*5
				131071, // mersenne prime
				ZKArchive.DEFAULT_PAGE_SIZE >> 1,
				ZKArchive.DEFAULT_PAGE_SIZE << 1,
				2000000, // big page, power of 10
			};
		for(int pageSize : pageSizes) {
			ZKArchive archive = master.createArchive(pageSize, "archive " + pageSize);
			ZKFS fs = archive.openBlank();
			byte[] immediateData = crypto.prng(Util.serializeInt(pageSize+0)).getBytes(crypto.hashLength()-1);
			byte[] onePageData = crypto.prng(Util.serializeInt(pageSize+1)).getBytes(pageSize-1);
			byte[] twoPageData = crypto.prng(Util.serializeInt(pageSize+2)).getBytes(2*pageSize-1);
			byte[] tenPageData = crypto.prng(Util.serializeInt(pageSize+3)).getBytes(10*pageSize-1);
			
			fs.write("immediate", immediateData);
			fs.write("1page", onePageData);
			fs.write("2page", twoPageData);
			fs.write("10page", tenPageData);
			RevisionTag rev = fs.commit();
			fs.close();
			
			fs = rev.getFS();
			assertArrayEquals(immediateData, fs.read("immediate"));
			assertArrayEquals(onePageData, fs.read("1page"));
			assertArrayEquals(twoPageData, fs.read("2page"));
			assertArrayEquals(tenPageData, fs.read("10page"));
			archive.close();
			fs.close();
		}
	}
	
	@Test
	public void testRebaseUpdatesBaseRevision() throws IOException {
		RevisionTag rev1 = zkscratch.commit();
		zkscratch.commit();
		assertNotEquals(rev1, zkscratch.baseRevision);
		zkscratch.rebase(rev1);
		assertEquals(rev1, zkscratch.baseRevision);
	}
	
	@Test
	public void testRebaseResetsFilesystem() throws IOException {
		String[] paths = new String[] { "dir/1", "dir/2", "dir/3", "a", "b", "c" };

		for(String path : paths) {
			zkscratch.write(path, path.getBytes());
		}
		zkscratch.mkdir("deaddir");
		RevisionTag revtag = zkscratch.commit();
		
		for(String path : paths) {
			zkscratch.write(path, (path + "modified").getBytes());
		}
		zkscratch.rmdir("deaddir");
		zkscratch.commit();
		
		zkscratch.rebase(revtag);
		for(String path : paths) {
			assertArrayEquals(path.getBytes(), zkscratch.read(path));
		}
		assertTrue(zkscratch.stat("deaddir").isDirectory());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForFileCreation() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.write("newfile", "some data".getBytes());
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForFileModification() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		zkscratch.write("newfile", "some data".getBytes());
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.write("newfile", "different data".getBytes());
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForFileUnlink() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		zkscratch.write("newfile", "some data".getBytes());
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.unlink("newfile");
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForSymlink() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		zkscratch.write("origin", "some data".getBytes());
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.symlink("origin", "destination");
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForHardlink() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		zkscratch.write("origin", "some data".getBytes());
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.link("origin", "destination");
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForMkfifo() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.mkfifo("somefifo");
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForMknod() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.mknod("somenode", Stat.TYPE_CHARACTER_DEVICE, 0, 0);
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForMkdir() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.mkdir("somedir");
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForRmdir() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		zkscratch.mkdir("somedir");
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.rmdir("somedir");
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForChmod() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		zkscratch.write("somefile", "some data".getBytes());
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.chmod("somefile", 0);
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForChownWithUid() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		zkscratch.write("somefile", "some data".getBytes());
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.chown("somefile", 1234);
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForChownWithUsername() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		zkscratch.write("somefile", "some data".getBytes());
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.chown("somefile", "someguy");
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForChgrpWithGid() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		zkscratch.write("somefile", "some data".getBytes());
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.chgrp("somefile", 1234);
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForChgrpWithGroupname() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		zkscratch.write("somefile", "some data".getBytes());
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.chgrp("somefile", "somegroup");
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForSetMtime() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		zkscratch.write("somefile", "some data".getBytes());
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.setMtime("somefile", 123456);
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForSetAtime() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		zkscratch.write("somefile", "some data".getBytes());
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.setAtime("somefile", 123456);
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testMonitorsReceiveDirtyMessagesForSetCtime() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		zkscratch.write("somefile", "some data".getBytes());
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.setCtime("somefile", 123456);
		assertTrue(notified.booleanValue());
	}
	
	@Test
	public void testRemovedMonitorsDoNotReceiveDirtyMessages() throws IOException {
		MutableBoolean notified = new MutableBoolean();
		ZKFSDirtyMonitor monitor = (f)->notified.setTrue();
		zkscratch.addMonitor(monitor);
		zkscratch.removeMonitor(monitor);
		
		assertFalse(notified.booleanValue());
		zkscratch.write("somefile", "bytes".getBytes());
		assertFalse(notified.booleanValue());
	}
	
	@Test
	public void testRevisionTitleDefaultsToArchiveDescription() throws IOException {
		assertEquals(zkscratch.getArchive().getConfig().getDescription(),
				zkscratch.getRevisionInfo().getTitle());
	}
	
	@Test
	public void testRevisionTitleDefaultsToPreviousRevision() throws IOException {
		zkscratch.getInodeTable().setNextTitle("testing");
		zkscratch.commit();
		assertEquals("testing", zkscratch.getRevisionInfo().getTitle());
		zkscratch.commit();
		assertEquals("testing", zkscratch.getRevisionInfo().getTitle());
	}
	
	protected byte[] generateFileData(String key, int length) {
		ByteBuffer buf = ByteBuffer.allocate(4);
		buf.putInt(length);
		return zkscratch.archive.crypto.expand(key.getBytes(), length, buf.array(), "zksync".getBytes());
	}
	
	public static void cheapenArgon2Costs() {
		CryptoSupport.cheapArgon2 = true;
	}
	
	public static void restoreArgon2Costs() {
		CryptoSupport.cheapArgon2 = false;
	}
}
