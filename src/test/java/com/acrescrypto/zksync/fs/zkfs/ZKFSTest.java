package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.HashContext;
import com.acrescrypto.zksync.crypto.PRNG;
import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.EISNOTDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.ENOTEMPTYException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FSTestBase;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.ZKFS.ZKFSDirtyMonitor;
import com.acrescrypto.zksync.utility.Shuffler;
import com.acrescrypto.zksync.utility.Util;

public class ZKFSTest extends FSTestBase {
	public class CantDoThatRightNowException extends RuntimeException {
		private static final long serialVersionUID = 1L;
	}
	
	int oldDefaultTimeCost, oldDefaultParallelism, oldDefaultMemoryCost;
	ZKFS zkscratch;
	ZKMaster master;
	CryptoSupport crypto;
	
	@Before
	public void beforeEach() throws IOException {
		TestUtils.startDebugMode();
		master = ZKMaster.openBlankTestVolume();
		scratch = zkscratch = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").openBlank();
		crypto = master.getCrypto();
		prepareExamples();
	}
	
	@After
	public void afterEach() throws IOException {
		master.close();
		zkscratch.close();
		zkscratch.archive.close();
		TestUtils.stopDebugMode();
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
		
		try(ZKFS readFs = rev.readOnlyFS()) {
			assertTrue(Arrays.equals(content, readFs.read("basic-archive-test")));
		}
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
		try(ZKFS revFs = rev.readOnlyFS()) {
			assertTrue(Arrays.equals(revFs.inodeTable.tree.getRefTag().getBytes(), zkscratch.inodeTable.tree.getRefTag().getBytes()));
			assertEquals(zkscratch.inodeTable.tree.maxNumPages, revFs.inodeTable.tree.maxNumPages);
			assertEquals(zkscratch.inodeTable.getStat().getSize(), revFs.inodeTable.getStat().getSize());
		}
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
				2*RevisionInfo.FIXED_SIZE,
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
	
	@Test
	public void testCommitWithOpenFileWithPendingChangesCommitsFile() throws IOException {
		try(ZKFile file = zkscratch.open("afile", File.O_CREAT|File.O_RDWR|File.O_TRUNC)) {
			byte[] contents = "We must part now.\nMy life goes on,\nbut my heart wont give you up\nEre I walk away,\nlet me hear you say,\nI meant as much to you...".getBytes();
			file.write(contents);
			zkscratch.commit();
			
			assertArrayEquals(contents, zkscratch.read(file.getPath()));
			Inode inode = zkscratch.inodeTable.inodeWithId(file.getStat().getInodeId());
			assertFalse(inode.getRefTag().isBlank());
		}
	}
	
	@Test
	public void testFileTruncateToImmediateSize() throws IOException {
		// mimics an observed bug: truncation to immediate size caused immediate reftag to be treated as non-immediate 
		String path = "tricky-file";
		PRNG prng = crypto.prng(crypto.symNonce(0));
		
		zkscratch.write(path, prng.getBytes(23610));
		try(ZKFile file = zkscratch.open(path, File.O_RDWR)) {
			file.seek(16785, File.SEEK_SET);
			file.write(prng.getBytes(129805));
		}
		
		zkscratch.truncate(path, 83154);
		zkscratch.truncate(path, 17);
		zkscratch.truncate(path, 11);
		
		// and if we don't lock up, we passed the test.
	}
	
	@Test
	public void testListRecursiveWithSymlinkLoopIsFinite() throws IOException {
		zkscratch.purge();
		zkscratch.symlink("/", "a");
		zkscratch.symlink("/", "b");
		
		try(ZKDirectory dir = zkscratch.opendir("/")) {
			dir.listRecursive();
		}
		
		// if we don't halt and catch fire, that's a victory
	}
	
	@Test
	public void testDirectoryCacheIsNotDeceivedBySymlinks() throws IOException {
		zkscratch.purge();
		zkscratch.mkdir("dir");
		zkscratch.symlink("dir", "link");
		zkscratch.symlink("link", "double-link");
		
		try(
			ZKDirectory direct = zkscratch.opendir("dir");
			ZKDirectory linked = zkscratch.opendir("link");
			ZKDirectory doubleLinked = zkscratch.opendir("double-link");
		) {
			assertTrue(direct == linked);
			assertTrue(direct == doubleLinked);
		}
	}
	
	@Test
	public void testMultithreadedFileExtensions() throws IOException {
		// spin up some write threads to put random data to separate files.
		// another thread commits while this is happening.
		// in the end, all the files should have all the data.
		
		int numDemons = 8; // number of simultaneous writers
		int durationMs = 1000; // how long should demon threads run for
		long deadline = System.currentTimeMillis() + durationMs;
		LinkedList<Thread> threads = new LinkedList<>();
		final CryptoSupport crypto = zkscratch.getArchive().getMaster().getCrypto();
		int[] lengths = new int[numDemons];
		HashContext[] hashes = new HashContext[numDemons];
		
		for(int i = 0; i < numDemons; i++) {
			final int ii = i;
			Thread t = new Thread(()->{
				PRNG prng = crypto.prng(Util.serializeInt(ii));
				hashes[ii] = crypto.startHash();
				try(ZKFile file = zkscratch.open("file" + ii, File.O_CREAT|File.O_RDWR|File.O_TRUNC)) {
					while(System.currentTimeMillis() < deadline) {
						byte[] data = prng.getBytes(prng.getInt(1024));
						hashes[ii].update(data);
						lengths[ii] += data.length;
						file.write(data);
					}
				} catch (IOException exc) {
					exc.printStackTrace();
					fail();
				}
			});
			threads.add(t);
			t.start();
		}
		
		Thread commitThread = new Thread(()->{
			try {
				while(System.currentTimeMillis() < deadline) {
					zkscratch.commit();
				}
			} catch(IOException exc) {
				exc.printStackTrace();
				fail();
			}
			
		});
		
		commitThread.start();
		
		Util.sleep(deadline - System.currentTimeMillis());
		assertTrue(Util.waitUntil(2000, ()->{
			if(commitThread.isAlive()) return false;
			for(Thread t : threads) {
				if(t.isAlive()) return false;
			}
			
			return true;
		}));
		
		zkscratch.commit();
		
		for(int i = 0; i < numDemons; i++) {
			String path = "file" + i;
			byte[] actualHash = crypto.hash(zkscratch.read(path));
			byte[] expectedHash = hashes[i].finish();
			assertEquals(lengths[i], zkscratch.stat("file"+i).getSize());
			assertArrayEquals(expectedHash, actualHash);
		}
	}
	
	@Test
	public void testMultithreadedFileCreations() throws IOException {
		// spin up some write threads to put random data to separate files.
		// another thread commits while this is happening.
		// in the end, all the files should have all the data.
		
		int numDemons = 8; // number of simultaneous writers
		int durationMs = 1000; // how long should demon threads run for
		long deadline = System.currentTimeMillis() + durationMs;
		LinkedList<Thread> threads = new LinkedList<>();
		final CryptoSupport crypto = zkscratch.getArchive().getMaster().getCrypto();
		ConcurrentHashMap<String,byte[]> hashes = new ConcurrentHashMap<>();
		ConcurrentHashMap<String,Long> lengths = new ConcurrentHashMap<>();
		
		for(int i = 0; i < numDemons; i++) {
			final int ii = i;
			Thread t = new Thread(()->{
				PRNG prng = crypto.prng(Util.serializeInt(ii));
				while(System.currentTimeMillis() < deadline) {
					int pages = prng.getInt(3);
					byte[] data;
					if(pages == 0) {
						data = prng.getBytes(prng.getInt(crypto.hashLength()));
					} else {
						int pageSize = zkscratch.getArchive().getConfig().getPageSize();
						data = prng.getBytes((pages-1)*pageSize + prng.getInt(pageSize));
					}
					
					String path = Util.bytesToHex(prng.getBytes(8));
					hashes.put(path, crypto.hash(data, 0, data.length));
					lengths.put(path, Long.valueOf(data.length));
					try {
						zkscratch.write(path, data);
					} catch (IOException e) {
						e.printStackTrace();
						fail();
					}
				}
			});
			threads.add(t);
			t.start();
		}
		
		Thread commitThread = new Thread(()->{
			try {
				while(System.currentTimeMillis() < deadline) {
					zkscratch.commit();
				}
			} catch(IOException exc) {
				exc.printStackTrace();
				fail();
			}
			
		});
		
		commitThread.start();
		
		Util.sleep(deadline - System.currentTimeMillis());
		assertTrue(Util.waitUntil(2000, ()->{
			if(commitThread.isAlive()) return false;
			for(Thread t : threads) {
				if(t.isAlive()) return false;
			}
			
			return true;
		}));
		
		zkscratch.commit();
		
		for(String path : lengths.keySet()) {
			byte[] actualHash = crypto.hash(zkscratch.read(path));
			long actualLen = zkscratch.stat(path).getSize();
			assertArrayEquals(hashes.get(path), actualHash);
			assertEquals(lengths.get(path).longValue(), actualLen);
		}
	}
	
	@Test
	public void testMultithreadedChaos() throws IOException {
		// spin up some write threads to put random data to separate files.
		// another thread commits while this is happening.
		// in the end, all the files should have all the data.
		
		int numDemons = 16; // number of simultaneous writers
		int durationMs = 3000; // how long should demon threads run for
		long deadline = System.currentTimeMillis() + durationMs;
		ConcurrentHashMap<Thread,Object> threads = new ConcurrentHashMap<>();
		final CryptoSupport crypto = zkscratch.getArchive().getMaster().getCrypto();
		ConcurrentHashMap<String,RefTag> tags = new ConcurrentHashMap<>();
		zkscratch.rebase(RevisionTag.blank(zkscratch.getArchive().getConfig()));
		
		for(int i = 0; i < numDemons; i++) {
			final int ii = i;
			Thread t = new Thread(()->{
				PRNG prng = crypto.prng(Util.serializeInt(ii));
				while(System.currentTimeMillis() < deadline) {
					try {
						takeRandomAction(zkscratch, prng, tags);
					} catch (IOException exc) {
						exc.printStackTrace();
						fail();
					}
				}
				
				threads.remove(Thread.currentThread());
			});
			threads.put(t, t);
			t.start();
		}
		
		Util.sleep(deadline - System.currentTimeMillis());
		long lastUpdate = System.currentTimeMillis();
		int lastThreadCount = threads.size();
		while(lastThreadCount != 0 && System.currentTimeMillis() - lastUpdate < 10000) {
			if(threads.size() != lastThreadCount) {
				lastThreadCount = threads.size();
				lastUpdate = System.currentTimeMillis();
			}
		}
		assertEquals(0, lastThreadCount);
		
		zkscratch.commit();

		verifyTags(zkscratch, tags);
		
		try(ZKFS fs = zkscratch.baseRevision.getFS()) {
			verifyTags(fs, tags);
		}
	}
	
	public void addTag(String path, RefTag tag, ZKFS fs, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		path = fs.absolutePath(path);
		if(!fs.exists(path)) return;
		if(fs.stat(path).isDirectory()) return;
		if(fs.lstat(path).isSymlink()) {
			String target = fs.readlink(path);
			addTag(target, null, fs, tags);
		}
		
		if(tag == null) {
			tag = fs.inodeForPath(path, false).getRefTag();
		}
		
//		System.out.println("addTag -- " + path + " " + Util.formatRefTag(tag));
		addTagToHardlinks(path, tag, fs, tags);
	}
	
	public void addTagToHardlinks(String path, RefTag tag, ZKFS fs, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		tags.put(path, tag);

		Inode inode = fs.inodeForPath(path, false);
		if(inode.nlink <= 1) return;
		
		tags.forEach((otherPath, otherTag)->{
			try {
				Inode otherStat = fs.inodeForPath(otherPath, false);
				if(otherStat.stat.getInodeId() == inode.stat.getInodeId()) {
					tags.put(otherPath, tag);
				}
			} catch(IOException exc) {}
		});
	}
	
	public String pickRandomFile(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		/* pick a random existing file. to protect against multiple threads claiming the same file,
		 * require the file to be listed in 'tags', then remove it from the list. all callers must then
		 * return the file to the 'tags' list when done with file operation.
		 */
		try(ZKDirectory dir = fs.opendir("/")) {
			ArrayList<String> files = new ArrayList<>(dir.listRecursive(Directory.LIST_OPT_OMIT_DIRECTORIES));
			if(files.size() == 0) throw new CantDoThatRightNowException();
			Shuffler shuffler = new Shuffler(files.size());
			while(shuffler.hasNext()) {
				String path = fs.absolutePath(files.get(shuffler.next()));
				if(!tags.containsKey(path)) continue;
				String target = null;
				if(fs.lstat(path).isSymlink()) {
					target = fs.absolutePath(fs.readlink(path));
				}
				
				synchronized(tags) {
					if(!tags.containsKey(path)) continue;
					if(target != null && !tags.containsKey(path)) continue;
					tags.remove(path);
					
					while(target != null) {
						tags.remove(target);
						try {
							if(fs.lstat(target).isSymlink()) {
								target = fs.readlink(target);
							} else {
								target = null;
							}
						} catch(ENOENTException exc) {
							target = null;
						}
					}
					
					return path;
				}
			}
			
			throw new CantDoThatRightNowException();
		} catch(ENOENTException exc) {
			throw new CantDoThatRightNowException();
		}
	}
	
	public String pickRandomDirectory(ZKFS fs, PRNG prng) throws IOException {
		try(ZKDirectory dir = fs.opendir("/")) {
			Collection<String> files = dir.listRecursive();
			LinkedList<String> directories = new LinkedList<>();
			for(String file : files) {
				try {
					if(!fs.lstat(file).isDirectory()) continue;
				} catch(ENOENTException exc) {
					continue;
				}
				directories.add(file);
			}
			
			directories.add("/");
			
			if(directories.isEmpty()) throw new CantDoThatRightNowException();
			return directories.get(prng.getInt(directories.size()));
		}
	}
	
	public String pickRandomEmptyDirectory(ZKFS fs, PRNG prng) throws IOException {
		try(ZKDirectory dir = fs.opendir("/")) {
			Collection<String> files = dir.listRecursive();
			LinkedList<String> directories = new LinkedList<>();
			for(String file : files) {
				try {
					if(!fs.lstat(file).isDirectory()) continue;
				} catch(ENOENTException exc) {
					continue;
				}
				
				try(ZKDirectory dir2 = fs.opendir(file)) {
					if(dir2.list().size() == 0) {
						directories.add(file);
					}
				}
			}
			
			if(directories.isEmpty()) throw new CantDoThatRightNowException();
			return directories.get(prng.getInt(directories.size()));
		}
	}
	
	public String makeRandomPath(ZKFS fs, PRNG prng) throws IOException {
		String directory = pickRandomDirectory(fs, prng);
		String filename = Util.bytesToHex(prng.getBytes(4));
		String path = Paths.get(directory, filename).toString();
		while(path.startsWith("/")) path = path.substring(1);
		return path;
	}
	
	public String writeRandomFile(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		int type = prng.getInt(3);
		int size, pageSize = fs.getArchive().getConfig().getPageSize();
		if(type == 0) {
			size = prng.getInt(crypto.hashLength()-1);
		} else if(type == 1) {
			size = prng.getInt(pageSize);
		} else {
			int pages = prng.getInt(5)+2;
			size = prng.getInt(pageSize*(pages-1) + pageSize);
		}
		
		String path = makeRandomPath(fs, prng) + "-file";
//		System.out.println("write " + path + " " + size);
		try(ZKFile file = fs.open(path, File.O_CREAT|File.O_TRUNC|File.O_RDWR)) {
			file.write(prng.getBytes(size));
			file.flush();
			addTag(path, file.getInode().getRefTag(), fs, tags);
		} catch(ENOENTException|EISNOTDIRException exc) {
			/* we have a race with the unlink directory action, where our parent directory might get unlinked
			 * from underneath us. Just ignore it.
			 * 
			 * EISNOTDIRException might seem a bit odd, but it can happen if the unlink occurs during
			 * assertPathIsDirectory after we get the inode (so no ENOENT) but before we check the file type.
			 */
			throw new CantDoThatRightNowException();
		}
		
		return path;
	}
	
	public String truncateRandomFile(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		String path = pickRandomFile(fs, prng, tags);
		
		try(ZKFile file = fs.open(path, File.O_RDWR)) {
			long size = file.getStat().getSize();
			long newSize = prng.getInt(Math.max(1, (int) size));
			if(size == 0) {
				addTag(path, file.getInode().getRefTag(), fs, tags);
				throw new CantDoThatRightNowException();
			}
			
//			System.out.println("truncate " + path + " " + newSize);
			file.truncate(newSize);
			file.flush();
			addTag(path, null, fs, tags);
		} catch(ENOENTException exc) {
			throw new CantDoThatRightNowException();
		}
		
		return path;
	}
	
	public String extendRandomFile(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		String path = pickRandomFile(fs, prng, tags);
		int pageSize = fs.getArchive().getConfig().getPageSize();
		byte[] data = prng.getBytes(prng.getInt(2*pageSize));
//		System.out.println("extend " + path + " " + data.length);
		
		try(ZKFile file = fs.open(path, File.O_RDWR|File.O_APPEND)) {
			file.write(data);
			file.flush();
			addTag(path, null, fs, tags);
		} catch(ENOENTException exc) {}
		
		return path;
	}
	
	public String modifyRandomFile(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		String path = pickRandomFile(fs, prng, tags);
		int pageSize = fs.getArchive().getConfig().getPageSize();
		byte[] data = prng.getBytes(prng.getInt(2*pageSize));
		
		try(ZKFile file = fs.open(path, File.O_RDWR)) {
			int offset = prng.getInt(Math.max(1, (int) file.inode.getStat().getSize()));
//			System.out.println("modify " + path + " " + data.length + " " + "@" + offset);
			file.seek(offset, File.SEEK_SET);
			file.write(data);
			file.flush();
			addTag(path, null, fs, tags);
		} catch(ENOENTException exc) {}
		
		return path;
	}

	public String unlinkRandomFile(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		String path = pickRandomFile(fs, prng, tags);
//		System.out.println("unlink " + path);
		
		String target = null;
		try {
			target = fs.readlink(path);
		} catch(EINVALException exc) {}
		
		fs.unlink(path);
		if(target != null) {
			addTag(target, null, fs, tags);
		}
		return path;
	}
	
	public String makeRandomDirectory(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		String path = makeRandomPath(fs, prng) + "-dir";
//		System.out.println("mkdir " + path);
		
		try {
			fs.mkdirp(path);
		} catch(ENOENTException exc) {
			// parent directory can get unlinked before we finish
			throw new CantDoThatRightNowException();
		}
		return path;
	}
	
	public String unlinkRandomDirectory(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		String path = pickRandomEmptyDirectory(fs, prng);
//		System.out.println("rmdir " + path);
		try {
			fs.rmdir(path);
		} catch(ENOENTException|ENOTEMPTYException exc) {
			/* there's a race here; nothing stops someone else from linking into this directory,
			 * or unlinking it themselves before we finish
			 */
			throw new CantDoThatRightNowException();
		}
		return path;
	}
	
	public String makeRandomCommit(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
//		System.out.println("commit");
		fs.commit();
		return null;
	}
	
	public String makeRandomValidFileSymlink(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		String path = makeRandomPath(fs, prng) + "-symlink-file";
		String target = pickRandomFile(fs, prng, tags);
		try {
//			System.out.println("symlink " + path + " -> " + target);
			fs.symlink(target, path);
		} catch(ENOENTException exc) {
			addTag(target, null, fs, tags);
			throw new CantDoThatRightNowException();
		}
		addTag(path, fs.inodeForPath(path, false).getRefTag(), fs, tags);
		return path;
	}
	
	public String makeRandomInvalidSymlink(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		String path = makeRandomPath(fs, prng) + "-symlink-broken";
		String target = makeRandomPath(fs, prng) + "-invalid";
//		System.out.println("symlink " + path + " -> " + target);
		try {
			fs.symlink(target, path);
		} catch(ENOENTException exc) {
			throw new CantDoThatRightNowException();
		}
		addTag(path, fs.inodeForPath(path, false).getRefTag(), fs, tags);
		return path;
	}
	
	public String makeRandomDirectorySymlink(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		String path = makeRandomPath(fs, prng) + "-symlink-dir";
		String target = pickRandomDirectory(fs, prng);
//		System.out.println("symlink " + path + " -> " + target);
		try {
			fs.symlink(target, path);
		} catch(ENOENTException exc) {
			throw new CantDoThatRightNowException();
		}
		addTag(path, fs.inodeForPath(path, false).getRefTag(), fs, tags);
		return path;
	}
	
	public String makeRandomHardlink(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		String link = makeRandomPath(fs, prng) + "-hardlink";
		String target = pickRandomFile(fs, prng, tags);
		try {
//			System.out.println("hardlink " + link + " -> " + target);
			fs.link(target, link);
		} catch(ENOENTException exc) {
			addTag(target, null, fs, tags);
			throw new CantDoThatRightNowException();
		}
		
		addTag(link, null, fs, tags);
		addTag(target, null, fs, tags);
		
		return link;
	}
	
	public String takeRandomAction(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		while(true) {
			try {
				return attemptRandomAction(fs, prng, tags);
			} catch(CantDoThatRightNowException exc) {}
		}
	}
	
	public String attemptRandomAction(ZKFS fs, PRNG prng, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		int r = prng.getInt(100);
		
		if((r -= 30) < 0) {
			return writeRandomFile(fs, prng, tags);
		} else if((r -= 10) < 0) {
			return truncateRandomFile(fs, prng, tags);
		} else if((r -= 10) < 0) {
			return extendRandomFile(fs, prng, tags);
		} else if((r -= 10) < 0) {
			return modifyRandomFile(fs, prng, tags);
		} else if((r -= 10) < 0) {
			return unlinkRandomFile(fs, prng, tags);
		} else if((r -= 10) < 0) {
			return makeRandomDirectory(fs, prng, tags);
		} else if((r -= 2) < 0) {
			return unlinkRandomDirectory(fs, prng, tags);
		} else if((r -= 2) < 0) {
			return makeRandomValidFileSymlink(fs, prng, tags);
		} else if((r -= 2) < 0) {
			return makeRandomInvalidSymlink(fs, prng, tags);
		} else if((r -= 2) < 0) {
			return makeRandomDirectorySymlink(fs, prng, tags);
		} else if((r -= 2) < 0) {
			return makeRandomHardlink(fs, prng, tags);
		} else {
			return makeRandomCommit(fs, prng, tags);
		}
	}
	
	public boolean pathHasRefTag(String path, ZKFS fs, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		Inode inode = fs.inodeForPath(path, false);
		RefTag expectedRefTag = tags.get(path), actualRefTag = inode.getRefTag();
		if(expectedRefTag.equals(actualRefTag)) return true;
		if(inode.nlink <= 1) return false;
		
		// if we wrote to a hardlink, we might have an outdated reftag for this path.
		MutableBoolean found = new MutableBoolean();
		
		tags.forEach((otherPath, otherRefTag)->{
			if(found.isTrue()) return;
			if(!otherRefTag.equals(actualRefTag)) return;
			try {
				Inode otherInode = fs.inodeForPath(otherPath, false);
				if(inode.getStat().getInodeId() == otherInode.getStat().getInodeId()) {
					found.setTrue();
					return;
				}
			} catch (IOException exc) {
				exc.printStackTrace();
				fail();
			}
		});
		
		return found.booleanValue();
	}
	
	public void verifyTags(ZKFS fs, ConcurrentHashMap<String, RefTag> tags) throws IOException {
		LinkedList<String> allowablePaths = new LinkedList<>();
		
		for(String path : tags.keySet()) {
			if(!pathHasRefTag(path, fs, tags)) {
				String text = String.format("Path %s -- expected reftag %s, have %s",
						path,
						Util.formatRefTag(tags.get(path)),
						Util.formatRefTag(fs.inodeForPath(path, false).getRefTag()));
//				System.out.println(text);
				fail(text);
			}
			
			try {
				allowablePaths.add(fs.canonicalPath(path));
			} catch(ENOENTException exc) {
				// ignore broken symlinks
			}
		}
		
		int numFails = 0;
		
		for(String path : fs.opendir("/").listRecursive()) {
			if(!fs.lstat(path).isRegularFile()) continue;
			if(!allowablePaths.contains(fs.canonicalPath(path))) {
//				System.out.println("Path " + fs.canonicalPath(path) + " (as " + path + ")");
				numFails++;
			}
		}
		
		assertEquals(0, numFails);
	}
	
	protected byte[] generateFileData(String key, int length) {
		ByteBuffer buf = ByteBuffer.allocate(4);
		buf.putInt(length);
		return zkscratch.archive.crypto.expand(key.getBytes(), length, buf.array(), "zksync".getBytes());
	}
}
