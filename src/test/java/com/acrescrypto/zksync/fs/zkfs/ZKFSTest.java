package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.ENOTEMPTYException;
import com.acrescrypto.zksync.fs.FSTestBase;
import com.acrescrypto.zksync.fs.File;

public class ZKFSTest extends FSTestBase {
	 // TODO: this is going to break on Windows
	public final static String SCRATCH_DIR = "/tmp/zksync-test/zkfs";
	
	int oldDefaultTimeCost, oldDefaultParallelism, oldDefaultMemoryCost;
	ZKFS zkscratch;
	static ZKMaster master;
	
	@Before
	public void beforeEach() throws IOException {
		cheapenArgon2Costs();
		master = ZKMaster.openAtPath((String description) -> { return "zksync".getBytes(); }, SCRATCH_DIR);
		master.purge();
		scratch = zkscratch = master.newArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").openBlank();
		prepareExamples();
	}
	
	@After
	public void afterEach() {
		restoreArgon2Costs();
	}
	
	@BeforeClass
	public static void beforeClass() {
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@AfterClass
	public static void afterClass() throws IOException {
		master.purge();
	}
	
	@Test
	public void testMultipageWrite() throws IOException {
		byte[] text = new byte[(int) (10*zkscratch.getArchive().config.pageSize)];
		for(int i = 0; i < text.length; i++) text[i] = (byte) (i % 256);
		scratch.write("multipage-write", text);
		assertTrue(Arrays.equals(text, scratch.read("multipage-write")));
	}
	
	@Test
	public void testMultipageModify() throws IOException {
		byte[] text = new byte[(int) (10*zkscratch.getArchive().config.pageSize)];
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
		byte[] text = new byte[(int) (10*zkscratch.getArchive().config.pageSize)];
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
				byte[] buf = generateFileData("page-boundary-test", (int) (pageCount*zkscratch.getArchive().config.pageSize + mod));
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
		byte[] testData = generateFileData("page-boundary", (int) (pageCount*zkscratch.getArchive().config.pageSize));
		ByteBuffer testBuf = ByteBuffer.wrap(testData);
		
		ZKFile testFile = zkscratch.open("page-boundary-extension-test", File.O_RDWR|File.O_CREAT);
		for(int page = 0; page < pageCount; page++) {
			int[] writeLengths = { 1, (int) zkscratch.getArchive().config.pageSize-2, 1 };
			for(int writeLength: writeLengths) {
				byte[] writeData = new byte[writeLength];
				testBuf.get(writeData);
				testFile.write(writeData);
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
	
	// TODO: boundary truncations
	
	@Test
	public void testBasicArchiveWrite() throws IOException {
		byte[] content = "there is a house down in new orleans they call the rising sun".getBytes();
		zkscratch.write("basic-archive-test", content);
		assertTrue(Arrays.equals(content, zkscratch.read("basic-archive-test")));
		RefTag rev = zkscratch.commit();
		
		ZKFS readFs = rev.readOnlyFS();
		assertTrue(Arrays.equals(content, readFs.read("basic-archive-test")));
	}
	
	// ---- cut here ----
	// TODO: everything after this should probably go into its own test class (revision, inode table) 
	
	@Test
	public void testSuccessiveRevisions() throws IOException {
		int numRevisions = 8;
		RefTag[] revisions = new RefTag[numRevisions+1];
		revisions[0] = RefTag.blank(zkscratch.archive);
		
		for(int i = 0; i < numRevisions; i++) {
			ZKFS revFs = revisions[i].getFS();
			revFs.write("successive-revisions", ("Version " + i).getBytes());
			revisions[i+1] = revFs.commit();
		}
		
		for(int i = 1; i <= numRevisions; i++) {
			ZKFS revFs = revisions[i].getFS();
			byte[] text = ("Version " + (i-1)).getBytes();
			assertTrue(Arrays.equals(text, revFs.read("successive-revisions")));
			assertEquals(1, revFs.getRevisionInfo().getNumParents());
			if(i > 1) {
				assertEquals(revisions[i-1], revFs.getRevisionInfo().getParents().get(0));
			}
		}
	}
	
	@Test
	public void testIntensiveRevisions() throws IOException {
		int numRevisions = 31;
		RefTag[] revisions = new RefTag[numRevisions];
		
		for(int i = 0; i < numRevisions; i++) {
			RefTag parent = null;
			if(i == 0) parent = RefTag.blank(zkscratch.archive);
			else parent = revisions[(i-1)/2]; 
			ZKFS revFs = parent.getFS();
			revFs.write("intensive-revisions", ("Version " + i).getBytes());
			revisions[i] = revFs.commit();
		}

		for(int i = 0; i < numRevisions; i++) {
			ZKFS revFs = revisions[i].getFS();
			byte[] text = ("Version " + i).getBytes();
			assertTrue(Arrays.equals(text, revFs.read("intensive-revisions")));
			assertEquals(1, revFs.getRevisionInfo().getNumParents());
			if(i > 0) {
				assertEquals(revisions[(i-1)/2], revFs.getRevisionInfo().getParents().get(0));
			}
		}
	}
	
	@Test
	public void testSinglePageInodeTable() throws IOException {
		RefTag rev = zkscratch.commit();
		assertTrue(zkscratch.inodeTable.getStat().getSize() >= zkscratch.archive.crypto.hashLength());
		assertTrue(zkscratch.inodeTable.getStat().getSize() <= zkscratch.archive.config.pageSize);
		assertEquals(1, zkscratch.inodeTable.inode.refTag.numPages);
		ZKFS revFs = rev.getFS();
		assertTrue(Arrays.equals(revFs.inodeTable.merkle.getRefTag().getBytes(), zkscratch.inodeTable.merkle.getRefTag().getBytes()));
		assertEquals(zkscratch.inodeTable.merkle.numPages, revFs.inodeTable.merkle.numPages);
		assertEquals(zkscratch.inodeTable.getStat().getSize(), revFs.inodeTable.getStat().getSize());
	}
	
	@Test
	public void testMultiPageInodeTable() throws IOException {
		for(int i = 0; i < 1000; i++) {
			String path = String.format("multipage-inode-table-padding-%04d", i);
			zkscratch.write(path, "foo".getBytes());
		}
		
		RefTag rev = zkscratch.commit();
		assertTrue(zkscratch.inodeTable.getStat().getSize() > zkscratch.archive.config.pageSize);
		ZKFS revFs = rev.readOnlyFS();
		
		assertTrue(Arrays.equals(revFs.inodeTable.merkle.getRefTag().getBytes(), zkscratch.inodeTable.merkle.getRefTag().getBytes()));
		assertEquals(zkscratch.inodeTable.merkle.numPages, revFs.inodeTable.merkle.numPages);
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
	
	// TODO: modes, directories, hardlinks, symlinks, fifos, sockets, chardevs, blockdevs
	// TODO: open default revision
	// TODO: open non-default revision
	// TODO: test alternative page size
	// TODO: test alternative literal size
	// TODO: test configurable argon2 parameters
	
	
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
