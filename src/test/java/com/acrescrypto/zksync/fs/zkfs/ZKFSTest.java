package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.fs.FSTestBase;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.zkfs.config.PubConfig;

public class ZKFSTest extends FSTestBase {
	 // TODO: this is going to break on Windows
	public final static String SCRATCH_DIR = "/tmp/zksync-test/zkfs";
	
	int oldDefaultTimeCost, oldDefaultParallelism, oldDefaultMemoryCost;
	ZKFS zkscratch;
	
	@Before
	public void beforeEach() throws IOException {
		cheapenArgon2Costs();
		deleteFiles();
		LocalFS storage = new LocalFS(SCRATCH_DIR);
		java.io.File scratchDir = new java.io.File(SCRATCH_DIR);
		try {
			FileUtils.deleteDirectory(scratchDir);
		} catch (IOException e) {}
		(new java.io.File(SCRATCH_DIR)).mkdirs();

		scratch = zkscratch = new ZKFS(storage, "zksync".toCharArray());
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
	public static void afterClass() {
		deleteFiles();
	}
	
	@Test
	public void testMultipageWrite() throws IOException {
		byte[] text = new byte[10*zkscratch.getPrivConfig().getPageSize()];
		for(int i = 0; i < text.length; i++) text[i] = (byte) (i % 256);
		scratch.write("multipage-write", text);
		assertTrue(Arrays.equals(text, scratch.read("multipage-write")));
	}
	
	@Test
	public void testMultipageModify() throws IOException {
		byte[] text = new byte[10*zkscratch.getPrivConfig().getPageSize()];
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
		byte[] text = new byte[10*zkscratch.getPrivConfig().getPageSize()];
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
				byte[] buf = generateFileData("page-boundary-test", pageCount*zkscratch.getPrivConfig().getPageSize() + mod);
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
		byte[] testData = generateFileData("page-boundary", pageCount*zkscratch.getPrivConfig().getPageSize());
		ByteBuffer testBuf = ByteBuffer.wrap(testData);
		
		ZKFile testFile = zkscratch.open("page-boundary-extension-test", File.O_RDWR|File.O_CREAT);
		for(int page = 0; page < pageCount; page++) {
			int[] writeLengths = { 1, zkscratch.getPrivConfig().getPageSize()-2, 1 };
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
		Revision rev = zkscratch.commit();
		
		ZKFS readFs = new ZKFS(zkscratch.getStorage(), "zksync".toCharArray(), rev);
		assertTrue(Arrays.equals(content, readFs.read("basic-archive-test")));
	}
	
	// ---- cut here ----
	// TODO: everything after this should probably go into its own test class (revision, inode table) 
	
	@Test
	public void testStoredRevision() throws IOException {
		zkscratch.write("storedrevision", "pak chooie unf".getBytes());
		Revision rev = zkscratch.commit();
		String path = rev.getTag().getPath();
		
		RevisionTag readTag = new RevisionTag(zkscratch, path);
		Revision readRev = new Revision(readTag);
		ZKFS readFs = new ZKFS(zkscratch.getStorage(), "zksync".toCharArray(), readRev);
		
		assertTrue(Arrays.equals("pak chooie unf".getBytes(), readFs.read("storedrevision")));
	}
	
	@Test(expected = SecurityException.class)
	public void testTamperedRevision() throws IOException {
		zkscratch.write("storedrevision", "pak chooie unf".getBytes());
		Revision rev = zkscratch.commit();
		String path = rev.getTag().getPath();
		
		byte[] ciphertext = zkscratch.storage.read(path);
		ciphertext[200] ^= 0x40; // no significance to the choice of byte or bit; just twiddling an arbitrary bit
		zkscratch.storage.write(path, ciphertext);
		
		RevisionTag readTag = new RevisionTag(zkscratch, path);
		new Revision(readTag);
	}

	@Test(expected = SecurityException.class)
	public void testTamperedRevisionTag() throws IOException {
		zkscratch.write("storedrevision", "pak chooie unf".getBytes());
		Revision rev = zkscratch.commit();
		String oldPath = rev.getTag().getPath();
		rev.getTag().tag[4] ^= 0x20; // arbitrary bitflip
		String path = rev.getTag().getPath();
		
		zkscratch.storage.link(oldPath, path);
		zkscratch.storage.unlink(oldPath);
		
		RevisionTag readTag = new RevisionTag(zkscratch, path);
		new Revision(readTag);
	}
	
	@Test
	public void testRevisionTimeSquashing() throws IOException {
		Revision rev = zkscratch.commit();
		Stat stat = zkscratch.storage.stat(rev.tag.getPath());
		assertTrue(stat.getMtime() == 0);
		assertTrue(stat.getAtime() == 0);
	}
	
	@Test
	public void testSuccessiveRevisions() throws IOException {
		int numRevisions = 8;
		Revision[] revisions = new Revision[numRevisions+1];
		
		for(int i = 0; i < numRevisions; i++) {
			ZKFS revFs = new ZKFS(zkscratch.getStorage(), "zksync".toCharArray(), revisions[i]);
			revFs.write("successive-revisions", ("Version " + i).getBytes());
			revisions[i+1] = revFs.commit();
		}
		
		for(int i = 1; i <= numRevisions; i++) {
			ZKFS revFs = new ZKFS(zkscratch.getStorage(), "zksync".toCharArray(), revisions[i]);
			byte[] text = ("Version " + (i-1)).getBytes();
			assertTrue(Arrays.equals(text, revFs.read("successive-revisions")));
			assertEquals(1, revFs.getInodeTable().getRevision().getNumParents());
			if(i > 1) {
				assertEquals(revisions[i-1].getTag(), revFs.getInodeTable().getRevision().getParentTag(0));
			}
		}
	}
	
	@Test
	public void testIntensiveRevisions() throws IOException {
		int numRevisions = 31;
		Revision[] revisions = new Revision[numRevisions];
		
		for(int i = 0; i < numRevisions; i++) {
			Revision parent = null;
			if(i > 0) parent = revisions[(i-1)/2]; 
			ZKFS revFs = new ZKFS(zkscratch.getStorage(), "zksync".toCharArray(), parent);
			revFs.write("intensive-revisions", ("Version " + i).getBytes());
			revisions[i] = revFs.commit();
		}

		for(int i = 0; i < numRevisions; i++) {
			ZKFS revFs = new ZKFS(zkscratch.getStorage(), "zksync".toCharArray(), revisions[i]);
			byte[] text = ("Version " + i).getBytes();
			assertTrue(Arrays.equals(text, revFs.read("intensive-revisions")));
			assertEquals(1, revFs.getInodeTable().getRevision().getNumParents());
			if(i > 0) {
				assertEquals(revisions[(i-1)/2].getTag(), revFs.getInodeTable().getRevision().getParentTag(0));
			}
		}
	}
	
	@Test
	public void testRevisionListing() throws IOException {
		int numRevisions = 2;
		HashSet<RevisionTag> set = new HashSet<RevisionTag>();
		Revision lastRev = null;

		for(int i = 0; i < numRevisions; i++) {
			ZKFS revFs = new ZKFS(zkscratch.getStorage(), "zksync".toCharArray(), lastRev);
			revFs.write("revision-listing", ("Version " + i).getBytes());
			lastRev = revFs.commit();
			set.add(lastRev.getTag());
		}
		
		RevisionTree tree = zkscratch.getRevisionTree();
		ArrayList<RevisionTag> tags = tree.revisionTags();
		
		assertEquals(numRevisions, set.size());
		assertEquals(numRevisions, tags.size());
		for(RevisionTag tag : tags) {
			assertTrue(set.contains(tag));
		}
	}
	
	@Ignore // ignored for now because the immediate threshold is smaller than the minimum table size, and it's not easy to vary (yet)
	public void testImmediateInodeTable() throws IOException {
		ZKFS fs = new ZKFS(zkscratch.getStorage(), "zksync".toCharArray());
		Revision rev = fs.commit();
		assertTrue(fs.inodeTable.getStat().getSize() < fs.getPrivConfig().getImmediateThreshold());
		assertEquals(fs.inodeTable.getStat().getSize(), rev.supernode.getStat().getSize());
		ZKFS revFs = new ZKFS(fs.getStorage(), "zksync".toCharArray(), rev);
		assertTrue(Arrays.equals(revFs.inodeTable.merkel.getMerkelTag(), fs.inodeTable.merkel.getMerkelTag()));
	}
	
	@Test
	public void testSinglePageInodeTable() throws IOException {
		Revision rev = zkscratch.commit();
		assertTrue(zkscratch.inodeTable.getStat().getSize() > zkscratch.getPrivConfig().getImmediateThreshold());
		assertTrue(zkscratch.inodeTable.getStat().getSize() < zkscratch.getPrivConfig().getPageSize());
		assertEquals(zkscratch.inodeTable.getStat().getSize(), rev.supernode.getStat().getSize());
		ZKFS revFs = new ZKFS(zkscratch.getStorage(), "zksync".toCharArray(), rev);
		assertTrue(Arrays.equals(revFs.inodeTable.merkel.getMerkelTag(), zkscratch.inodeTable.merkel.getMerkelTag()));
	}
	
	@Test
	public void testMultiPageInodeTable() throws IOException {
		for(int i = 0; i < 1000; i++) {
			String path = String.format("multipage-inode-table-padding-%04d", i);
			zkscratch.write(path, "foo".getBytes());
		}
		
		Revision rev = zkscratch.commit();
		assertTrue(zkscratch.inodeTable.getStat().getSize() > zkscratch.getPrivConfig().getPageSize());
		assertEquals(zkscratch.inodeTable.getStat().getSize(), rev.supernode.getStat().getSize());
		ZKFS revFs = new ZKFS(zkscratch.getStorage(), "zksync".toCharArray(), rev);
		assertTrue(Arrays.equals(revFs.inodeTable.merkel.getMerkelTag(), zkscratch.inodeTable.merkel.getMerkelTag()));
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
	public void testNextInodeIdAfterFSLoad() throws IOException {
		for(int i = 0; i < 10; i++) zkscratch.write("burner-inode-"+i, ("content"+i).getBytes());
		Revision rev = zkscratch.commit();
		ZKFS recoveredFs = new ZKFS(rev);
		assertEquals(zkscratch.inodeTable.nextInodeId, recoveredFs.inodeTable.nextInodeId);
	}
	
	// TODO: modes, directories, hardlinks, symlinks, fifos, sockets, chardevs, blockdevs
	// TODO: open default revision
	// TODO: open non-default revision
	// TODO: test alternative page size
	// TODO: test alternative literal size
	// TODO: test configurable argon2 parameters
	
	protected byte[] generatePageData(String key, int pageNum) {
		ByteBuffer buf = ByteBuffer.allocate(4);
		buf.putInt(pageNum);
		return zkscratch.crypto.expand(key.getBytes(), zkscratch.getPrivConfig().getPageSize(), buf.array(), "zksync".getBytes());
	}
	
	protected byte[] generateFileData(String key, int length) {
		ByteBuffer buf = ByteBuffer.allocate(4);
		buf.putInt(length);
		return zkscratch.crypto.expand(key.getBytes(), length, buf.array(), "zksync".getBytes());
	}
	
	public static void cheapenArgon2Costs() {
		// TODO: this broke when we made it static, but right now we really don't care because we never actually want expensive argon2 in tests
//		// cut down test runtime by making argon2 really cheap
//		oldDefaultTimeCost = PubConfig.defaultArgon2TimeCost;
//		oldDefaultMemoryCost = PubConfig.defaultArgon2MemoryCost;
//		oldDefaultParallelism = PubConfig.defaultArgon2Parallelism;
//		
		PubConfig.defaultArgon2TimeCost = 1;
		PubConfig.defaultArgon2MemoryCost = 32;
		PubConfig.defaultArgon2Parallelism = 4;
	}
	
	protected static void restoreArgon2Costs() {
		// TODO: see note for cheapenArgon2Costs
//		PubConfig.defaultArgon2TimeCost = oldDefaultTimeCost;
//		PubConfig.defaultArgon2MemoryCost = oldDefaultMemoryCost;
//		PubConfig.defaultArgon2Parallelism = oldDefaultParallelism;
	}
	
	public static void deleteFiles() {
		java.io.File scratchDir = new java.io.File(SCRATCH_DIR);
		try {
			FileUtils.deleteDirectory(scratchDir);
		} catch (IOException e) {}
	}
}
