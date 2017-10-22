package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Security;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.fs.FSTestBase;
import com.acrescrypto.zksync.fs.File;
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
				if(!Arrays.equals(buf, zkscratch.read("page-boundary-test"))) {
					System.out.println("page " + pageCount + ", mod " + mod);
				}
				
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
		ZKDirectory rootDir = zkscratch.opendir("/");
		Revision rev = zkscratch.commit();
		
		ZKFS readFs = new ZKFS(zkscratch.getStorage(), "zksync".toCharArray(), rev);
		ZKDirectory readRootDir = readFs.opendir("/");
		assertTrue(Arrays.equals(content, readFs.read("basic-archive-test")));
	}
	
	// TODO: inode table tests: literal inode table, single-page inode table, multipage inode table
	//       modes, directories, hardlinks, symlinks, fifos, sockets, chardevs, blockdevs  
	// TODO: open default revision
	// TODO: open non-default revision
	// TODO: test alternative page size
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
	
	protected void cheapenArgon2Costs() {
		// cut down test runtime by making argon2 really cheap
		oldDefaultTimeCost = PubConfig.defaultArgon2TimeCost;
		oldDefaultMemoryCost = PubConfig.defaultArgon2MemoryCost;
		oldDefaultParallelism = PubConfig.defaultArgon2Parallelism;
		
		PubConfig.defaultArgon2TimeCost = 1;
		PubConfig.defaultArgon2MemoryCost = 32;
		PubConfig.defaultArgon2Parallelism = 4;
	}
	
	protected void restoreArgon2Costs() {
		PubConfig.defaultArgon2TimeCost = oldDefaultTimeCost;
		PubConfig.defaultArgon2MemoryCost = oldDefaultMemoryCost;
		PubConfig.defaultArgon2Parallelism = oldDefaultParallelism;
	}
	
	protected static void deleteFiles() {
		java.io.File scratchDir = new java.io.File(SCRATCH_DIR);
		try {
			FileUtils.deleteDirectory(scratchDir);
		} catch (IOException e) {}
	}
}
