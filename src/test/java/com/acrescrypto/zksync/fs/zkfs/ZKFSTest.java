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
		(new java.io.File(SCRATCH_DIR)).mkdirs();
		LocalFS storage = new LocalFS(SCRATCH_DIR);
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
	public void filesCreateWithNlink1() throws IOException {
		scratch.write("files-create-with-nlink-1", "yadda yadda".getBytes());
		ZKFile file = zkscratch.open("files-create-with-nlink-1", File.O_RDONLY);
		int nlink = file.inode.getNlink();
		file.close();
		assertEquals(1, nlink);
	}

	@Test
	public void hardlinksIncreaseNlinkBy1() throws IOException {
		scratch.write("hardlinks-increase-nlink-by-1", "yadda yadda".getBytes());
		scratch.link("hardlinks-increase-nlink-by-1", "hardlinks-increase-nlink-by-1-link");
		ZKFile file = zkscratch.open("hardlinks-increase-nlink-by-1", File.O_RDONLY);
		int nlink = file.inode.getNlink();
		file.close();
		assertEquals(2, nlink);
	}

	@Test
	public void unlinkReducesNlinkBy1() throws IOException {
		scratch.write("hardlinks-increase-nlink-by-1", "yadda yadda".getBytes());
		scratch.link("hardlinks-increase-nlink-by-1", "hardlinks-increase-nlink-by-1-link");
		scratch.unlink("hardlinks-increase-nlink-by-1");
		ZKFile file = zkscratch.open("hardlinks-increase-nlink-by-1-link", File.O_RDONLY);
		int nlink = file.inode.getNlink();
		file.close();
		assertEquals(1, nlink);
	}

	@Test
	public void symlinksDontAffectNlink() throws IOException {
		scratch.write("hardlinks-increase-nlink-by-1", "yadda yadda".getBytes());
		scratch.symlink("hardlinks-increase-nlink-by-1", "hardlinks-increase-nlink-by-1-link");
		ZKFile file = zkscratch.open("hardlinks-increase-nlink-by-1", File.O_RDONLY);
		int nlink = file.inode.getNlink();
		file.close();
		assertEquals(1, nlink);
	}
	
	// TODO: close and write the archive
	
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
