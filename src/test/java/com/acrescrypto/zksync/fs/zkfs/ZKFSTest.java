package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.fs.FSTestBase;
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
	
	// TODO: multipage write test
	// TODO: mutlipage modification test
	// TODO: multipage truncate test
	// TODO: nlink consistency checks 
	
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
