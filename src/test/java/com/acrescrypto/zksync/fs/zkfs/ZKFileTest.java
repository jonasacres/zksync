package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.Security;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.fs.FileTestBase;
import com.acrescrypto.zksync.fs.localfs.LocalFS;

public class ZKFileTest extends FileTestBase {
	ZKFS zkscratch;
	
	@Before
	public void beforeEach() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		ZKFSTest.deleteFiles();
		LocalFS storage = new LocalFS(ZKFSTest.SCRATCH_DIR);
		java.io.File scratchDir = new java.io.File(ZKFSTest.SCRATCH_DIR);
		try {
			FileUtils.deleteDirectory(scratchDir);
		} catch (IOException e) {}
		(new java.io.File(ZKFSTest.SCRATCH_DIR)).mkdirs();

		scratch = zkscratch = new ZKFS(storage, "zksync".toCharArray());
	}
	
	@After
	public void afterEach() {
		ZKFSTest.restoreArgon2Costs();
	}

	@BeforeClass
	public static void beforeClass() {
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@Test
	public void testMultipageWrite() throws IOException {
		byte[] contents = new byte[5*zkscratch.getPrivConfig().getPageSize()];
		for(int i = 0; i < contents.length; i++) contents[i] = (byte) (i & 0xff);
		ZKFile file = zkscratch.open("multipage-write-test", ZKFile.O_CREAT|ZKFile.O_RDWR);
		file.write(contents);
		file.close();
		
		file = zkscratch.open("multipage-write-test", ZKFile.O_RDONLY);
		assertTrue(Arrays.equals(file.read(), contents));
	}
}
