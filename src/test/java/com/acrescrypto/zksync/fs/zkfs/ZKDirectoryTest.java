package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.security.Security;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.acrescrypto.zksync.fs.DirectoryTestBase;
import com.acrescrypto.zksync.fs.localfs.LocalFS;

public class ZKDirectoryTest extends DirectoryTestBase {
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
}
