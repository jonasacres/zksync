package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.acrescrypto.zksync.fs.DirectoryTestBase;

public class ZKDirectoryTest extends DirectoryTestBase {
	ZKFS zkscratch;
	ZKMaster master;
	
	@Before
	public void beforeEach() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		master = ZKMaster.openAtPath((String desc) -> { return "zksync".getBytes(); }, ZKFSTest.SCRATCH_DIR);
		scratch = zkscratch = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").openBlank();
	}
	
	@After
	public void afterEach() throws IOException {
		ZKFSTest.restoreArgon2Costs();
		master.purge();
	}

	@BeforeClass
	public static void beforeClass() {
		Security.addProvider(new BouncyCastleProvider());
	}
	
	// TODO: test that invalid directories cause an appropriate exception
}
