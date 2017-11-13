package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.fs.localfs.LocalFS;

public class RevisionTagTest {
	ZKFS fs;
	RefTag rev, parent, tag;
	
	@Before
	public void beforeEach() throws IOException {
		if(fs != null) return;
		
		ZKFSTest.cheapenArgon2Costs();
		Security.addProvider(new BouncyCastleProvider());
		fs = ZKFS.fsForStorage(new LocalFS("/tmp/revision-tag-test"), "zksync".toCharArray());
		fs.getInodeTable().getInode().getStat().setUser("zksync");
		parent = fs.commit();
		
		fs = parent.getFS();
		tag = rev = fs.commit();
	}
	
	@AfterClass
	public static void afterClass() {
		ZKFSTest.restoreArgon2Costs();
	}

	@Test
	public void testTagFormat() {
		assertTrue(true);
	}
}
