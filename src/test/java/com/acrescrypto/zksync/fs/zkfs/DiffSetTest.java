package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.fs.localfs.LocalFS;

public class DiffSetTest {
	LocalFS storage;
	Revision parent;
	Revision[] children;
	char[] password = "zksync".toCharArray();
	
	public final static int NUM_CHILDREN = 8;
	
	@BeforeClass
	public static void beforeClass() {
		ZKFSTest.cheapenArgon2Costs();
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@AfterClass
	public static void afterClass() {
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		storage = new LocalFS("/tmp/zksync-diffset");
		if(storage.equals("/")) storage.rmrf("/");
		ZKFS fs = new ZKFS(storage, password);
		fs.write("unmodified", "parent".getBytes());
		fs.write("modified", "replaceme".getBytes());
		parent = fs.commit();
		children = new Revision[NUM_CHILDREN];
		
		for(int i = 0; i < NUM_CHILDREN; i++) {
			fs = new ZKFS(storage, password, parent);
			fs.write("modified", "replaced!".getBytes());
			fs.write("child", ("text " + i).getBytes());
			children[i] = fs.commit();
		}
	}
	
	@Test
	public void testDetectsDifferencesBetweenSiblings() throws IOException {
		Revision[] list = new Revision[] { children[0], children[1] };
		DiffSet diffset = new DiffSet(list);
		ArrayList<FileDiff> diffs = diffset.getDiffs();
		assertEquals(2, diffs.size());
	}
	
	// TODO: a parent and a child should show differences
	// TODO: two siblings should show differences when they exist
	// TODO: two siblings should not show differences when files are common
	// TODO: isResolved reflects if all diffs are resolved
	
	// TODO: applyResolution should create a new revision that is descended from all its parents
	// TODO: applyResolution should be deterministic
	
}
