package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.fs.localfs.LocalFS;

public class RevisionTreeTest {
	public final static int NUM_REVISIONS = 31;
	ZKFS fs;
	RevisionTree tree;
	
	static boolean initialized;
	
	@Before
	public void beforeEach() throws IOException {
		LocalFS storage = new LocalFS("/tmp/revision-tree-test");
		if(!initialized) {
			initialized = true;
			ZKFSTest.cheapenArgon2Costs();
			Security.addProvider(new BouncyCastleProvider());

			storage.rmrf("/");
			fs = new ZKFS(storage, "zksync".toCharArray());

			Revision[] revisions = new Revision[NUM_REVISIONS];
			
			for(int i = 0; i < NUM_REVISIONS; i++) {
				Revision parent = null;
				if(i > 0) parent = revisions[(i-1)/2]; 
				ZKFS revFs = new ZKFS(fs.getStorage(), "zksync".toCharArray(), parent);
				revFs.write("intensive-revisions", ("Version " + i).getBytes());
				revisions[i] = revFs.commit();
			}
		} else {
			fs = new ZKFS(storage, "zksync".toCharArray());
		}
		
		tree = fs.getRevisionTree();
		tree.scan();
	}
	
	@AfterClass
	public static void afterClass() {
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Test
	public void testRevisionCount() {
		assertEquals(NUM_REVISIONS, tree.size());
	}
	
	@Test
	public void testRevisionList() {
		ArrayList<RevisionTag> list = tree.revisionTags();
		assertEquals(NUM_REVISIONS, list.size());
		for(RevisionTag tag : list) assertTrue(fs.storage.exists(tag.getPath()));
	}
	
	// TODO: test descendents
}
