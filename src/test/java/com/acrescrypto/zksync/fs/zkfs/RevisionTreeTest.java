package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.fs.localfs.LocalFS;

public class RevisionTreeTest {
	public final static int NUM_REVISIONS = 60;
	public final static int NUM_ROOTS = 4;
	ZKFS fs;
	RevisionTree tree;
	
	static boolean initialized;
	static Revision[] revisions;
	
	@Before
	public void beforeEach() throws IOException {
		LocalFS storage = new LocalFS("/tmp/revision-tree-test");
		if(!initialized) {
			initialized = true;
			ZKFSTest.cheapenArgon2Costs();
			Security.addProvider(new BouncyCastleProvider());

			storage.rmrf("/");
			fs = new ZKFS(storage, "zksync".toCharArray());

			revisions = new Revision[NUM_REVISIONS];
			
			for(int i = 0; i < NUM_REVISIONS; i++) {
				Revision parent = null;
				if(i >= NUM_ROOTS) {
					int tier = (int) (Math.floor(Math.log(i+NUM_ROOTS)/Math.log(2)) - Math.log(NUM_ROOTS)/Math.log(2));
					int offset = (int) (i - Math.pow(2, tier+2) + NUM_ROOTS);
					int pIndex = (int) (Math.pow(2, tier+1) - NUM_ROOTS) + offset/2;
					parent = revisions[pIndex]; 
				}
				ZKFS revFs = new ZKFS(fs.getStorage(), "zksync".toCharArray(), parent);
				revFs.write("intensive-revisions", ("Version " + i).getBytes());
				revFs.inodeTable.inode.getStat().setMtime(i*1000l*1000l*1000l*60l); // each rev is 1 minute later
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
	
	@Test
	public void testImmediateDescendants() {
		int count = 0;
		int rootsFound = 0;
		
		for(RevisionTag tag : tree.revisionTags()) {
			if(tag.parentShortTag == 0) rootsFound++;
			
			for(RevisionTag child : tree.descendantsOf(tag)) {
				count++;
				assertEquals(child.parentShortTag, tag.getShortTag());
			}
		}
		
		assertEquals(tree.size, count + rootsFound);
		assertEquals(NUM_ROOTS, rootsFound);
	}
	
	@Test
	public void testFirstDescendants() {
		// test the following things about first descendant aka d0
		// d0 == null <=> tag has no children
		// d0's parent is tag
		// d0's timestamp <= all other children of tag
		for(RevisionTag tag : tree.revisionTags()) {
			RevisionTag first = tree.firstDescendantOf(tag);
			
			if(first == null) {
				assertEquals(0, tree.descendantsOf(tag).size());
				assertNotEquals(0, tag.getParentShortTag());
			} else {
				assertEquals(tag.getShortTag(), first.getParentShortTag());
				for(RevisionTag child : tree.descendantsOf(tag)) {
					assertTrue(first.timestamp <= child.timestamp);
				}
			}
		}
	}
	
	@Test
	public void testHeir() {
		for(RevisionTag tag : tree.revisionTags()) {
			testHeirOfRevision(tag);
		}
	}
	
	@Test
	public void testRootRevisions() {
		int count = 0;
		for(RevisionTag root : tree.rootRevisions()) {
			count++;
			assertEquals(0l, root.getParentShortTag());
		}
		
		assertEquals(NUM_ROOTS, count);
	}
	
	@Test
	public void testEarliestRoot() {
		RevisionTag earliest = tree.earliestRoot();
		assertEquals(0l, earliest.getParentShortTag());
		for(RevisionTag root : tree.rootRevisions()) {
			assertTrue(earliest.timestamp <= root.timestamp);
		}
	}
	
	@Test
	public void testDefaultRevision() {
		assertEquals(tree.heirOf(tree.earliestRoot()), tree.defaultRevision());
	}
	
	@Test
	public void testLeaves() {
		int count = 0;
		for(RevisionTag leaf : tree.leaves()) {
			count++;
			assertEquals(0, tree.descendantsOf(leaf).size());
		}
		
		// this is probably not gonna work if we don't choose NUM_ROOTS/NUM_REVISIONS to get a full tree, but who cares
		int tier = (int) (Math.log(NUM_REVISIONS+NUM_ROOTS-1)/Math.log(2) - Math.log(NUM_ROOTS)/Math.log(2));
		int tierBase = (int) (Math.pow(2, tier+2) - NUM_ROOTS);
		assertEquals(NUM_REVISIONS - tierBase, count);
	}
	
	private void testHeirOfRevision(RevisionTag tag) {
		RevisionTag expected = tag, next = tree.firstDescendantOf(expected);
		while(next != null) {
			expected = next;
			next = tree.firstDescendantOf(expected);
		}
		
		assertEquals(expected, tree.heirOf(tag));
	}
}
