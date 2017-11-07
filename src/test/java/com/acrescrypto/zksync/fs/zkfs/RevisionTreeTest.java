package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;
import java.util.HashSet;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.fs.localfs.LocalFS;

public class RevisionTreeTest {
	public final static int NUM_REVISIONS = 60;
	public final static int NUM_ROOTS = 4;
	static ZKFS fs, mfs;
	
	static RevisionTree tree, mtree;
	static Revision[] revisions, mrevisions;
	static LocalFS storage, mstorage;
	
	@BeforeClass
	public static void beforeClass() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		Security.addProvider(new BouncyCastleProvider());
		setupSingleParentTest();
		setupMultipleParentTest();
	}
	
	public static void setupSingleParentTest() throws IOException {
		storage = new LocalFS("/tmp/revision-tree-test");
		storage.rmrf("/");
		fs = new ZKFS(storage, "zksync".toCharArray());

		revisions = new Revision[NUM_REVISIONS];
		
		for(int i = 0; i < NUM_REVISIONS; i++) {
			Revision parent = null;
			if(i >= NUM_ROOTS) {
				parent = revisions[parentIndex(i)];
			}
			
			ZKFS revFs = new ZKFS(fs.getStorage(), "zksync".toCharArray(), parent);
			revFs.write("intensive-revisions", ("Version " + i).getBytes());
			revFs.inodeTable.inode.getStat().setMtime(i*1000l*1000l*1000l*60l); // each rev is 1 minute later
			revisions[i] = revFs.commit();
		}
		
		tree = fs.getRevisionTree();
	}
	
	public static void setupMultipleParentTest() throws IOException {
		mstorage = new LocalFS("/tmp/revision-tree-test-multipleparents");
		if(mstorage.exists("/")) mstorage.rmrf("/");
		mfs = new ZKFS(mstorage, "zksync".toCharArray());
		
		// 0 -> 1 -> 2 -> 3 -> ... -> n-3
		//  \-> n-2 ->  --\-> n-1 (n-1 is child of 2 and n-2, but not 3 ... n-3
		
		mrevisions = new Revision[8];
		
		for(int i = 0; i < mrevisions.length; i++) {
			Revision parent = null;
			if(i == 0) parent = null;
			else if(i == mrevisions.length-2) parent = mrevisions[0];
			else parent = mrevisions[i-1];
			
			ZKFS revFs = new ZKFS(mstorage, "zksync".toCharArray(), parent);
			revFs.write("multiple-ancestors", ("version " + i).getBytes());
			revFs.inodeTable.inode.getStat().setMtime(i*1000l*1000l*1000l*60l); // each rev is 1 minute later
			if(i == mrevisions.length-1) {
				mrevisions[i] = revFs.commit(new RevisionTag[] { mrevisions[2].tag }, null);
			} else {
				mrevisions[i] = revFs.commit();
			}
		}
		
		mtree = mfs.getRevisionTree();
	}
	
	public static int parentIndex(int childIndex) {
		if(childIndex < NUM_ROOTS) return -1;
		int tier = (int) (Math.floor(Math.log(childIndex+NUM_ROOTS)/Math.log(2)) - Math.log(NUM_ROOTS)/Math.log(2));
		int offset = (int) (childIndex - Math.pow(2, tier+2) + NUM_ROOTS);
		int pIndex = (int) (Math.pow(2, tier+1) - NUM_ROOTS) + offset/2;
		return pIndex; 
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
	
	@Test
	public void testAncestorsOf() throws IOException {
		for(int i = 0; i < revisions.length; i++) {
			HashSet<RevisionTag> seenTags = new HashSet<RevisionTag>();
			seenTags.add(RevisionTag.nullTag(fs));
			for(int j = i; j >= 0; j = parentIndex(j)) seenTags.add(revisions[j].tag);
			HashSet<RevisionTag> ancestors = tree.ancestorsOf(revisions[i].tag);
			assertEquals(seenTags.size(), ancestors.size());
			for(RevisionTag tag : ancestors) assertTrue(seenTags.contains(tag));
			for(RevisionTag tag : seenTags) assertTrue(ancestors.contains(tag));
		}
	}
	
	@Test
	public void testAncestorsOfWithMultipleParents() throws IOException {
		HashSet<RevisionTag> ancestors = mtree.ancestorsOf(mrevisions[mrevisions.length-1].tag);
		
		HashSet<RevisionTag> expected = new HashSet<RevisionTag>();
		expected.add(RevisionTag.nullTag(mfs));
		for(int i = 0; i < mrevisions.length; i++) {
			if(i <= 2 || i >= mrevisions.length-2) expected.add(mrevisions[i].tag);
		}
		
		assertEquals(expected.size(), ancestors.size());
		for(RevisionTag tag : ancestors) assertTrue(expected.contains(tag));
		for(RevisionTag tag : expected) assertTrue(ancestors.contains(tag));
	}
	
	@Test
	public void testCommonAncestorSiblings() throws IOException {
		RevisionTag ancestor = tree.commonAncestorOf(new RevisionTag[] { revisions[8-NUM_ROOTS].tag, revisions[8-NUM_ROOTS+1].tag });
		assertTrue(revisions[4-NUM_ROOTS].tag.equals(ancestor));
	}
	
	@Test
	public void testCommonAncestorParentChild() throws IOException {
		RevisionTag ancestor = tree.commonAncestorOf(new RevisionTag[] { revisions[NUM_ROOTS].tag, revisions[0].tag });
		assertTrue(revisions[0].tag.equals(ancestor));
	}
	
	@Test
	public void testCommonAncestorCousins() throws IOException {
		RevisionTag ancestor = tree.commonAncestorOf(new RevisionTag[] { revisions[NUM_ROOTS+8].tag, revisions[NUM_ROOTS+10].tag });
		assertTrue(revisions[0].tag.equals(ancestor));
	}
	
	@Test
	public void testCommonAncestorCousinsOnceRemoved() throws IOException {
		RevisionTag ancestor = tree.commonAncestorOf(new RevisionTag[] { revisions[NUM_ROOTS+8].tag, revisions[NUM_ROOTS+1].tag });
		assertTrue(revisions[0].tag.equals(ancestor));
	}
	
	@Test
	public void testCommonAncestorRoots() throws IOException {
		RevisionTag ancestor = tree.commonAncestorOf(new RevisionTag[] { revisions[0].tag, revisions[1].tag });
		assertTrue(ancestor.equals(RevisionTag.nullTag(ancestor.fs)));
	}
	
	@Test
	public void testParentOf() throws IOException {
		for(int i = 0; i < revisions.length; i++) {
			RevisionTag tag = tree.parentOf(revisions[i].tag);
			RevisionTag expected;
			if(i < NUM_ROOTS) {
				expected = RevisionTag.nullTag(fs);
			} else {
				expected = revisions[parentIndex(i)].tag;
			}
			
			assertEquals(expected, tag);
		}
	}

	@Test
	public void testParentOfWithMultipleParents() throws IOException {
		RevisionTag parent = mtree.parentOf(mrevisions[mrevisions.length-1].tag);
		assertEquals(mrevisions[mrevisions.length-2].tag, parent);
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
