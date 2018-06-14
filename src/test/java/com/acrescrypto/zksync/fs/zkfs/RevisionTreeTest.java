package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.HashSet;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

public class RevisionTreeTest {
	public final static int NUM_REVISIONS = 60;
	public final static int NUM_ROOTS = 4;
	static ZKFS fs, mfs;
	
	static RevisionTree tree, mtree;
	static RefTag[] revisions, mrevisions;
	static ZKMaster singlemaster, multimaster;
	
	@BeforeClass
	
	public static void beforeClass() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		Security.addProvider(new BouncyCastleProvider());
		setupSingleParentTest();
		setupMultipleParentTest();
	}
	
	public static void setupSingleParentTest() throws IOException {
		singlemaster = ZKMaster.openTestVolume((String desc) -> { return "zksync".getBytes(); }, "/tmp/zksync-test/revision-tree-test-single-parent");
		singlemaster.purge();
		fs = singlemaster.createArchive(65536, "singlemaster").openBlank();

		revisions = new RefTag[NUM_REVISIONS];
		
		for(int i = 0; i < NUM_REVISIONS; i++) {
			RefTag parent = null;
			if(i >= NUM_ROOTS) {
				parent = revisions[parentIndex(i)];
			}
			
			ZKFS revFs = parent != null ? parent.getFS() : RefTag.blank(fs.archive).getFS();
			revisions[i] = revFs.commit();
		}
		
		tree = fs.archive.config.getRevisionTree();
	}
	
	public static void setupMultipleParentTest() throws IOException {
		multimaster = ZKMaster.openTestVolume((String desc) -> { return "zksync".getBytes(); }, "/tmp/zksync-test/revision-tree-test-multi-parent");
		multimaster.purge();
		mfs = multimaster.createArchive(65536, "multimaster").openBlank();
		
		// 0 -> 1 -> 2 -> 3 -> ... -> n-3
		//  \-> n-2 ->  --\-> n-1 (n-1 is child of 2 and n-2, but not 3 ... n-3
		
		mrevisions = new RefTag[8];
		
		for(int i = 0; i < mrevisions.length; i++) {
			RefTag parent = null;
			if(i == 0) parent = null;
			else if(i == mrevisions.length-2) parent = mrevisions[0];
			else parent = mrevisions[i-1];
			
			ZKFS revFs = parent != null ? parent.getFS() : mfs;
			if(i == mrevisions.length-1) {
				mrevisions[i] = revFs.commit(new RefTag[] { mrevisions[2] });
			} else {
				mrevisions[i] = revFs.commit();
			}
		}
		
		mtree = mfs.archive.config.getRevisionTree();
	}
	
	public static int parentIndex(int childIndex) {
		if(childIndex < NUM_ROOTS) return -1;
		return (childIndex - NUM_ROOTS) >> 1;
	}
	
	@AfterClass
	public static void afterClass() {
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Test
	public void testRevisionCount() {
		assertEquals((int) (Math.floor(0.5*(NUM_REVISIONS+1))+2), tree.branchTips().size());
		assertEquals(2, mtree.branchTips().size());
	}
	
	@Ignore
	public void testDefaultRevision() {
	}
	
	@Test
	public void testBranchTips() {
		int count = tree.branchTips().size();
		
		// this is probably not gonna work if we don't choose NUM_ROOTS/NUM_REVISIONS to get a full tree, but who cares
		int tier = (int) (Math.log(NUM_REVISIONS+NUM_ROOTS-1)/Math.log(2) - Math.log(NUM_ROOTS)/Math.log(2));
		int tierBase = (int) (Math.pow(2, tier+2) - NUM_ROOTS);
		assertEquals(NUM_REVISIONS - tierBase, count);
	}
	
	@Test
	public void testAncestorsOf() throws IOException {
		for(int i = 0; i < revisions.length; i++) {
			HashSet<RefTag> seenTags = new HashSet<RefTag>();
			seenTags.add(RefTag.blank(fs.archive));
			for(int j = i; j >= 0; j = parentIndex(j)) seenTags.add(revisions[j]);
			HashSet<RefTag> ancestors = tree.ancestorsOf(revisions[i]);
			assertEquals(seenTags.size(), ancestors.size());
			for(RefTag tag : ancestors) assertTrue(seenTags.contains(tag));
			for(RefTag tag : seenTags) assertTrue(ancestors.contains(tag));
		}
	}
	
	@Test
	public void testAncestorsOfWithMultipleParents() throws IOException {
		HashSet<RefTag> ancestors = mtree.ancestorsOf(mrevisions[mrevisions.length-1]);
		
		HashSet<RefTag> expected = new HashSet<RefTag>();
		expected.add(RefTag.blank(mfs.archive));
		for(int i = 0; i < mrevisions.length; i++) {
			if(i <= 2 || i >= mrevisions.length-2) expected.add(mrevisions[i]);
		}
		
		assertEquals(expected.size(), ancestors.size());
		for(RefTag tag : ancestors) assertTrue(expected.contains(tag));
		for(RefTag tag : expected) assertTrue(ancestors.contains(tag));
	}
	
	@Test
	public void testCommonAncestorSiblings() throws IOException {
		RefTag ancestor = tree.commonAncestorOf(new RefTag[] { revisions[8-NUM_ROOTS], revisions[8-NUM_ROOTS+1] });
		assertTrue(revisions[4-NUM_ROOTS].equals(ancestor));
	}
	
	@Test
	public void testCommonAncestorParentChild() throws IOException {
		RefTag ancestor = tree.commonAncestorOf(new RefTag[] { revisions[NUM_ROOTS], revisions[0] });
		assertTrue(revisions[0].equals(ancestor));
	}
	
	@Test
	public void testCommonAncestorCousins() throws IOException {
		RefTag ancestor = tree.commonAncestorOf(new RefTag[] { revisions[NUM_ROOTS+8], revisions[NUM_ROOTS+10] });
		assertTrue(revisions[0].equals(ancestor));
	}
	
	@Test
	public void testCommonAncestorCousinsOnceRemoved() throws IOException {
		RefTag ancestor = tree.commonAncestorOf(new RefTag[] { revisions[NUM_ROOTS+8], revisions[NUM_ROOTS+1] });
		assertTrue(revisions[0].equals(ancestor));
	}
	
	@Test
	public void testCommonAncestorRoots() throws IOException {
		RefTag ancestor = tree.commonAncestorOf(new RefTag[] { revisions[0], revisions[1] });
		assertEquals(RefTag.blank(fs.archive), ancestor);
	}
	
}
