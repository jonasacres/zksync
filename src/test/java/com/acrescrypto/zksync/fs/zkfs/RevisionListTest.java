package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.TestUtils;

public class RevisionListTest {
	public final static int NUM_REVISIONS = 60;
	public final static int NUM_ROOTS = 4;
	static ZKFS fs, mfs;
	
	static RevisionList list, mlist;
	static RevisionTag[] revisions, mrevisions;
	static ZKMaster singlemaster, multimaster;
	
	@BeforeClass	
	public static void beforeClass() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		Security.addProvider(new BouncyCastleProvider());
		setupSingleParentTest();
		setupMultipleParentTest();
	}
	
	@AfterClass
	public static void afterAll() throws IOException {
		fs.archive.close();
		mfs.archive.close();
		fs.close();
		mfs.close();
		singlemaster.close();
		multimaster.close();
		TestUtils.assertTidy();
	}
	
	public static void setupSingleParentTest() throws IOException {
		singlemaster = ZKMaster.openTestVolume((String desc) -> { return "zksync".getBytes(); }, "/tmp/zksync-test/revision-list-test-single-parent");
		singlemaster.purge();
		fs = singlemaster.createArchive(65536, "singlemaster").openBlank();

		revisions = new RevisionTag[NUM_REVISIONS];
		
		for(int i = 0; i < NUM_REVISIONS; i++) {
			RevisionTag parent = null;
			if(i >= NUM_ROOTS) {
				parent = revisions[parentIndex(i)];
			}
			
			ZKFS revFs = parent != null ? parent.getFS() : fs.archive.openBlank();
			revisions[i] = revFs.commit();
		}
		
		list = fs.archive.config.getRevisionList();
	}
	
	public static void setupMultipleParentTest() throws IOException {
		multimaster = ZKMaster.openTestVolume((String desc) -> { return "zksync".getBytes(); }, "/tmp/zksync-test/revision-list-test-multi-parent");
		multimaster.purge();
		mfs = multimaster.createArchive(65536, "multimaster").openBlank();
		
		// 0 -> 1 -> 2 -> 3 -> ... -> n-3
		//  \-> n-2 ->  --\-> n-1 (n-1 is child of 2 and n-2, but not 3 ... n-3
		
		mrevisions = new RevisionTag[8];
		
		for(int i = 0; i < mrevisions.length; i++) {
			RevisionTag parent = null;
			if(i == 0) parent = null;
			else if(i == mrevisions.length-2) parent = mrevisions[0];
			else parent = mrevisions[i-1];
			
			ZKFS revFs = parent != null ? parent.getFS() : mfs;
			if(i == mrevisions.length-1) {
				mrevisions[i] = revFs.commit(new RevisionTag[] { mrevisions[2] });
			} else {
				mrevisions[i] = revFs.commit();
			}
		}
		
		mlist = mfs.archive.config.getRevisionList();
	}
	
	public static int parentIndex(int childIndex) {
		if(childIndex < NUM_ROOTS) return -1;
		return (childIndex - NUM_ROOTS) >> 1;
	}
	
	@AfterClass
	public static void afterClass() {
		ZKFSTest.restoreArgon2Costs();
	}
	
	// TODO DHT: (test) Write new tests for RevisionList. These ones are broken, incomprehensive and I don't even remember how they're supposed to work.
	@Test @Ignore
	public void testRevisionCount() {
		assertEquals((int) (Math.floor(0.5*(NUM_REVISIONS+1))+2), list.branchTips().size());
		assertEquals(2, mlist.branchTips().size());
	}
	
	@Test @Ignore
	public void testBranchTips() {
		int count = list.branchTips().size();
		
		// this is probably not gonna work if we don't choose NUM_ROOTS/NUM_REVISIONS to get a full list, but who cares
		int tier = (int) (Math.log(NUM_REVISIONS+NUM_ROOTS-1)/Math.log(2) - Math.log(NUM_ROOTS)/Math.log(2));
		int tierBase = (int) (Math.pow(2, tier+2) - NUM_ROOTS);
		assertEquals(NUM_REVISIONS - tierBase, count);
	}	
}
