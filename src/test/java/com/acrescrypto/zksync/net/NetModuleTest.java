package com.acrescrypto.zksync.net;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver;
import com.acrescrypto.zksync.utility.Util;

public class NetModuleTest {
	static CryptoSupport crypto;
	
	void addMockData(ZKArchive archive) throws IOException {
		ZKFS fs = archive.openBlank();
		fs.write("file0", crypto.rng(crypto.hashLength()-1));
		fs.write("file1", crypto.rng(archive.getConfig().getPageSize()));
		fs.write("file2", crypto.rng(10*archive.getConfig().getPageSize()));
		fs.commit();
	}
	
	@BeforeClass
	public static void beforeClass() {
		crypto = new CryptoSupport();
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@AfterClass
	public static void afterClass() {
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}

	/** Party A writes data to an archive. B connects and downloads it. */ 
	@Test
	public void testOneWaySync() throws IOException, UnconnectableAdvertisementException {
		Key rootKey = new Key(crypto);
		
		ZKMaster aMaster = ZKMaster.openBlankTestVolume("copy1");
		ArchiveAccessor aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig aConfig = new ZKArchiveConfig(aAccessor, "", ZKArchive.DEFAULT_PAGE_SIZE);
		addMockData(aConfig.getArchive());
		aMaster.listenOnTCP(0);
		aMaster.getTCPListener().advertise(aConfig.getSwarm());
		TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
		
		ZKMaster bMaster = ZKMaster.openBlankTestVolume("copy2");
		ArchiveAccessor bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig bConfig = new ZKArchiveConfig(bAccessor, aConfig.getArchiveId(), false);
		bConfig.getSwarm().addPeerAdvertisement(ad);
		bConfig.finishOpening();
		bConfig.getSwarm().requestAll();
		
		// make sure we get all the pages from A, and we don't somehow end up with unexpected extra pages (yes, this was a real bug)
		Util.waitUntil(2000, ()->aConfig.getArchive().allPageTags().size() == bConfig.getArchive().allPageTags().size());
		assertFalse(Util.waitUntil(50, ()->aConfig.getArchive().allPageTags().size() != bConfig.getArchive().allPageTags().size()));
		assertEquals(aConfig.getArchive().allPageTags().size(), bConfig.getArchive().allPageTags().size());
		
		ZKFS fsa = aConfig.getRevisionTree().plainBranchTips().get(0).getFS();
		ZKFS fsb = bConfig.getRevisionTree().plainBranchTips().get(0).getFS();
		assertArrayEquals(fsa.read("file0"), fsb.read("file0"));
		assertArrayEquals(fsa.read("file1"), fsb.read("file1"));
		assertArrayEquals(fsa.read("file2"), fsb.read("file2"));
		
		fsa.close();
		fsb.close();
		aConfig.getArchive().close();
		bConfig.getArchive().close();
		aMaster.close();
		bMaster.close();
	}
	
	/** Party A writes data to an archive. B connects and downloads it. Then A makes changes, and B automatically receives them. */
	@Test
	public void testOneWaySyncWithUpdates() throws IOException, UnconnectableAdvertisementException, DiffResolutionException {
		Key rootKey = new Key(crypto);
		
		ZKMaster aMaster = ZKMaster.openBlankTestVolume("copy1");
		ArchiveAccessor aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig aConfig = new ZKArchiveConfig(aAccessor, "", ZKArchive.DEFAULT_PAGE_SIZE);
		addMockData(aConfig.getArchive());
		aMaster.listenOnTCP(0);
		aMaster.getTCPListener().advertise(aConfig.getSwarm());
		TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
		
		ZKMaster bMaster = ZKMaster.openBlankTestVolume("copy2");
		ArchiveAccessor bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig bConfig = new ZKArchiveConfig(bAccessor, aConfig.getArchiveId(), false);
		bConfig.getSwarm().addPeerAdvertisement(ad);
		bConfig.finishOpening();
		bConfig.getSwarm().requestAll();
		
		assertTrue(Util.waitUntil(2000, ()->aConfig.getArchive().allPageTags().size() == bConfig.getArchive().allPageTags().size()));
		
		ZKFS fsa = aConfig.getRevisionTree().plainBranchTips().get(0).getFS();
		fsa.write("file3", crypto.rng(2*aConfig.getPageSize()));
		fsa.commit();
		
		assertTrue(Util.waitUntil(1000, ()->bConfig.getRevisionTree().plainBranchTips().contains(aConfig.getRevisionTree().plainBranchTips().get(0))));
		Util.waitUntil(2000, ()->aConfig.getArchive().allPageTags().size() == bConfig.getArchive().allPageTags().size());
		assertEquals(aConfig.getArchive().allPageTags().size(), bConfig.getArchive().allPageTags().size());
		RefTag tag = DiffSetResolver.canonicalMergeResolver(bConfig.getArchive()).resolve();
		
		ZKFS fsb = tag.getFS();
		for(int i = 0; i <= 3; i++) {
			assertArrayEquals(fsa.read("file"+i), fsb.read("file"+i));
		}
		
		fsa.close();
		fsb.close();
		aConfig.getArchive().close();
		bConfig.getArchive().close();
		aMaster.close();
		bMaster.close();
	}
	
	/** Party A writes data to an archive, which party B downloads. B then creates changes, which A receives. */
	@Test
	public void testTwoWaySync() throws IOException, DiffResolutionException, UnconnectableAdvertisementException {
		Key rootKey = new Key(crypto);
		
		ZKMaster aMaster = ZKMaster.openBlankTestVolume("copy1");
		ArchiveAccessor aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig aConfig = new ZKArchiveConfig(aAccessor, "", ZKArchive.DEFAULT_PAGE_SIZE);
		addMockData(aConfig.getArchive());
		aMaster.listenOnTCP(0);
		aMaster.getTCPListener().advertise(aConfig.getSwarm());
		TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
		
		ZKMaster bMaster = ZKMaster.openBlankTestVolume("copy2");
		ArchiveAccessor bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig bConfig = new ZKArchiveConfig(bAccessor, aConfig.getArchiveId(), false);
		bConfig.getSwarm().addPeerAdvertisement(ad);
		bConfig.finishOpening();
		bConfig.getSwarm().requestAll();
		aConfig.getSwarm().requestAll();
		
		assertTrue(Util.waitUntil(2000, ()->aConfig.getArchive().allPageTags().size() == bConfig.getArchive().allPageTags().size()));
		
		ZKFS fsb = bConfig.getRevisionTree().plainBranchTips().get(0).getFS();
		fsb.write("file3", crypto.rng(2*bConfig.getPageSize()));
		fsb.commit();
		
		assertTrue(Util.waitUntil(1000, ()->aConfig.getRevisionTree().plainBranchTips().contains(bConfig.getRevisionTree().plainBranchTips().get(0))));
		Util.waitUntil(2000, ()->aConfig.getArchive().allPageTags().size() == bConfig.getArchive().allPageTags().size());
		assertEquals(aConfig.getArchive().allPageTags().size(), bConfig.getArchive().allPageTags().size());
		RefTag tag = DiffSetResolver.canonicalMergeResolver(bConfig.getArchive()).resolve();
		
		ZKFS fsa = tag.getFS();
		for(int i = 0; i <= 3; i++) {
			assertArrayEquals(fsb.read("file"+i), fsa.read("file"+i));
		}
		
		fsa.close();
		fsb.close();
		aConfig.getArchive().close();
		bConfig.getArchive().close();
		aMaster.close();
		bMaster.close();
	}
	
	/** Party A writes multiple revisions. Party B requests just one of them. Party B should not have pages not related to the requested revtag. */
	@Test
	public void testRevTagSync() throws IOException, UnconnectableAdvertisementException, DiffResolutionException {
		Key rootKey = new Key(crypto);
		
		ZKMaster aMaster = ZKMaster.openBlankTestVolume("copy1");
		ArchiveAccessor aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig aConfig = new ZKArchiveConfig(aAccessor, "", ZKArchive.DEFAULT_PAGE_SIZE);
		ZKFS fsa = aConfig.getArchive().openBlank();
		
		fsa.write("path", crypto.rng(2*aConfig.getPageSize()));
		fsa.commit();
		
		fsa.write("path", crypto.rng(2*aConfig.getPageSize()));
		fsa.commit();
		
		aMaster.listenOnTCP(0);
		aMaster.getTCPListener().advertise(aConfig.getSwarm());
		TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
		
		ZKMaster bMaster = ZKMaster.openBlankTestVolume("copy2");
		ArchiveAccessor bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig bConfig = new ZKArchiveConfig(bAccessor, aConfig.getArchiveId(), false);
		bConfig.getSwarm().addPeerAdvertisement(ad);
		bConfig.finishOpening();
		bConfig.getSwarm().requestRevision(0, fsa.getBaseRevision());
		
		// we should get a config (1 page), inode table (1 page), revinfo (1 page), the file (2 pages) and its pagetree (1 page). root is literal so no page.
		assertTrue(Util.waitUntil(1000, ()->bConfig.getRevisionTree().plainBranchTips().size() == 1));
		Util.waitUntil(2000, ()->bConfig.getArchive().allPageTags().size() == 5);
		assertEquals(5, bConfig.getArchive().allPageTags().size());
				
		ZKFS fsb = bConfig.getRevisionTree().plainBranchTips().get(0).getFS();
		assertArrayEquals(fsb.read("path"), fsa.read("path"));
		
		fsa.close();
		fsb.close();
		aConfig.getArchive().close();
		bConfig.getArchive().close();
		aMaster.close();
		bMaster.close();
	}
	
	/** Party A writes multiple inodes. Party B requests a single inode. Party B should not have pages unrelated to the requested inode. */
	@Test
	public void testInodeSync() throws IOException, UnconnectableAdvertisementException {
		Key rootKey = new Key(crypto);
		
		ZKMaster aMaster = ZKMaster.openBlankTestVolume("copy1");
		ArchiveAccessor aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig aConfig = new ZKArchiveConfig(aAccessor, "", ZKArchive.DEFAULT_PAGE_SIZE);
		ZKFS fsa = aConfig.getArchive().openBlank();
		
		for(int i = 0; i < 10; i++) {
			fsa.write("path"+i, crypto.rng(2*aConfig.getPageSize()));
		}
		
		fsa.commit();
		Inode inode = fsa.inodeForPath("path0");
		
		aMaster.listenOnTCP(0);
		aMaster.getTCPListener().advertise(aConfig.getSwarm());
		TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
		
		ZKMaster bMaster = ZKMaster.openBlankTestVolume("copy2");
		ArchiveAccessor bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig bConfig = new ZKArchiveConfig(bAccessor, aConfig.getArchiveId(), false);
		bConfig.getSwarm().addPeerAdvertisement(ad);
		bConfig.finishOpening();

		assertTrue(Util.waitUntil(1000, ()->bConfig.getRevisionTree().plainBranchTips().size() == 1));
		bConfig.getSwarm().requestInode(0, bConfig.getRevisionTree().plainBranchTips().get(0), inode.getStat().getInodeId());
		
		// we should get a config (1 page), pagetree (1), and file pages (numPages from reftag)
		Util.waitUntil(2000, ()->bConfig.getArchive().allPageTags().size() == 2 + inode.getRefTag().getNumPages());
		assertEquals(2 + inode.getRefTag().getNumPages(), bConfig.getArchive().allPageTags().size());
				
		fsa.close();
		aConfig.getArchive().close();
		bConfig.getArchive().close();
		aMaster.close();
		bMaster.close();
	}

	/** Party A writes a file with multiple pages. Party B requests a single page. Party B should not have pages unrelated to the requested page, and the archive config. */
	@Test
	public void testPageSync() throws IOException, UnconnectableAdvertisementException {
		Key rootKey = new Key(crypto);
		
		ZKMaster aMaster = ZKMaster.openBlankTestVolume("copy1");
		ArchiveAccessor aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig aConfig = new ZKArchiveConfig(aAccessor, "", ZKArchive.DEFAULT_PAGE_SIZE);
		ZKFS fsa = aConfig.getArchive().openBlank();
		
		fsa.write("path", crypto.rng(5*aConfig.getPageSize()));
		fsa.commit();
		byte[] requestedTag = new PageTree(fsa.inodeForPath("path")).getPageTag(0);
		
		aMaster.listenOnTCP(0);
		aMaster.getTCPListener().advertise(aConfig.getSwarm());
		TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
		
		ZKMaster bMaster = ZKMaster.openBlankTestVolume("copy2");
		ArchiveAccessor bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig bConfig = new ZKArchiveConfig(bAccessor, aConfig.getArchiveId(), false);
		bConfig.getSwarm().addPeerAdvertisement(ad);
		bConfig.finishOpening();

		assertTrue(Util.waitUntil(1000, ()->bConfig.getRevisionTree().plainBranchTips().size() == 1));
		bConfig.getSwarm().requestTag(0, requestedTag);
		
		// we should get the requested page and the config file
		Util.waitUntil(2000, ()->bConfig.getArchive().allPageTags().size() == 2);
		assertEquals(2, bConfig.getArchive().allPageTags().size());
		
		for(byte[] pageTag : bConfig.getArchive().allPageTags()) {
			if(Arrays.equals(bConfig.tag(), pageTag)) continue;
			if(Arrays.equals(requestedTag, pageTag)) continue;
			fail();
		}
				
		fsa.close();
		aConfig.getArchive().close();
		bConfig.getArchive().close();
		aMaster.close();
		bMaster.close();
	}
}
