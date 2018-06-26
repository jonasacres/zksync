package com.acrescrypto.zksync.net;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
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
	
	@Test
	public void testOneWaySync() throws IOException, UnconnectableAdvertisementException {
		Key rootKey = new Key(crypto);
		
		ZKMaster aMaster = ZKMaster.openBlankTestVolume();
		ArchiveAccessor aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig aConfig = new ZKArchiveConfig(aAccessor, "", ZKArchive.DEFAULT_PAGE_SIZE);
		addMockData(aConfig.getArchive());
		aMaster.listenOnTCP(0);
		aMaster.getTCPListener().advertise(aConfig.getSwarm());
		TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
		
		ZKMaster bMaster = ZKMaster.openBlankTestVolume();
		ArchiveAccessor bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
		ZKArchiveConfig bConfig = new ZKArchiveConfig(bAccessor, aConfig.getArchiveId(), false);
		bConfig.getSwarm().addPeerAdvertisement(ad);
		bConfig.finishOpening();
		bConfig.getSwarm().requestAll();
		
		Util.waitUntil(2000, ()->aConfig.getArchive().allPageTags().size() == bConfig.getArchive().allPageTags().size());
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
}
