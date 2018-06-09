package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
		fs.write("file2", crypto.rng(2*archive.getConfig().getPageSize()));
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
		bConfig.getSwarm().requestAll();
		bConfig.finishOpening();
		System.out.println("a: " + aConfig.getArchive().allPageTags().size());
		System.out.println("b: " + bConfig.getArchive().allPageTags().size());
		assertTrue(Util.waitUntil(10000, ()->aConfig.getArchive().allPageTags().size() == bConfig.getArchive().allPageTags().size()));
	}
}
