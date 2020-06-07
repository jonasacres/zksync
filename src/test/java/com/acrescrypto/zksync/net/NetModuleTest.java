package com.acrescrypto.zksync.net;

import static org.junit.Assert.*;

import java.io.IOException;

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
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.StorageTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
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
		fs.commitAndClose();
	}
	
	@BeforeClass
	public static void beforeClass() {
		crypto = CryptoSupport.defaultCrypto();
		TestUtils.startDebugMode();
	}
	
	@AfterClass
	public static void afterClass() {
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}

	/** Party A writes data to an archive. B connects and downloads it. */ 
	@Test
	public void testOneWaySync() throws IOException, UnconnectableAdvertisementException {
		ZKMaster        aMaster   = null, bMaster   = null;
		ArchiveAccessor aAccessor = null, bAccessor = null;
		ZKArchiveConfig aConfig   = null, bConfig   = null;
		ZKFS            fsa       = null, fsb       = null;
		Key             rootKey   = new Key(crypto);
		
		try {
			aMaster   = ZKMaster.openBlankTestVolume("copy1");
			aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
			aConfig   = ZKArchiveConfig.createDefault(aAccessor);
			addMockData(aConfig.getArchive());
			aMaster.getGlobalConfig().set("net.swarm.enabled", true);
			aMaster.getTCPListener().advertise(aConfig.getSwarm());
			TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
			
			bMaster   = ZKMaster.openBlankTestVolume("copy2");
			bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
			bConfig   = ZKArchiveConfig.openExisting(bAccessor, aConfig.getArchiveId(), false, Key.blank(crypto));
			bConfig.getSwarm().addPeerAdvertisement(ad);
			bConfig.finishOpening();
			bConfig.getSwarm().requestAll();
			
			// make sure we get all the pages from A, and we don't somehow end up with unexpected extra pages (yes, this was a real bug)
			ZKArchiveConfig aConfig_ = aConfig,
					        bConfig_ = bConfig;
			Util.waitUntil(2000, ()->aConfig_.getArchive().allPageTags().size() == bConfig_.getArchive().allPageTags().size());
			assertFalse(Util.waitUntil(50, ()->aConfig_.getArchive().allPageTags().size() != bConfig_.getArchive().allPageTags().size()));
			assertEquals(aConfig.getArchive().allPageTags().size(), bConfig.getArchive().allPageTags().size());
			
			fsa = aConfig.getRevisionList().branchTips().get(0).getFS();
			fsb = bConfig.getRevisionList().branchTips().get(1).getFS();
			assertArrayEquals(fsa.read("file0"), fsb.read("file0"));
			assertArrayEquals(fsa.read("file1"), fsb.read("file1"));
			assertArrayEquals(fsa.read("file2"), fsb.read("file2"));
		} finally {
			if(fsa     != null) fsa.close();
			if(fsb     != null) fsb.close();
			if(aConfig != null) aConfig.getArchive().close();
			if(bConfig != null) bConfig.getArchive().close();
			if(aMaster != null) aMaster.close();
			if(bMaster != null) bMaster.close();
		}
	}
	
	/** Party A writes data to an archive. B connects and downloads it. Then A makes changes, and B automatically receives them. */
	@Test
	public void testOneWaySyncWithUpdates() throws IOException, UnconnectableAdvertisementException, DiffResolutionException {
		ZKMaster        aMaster   = null, bMaster   = null;
		ArchiveAccessor aAccessor = null, bAccessor = null;
		ZKArchiveConfig aConfig   = null, bConfig   = null;
		ZKFS            fsa       = null, fsb       = null;
		Key             rootKey   = new Key(crypto);
		
		try {
			aMaster   = ZKMaster.openBlankTestVolume("copy1");
			aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
			aConfig   = ZKArchiveConfig.createDefault(aAccessor);
			addMockData(aConfig.getArchive());
			aMaster.getGlobalConfig().set("net.swarm.enabled", true);
			aMaster.getTCPListener().advertise(aConfig.getSwarm());
			TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
			
			bMaster   = ZKMaster.openBlankTestVolume("copy2");
			bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
			bConfig   = ZKArchiveConfig.openExisting(bAccessor, aConfig.getArchiveId(), false, Key.blank(crypto));
			bConfig.getSwarm().addPeerAdvertisement(ad);
			bConfig.finishOpening();
			bConfig.getSwarm().requestAll();
			
			ZKArchiveConfig aConfig_ = aConfig,
					        bConfig_ = bConfig;
			assertTrue(Util.waitUntil(2000, ()->aConfig_.getArchive().allPageTags().size() == bConfig_.getArchive().allPageTags().size()));
			
			fsa = aConfig.getRevisionList().branchTips().get(0).getFS();
			fsa.write("file3", crypto.rng(2*aConfig.getPageSize()));
			fsa.commit();
			
			assertTrue(Util.waitUntil(1000, ()->bConfig_.getRevisionList().branchTips().contains(aConfig_.getRevisionList().branchTips().get(0))));
			Util.waitUntil(2000, ()->aConfig_.getArchive().allPageTags().size() == bConfig_.getArchive().allPageTags().size());
			assertEquals(aConfig.getArchive().allPageTags().size(), bConfig.getArchive().allPageTags().size());
			RevisionTag tag = DiffSetResolver.canonicalMergeResolver(bConfig.getArchive()).resolve();
			
			fsb = tag.getFS();
			for(int i = 0; i <= 3; i++) {
				assertArrayEquals(fsa.read("file"+i), fsb.read("file"+i));
			}
		} finally {
			if(fsa     != null) fsa.close();
			if(fsb     != null) fsb.close();
			if(aConfig != null) aConfig.getArchive().close();
			if(bConfig != null) bConfig.getArchive().close();
			if(aMaster != null) aMaster.close();
			if(bMaster != null) bMaster.close();
		}
	}
	
	/** Party A writes data to an archive, which party B downloads. B then creates changes, which A receives. */
	@Test
	public void testTwoWaySync() throws IOException, DiffResolutionException, UnconnectableAdvertisementException {
		ZKMaster        aMaster   = null, bMaster   = null;
		ArchiveAccessor aAccessor = null, bAccessor = null;
		ZKArchiveConfig aConfig   = null, bConfig   = null;
		ZKFS            fsa       = null, fsb       = null;
		Key             rootKey   = new Key(crypto);
		
		try {
			aMaster   = ZKMaster.openBlankTestVolume("copy1");
			aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
			aConfig   = ZKArchiveConfig.createDefault(aAccessor);
			addMockData(aConfig.getArchive());
			aMaster.getGlobalConfig().set("net.swarm.enabled", true);
			aMaster.getTCPListener().advertise(aConfig.getSwarm());
			TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
			
			bMaster   = ZKMaster.openBlankTestVolume("copy2");
			bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
			bConfig   = ZKArchiveConfig.openExisting(bAccessor, aConfig.getArchiveId(), false, Key.blank(crypto));
			bConfig.getSwarm().addPeerAdvertisement(ad);
			bConfig.finishOpening();
			bConfig.getSwarm().requestAll();
			aConfig.getSwarm().requestAll();
			
			ZKArchiveConfig aConfig_ = aConfig,
					        bConfig_ = bConfig;
			assertTrue(Util.waitUntil(2000, ()->aConfig_.getArchive().allPageTags().size() == bConfig_.getArchive().allPageTags().size()));
			
			fsb = bConfig.getRevisionList().branchTips().get(1).getFS();
			fsb.write("file3", crypto.rng(2*bConfig.getPageSize()));
			fsb.commit();
			
			assertTrue(Util.waitUntil(1000, ()->aConfig_.getRevisionList().branchTips().contains(bConfig_.getRevisionList().branchTips().get(0))));
			Util.waitUntil(2000, ()->aConfig_.getArchive().allPageTags().size() == bConfig_.getArchive().allPageTags().size());
			assertEquals(aConfig.getArchive().allPageTags().size(), bConfig.getArchive().allPageTags().size());
			RevisionTag tag = DiffSetResolver.canonicalMergeResolver(bConfig.getArchive()).resolve();
			
			fsa = tag.getFS();
			for(int i = 0; i <= 3; i++) {
				assertArrayEquals(fsb.read("file"+i), fsa.read("file"+i));
			}
		} finally {			
			if(fsa     != null) fsa.close();
			if(fsb     != null) fsb.close();
			if(aConfig != null) aConfig.getArchive().close();
			if(bConfig != null) bConfig.getArchive().close();
			if(aMaster != null) aMaster.close();
			if(bMaster != null) bMaster.close();
		}
	}
	
	/** Party A writes multiple revisions. Party B requests just one of them. Party B should not have pages not related to the requested revtag. */
	@Test
	public void testRevTagSync() throws IOException, UnconnectableAdvertisementException, DiffResolutionException {
		ZKMaster        aMaster   = null, bMaster   = null;
		ArchiveAccessor aAccessor = null, bAccessor = null;
		ZKArchiveConfig aConfig   = null, bConfig   = null;
		ZKFS            fsa       = null, fsb       = null;
		Key             rootKey   = new Key(crypto);
		
		try {
			aMaster   = ZKMaster.openBlankTestVolume("copy1");
			aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
			aConfig   = ZKArchiveConfig.createDefault(aAccessor);
			fsa       = aConfig.getArchive().openBlank();
			
			fsa.write("path", crypto.rng(2*aConfig.getPageSize()));
			fsa.commit();
			
			fsa.write("path", crypto.rng(2*aConfig.getPageSize()));
			fsa.commit();
			
			aMaster.getGlobalConfig().set("net.swarm.enabled", true);
			aMaster.getTCPListener().advertise(aConfig.getSwarm());
			TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
			
			bMaster   = ZKMaster.openBlankTestVolume("copy2");
			bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
			bConfig   = ZKArchiveConfig.openExisting(bAccessor, aConfig.getArchiveId(), false, Key.blank(crypto));
			bConfig.getSwarm().addPeerAdvertisement(ad);
			bConfig.finishOpening();
			bConfig.getSwarm().requestRevision(0, fsa.getBaseRevision());
			
			// we should get a config (1 page), inode table (1 page), the file (2 pages) and its pagetree (1 page). root is literal so no page.
			ZKArchiveConfig bConfig_ = bConfig;
			assertTrue(Util.waitUntil(1000, ()->bConfig_.getRevisionList().branchTips().size() == 1));
			Util.waitUntil(2000, ()->bConfig_.getArchive().allPageTags().size() == 6);
			assertEquals(5, bConfig.getArchive().allPageTags().size());
					
			fsb = bConfig.getRevisionList().branchTips().get(1).getFS();
			assertArrayEquals(fsb.read("path"), fsa.read("path"));
		} finally {
			if(fsa     != null) fsa.close();
			if(fsb     != null) fsb.close();
			if(aConfig != null) aConfig.getArchive().close();
			if(bConfig != null) bConfig.getArchive().close();
			if(aMaster != null) aMaster.close();
			if(bMaster != null) bMaster.close();
		}
	}
	
	/** Party A writes multiple inodes. Party B requests a single inode. Party B should not have pages unrelated to the requested inode. */
	@Test
	public void testInodeSync() throws IOException, UnconnectableAdvertisementException {
		ZKMaster        aMaster   = null, bMaster   = null;
		ArchiveAccessor aAccessor = null, bAccessor = null;
		ZKArchiveConfig aConfig   = null, bConfig   = null;
		ZKFS            fsa       = null;
		Key             rootKey   = new Key(crypto);
		
		try {
			aMaster   = ZKMaster.openBlankTestVolume("copy1");
			aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
			aConfig   = ZKArchiveConfig.createDefault(aAccessor);
			fsa       = aConfig.getArchive().openBlank();
			
			for(int i = 0; i < 10; i++) {
				fsa.write("path"+i, crypto.rng(2*aConfig.getPageSize()));
			}
			
			fsa.commit();
			Inode inode = fsa.inodeForPath("path0");
			
			aMaster.getGlobalConfig().set("net.swarm.enabled", true);
			aMaster.getTCPListener().advertise(aConfig.getSwarm());
			TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
			
			bMaster   = ZKMaster.openBlankTestVolume("copy2");
			bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
			bConfig   = ZKArchiveConfig.openExisting(bAccessor, aConfig.getArchiveId(), false, Key.blank(crypto));
			bConfig.getSwarm().addPeerAdvertisement(ad);
			bConfig.finishOpening();
			
			ZKArchiveConfig bConfig_ = bConfig;
			assertTrue(Util.waitUntil(1000, ()->bConfig_.getRevisionList().branchTips().size() > 1));
			bConfig.getSwarm().requestInode(0, bConfig_.getRevisionList().branchTips().get(1), inode.getStat().getInodeId());
			
			// we should get a config (1 page), pagetree (1), and file pages (numPages from reftag)
			Util.waitUntil(2000, ()->bConfig_.getArchive().allPageTags().size() == 2 + inode.getRefTag().getNumPages());
			assertEquals(2 + inode.getRefTag().getNumPages(), bConfig.getArchive().allPageTags().size());
		} finally {
			if(fsa     != null) fsa.close();
			if(aConfig != null) aConfig.getArchive().close();
			if(bConfig != null) bConfig.getArchive().close();
			if(aMaster != null) aMaster.close();
			if(bMaster != null) bMaster.close();
		}
	}

	/** Party A writes a file with multiple pages. Party B requests a single page. Party B should not have pages unrelated to the requested page, and the archive config. */
	@Test
	public void testPageSync() throws IOException, UnconnectableAdvertisementException {
		ZKMaster        aMaster   = null, bMaster   = null;
		ArchiveAccessor aAccessor = null, bAccessor = null;
		ZKArchiveConfig aConfig   = null, bConfig   = null;
		ZKFS            fsa       = null;
		Key             rootKey   = new Key(crypto);
		
		try {
			aMaster   = ZKMaster.openBlankTestVolume("copy1");
			aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
			aConfig   = ZKArchiveConfig.createDefault(aAccessor);
			fsa       = aConfig.getArchive().openBlank();
			
			fsa.write("path", crypto.rng(5*aConfig.getPageSize()));
			fsa.commit();
			StorageTag requestedTag = new PageTree(fsa.inodeForPath("path")).getPageTag(0);
			
			aMaster.getGlobalConfig().set("net.swarm.enabled", true);
			aMaster.getTCPListener().advertise(aConfig.getSwarm());
			TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
			
			bMaster   = ZKMaster.openBlankTestVolume("copy2");
			bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
			bConfig   = ZKArchiveConfig.openExisting(bAccessor, aConfig.getArchiveId(), false, Key.blank(crypto));
			bConfig.getSwarm().addPeerAdvertisement(ad);
			bConfig.finishOpening();
			
			// TODO ITF: 2020-06-07 135bcbe, AssertionError
			ZKArchiveConfig bConfig_ = bConfig;
			assertTrue(Util.waitUntil(5000, ()->bConfig_.getRevisionList().branchTips().size() == 1));
			bConfig.getSwarm().requestTag(0, requestedTag);
			
			// we should get the requested page and the config file
			Util.waitUntil(2000, ()->bConfig_.getArchive().allPageTags().size() == 2);
			assertEquals(2, bConfig.getArchive().allPageTags().size());
			
			for(StorageTag pageTag : bConfig.getArchive().allPageTags()) {
				if(bConfig.tag().equals(pageTag)) continue;
				if(requestedTag .equals(pageTag)) continue;
				fail();
			}
		} finally {
			if(fsa     != null) fsa.close();
			if(aConfig != null) aConfig.getArchive().close();
			if(bConfig != null) bConfig.getArchive().close();
			if(aMaster != null) aMaster.close();
			if(bMaster != null) bMaster.close();
		}
	}
	
	/** Party A writes files to the archive. Party B does not specially request anything, but is able to read the files. */
	@Test
	public void testOnDemandDownload() throws IOException, UnconnectableAdvertisementException {
		ZKMaster        aMaster   = null, bMaster   = null;
		ArchiveAccessor aAccessor = null, bAccessor = null;
		ZKArchiveConfig aConfig   = null, bConfig   = null;
		ZKFS            fsa       = null, fsb       = null;
		Key             rootKey   = new Key(crypto);
		
		try {
			aMaster   = ZKMaster.openBlankTestVolume("copy1");
			aAccessor = aMaster.makeAccessorForRoot(rootKey, false);
			aConfig   = ZKArchiveConfig.createDefault(aAccessor);
			fsa       = aConfig.getArchive().openBlank();
			
			fsa.write("immediate", crypto.rng(crypto.hashLength()-1));
			fsa.write("indirect", crypto.rng(aConfig.getPageSize()));
			fsa.write("2indirect", crypto.rng(5*aConfig.getPageSize()));
			fsa.commit();
			
			aMaster.getGlobalConfig().set("net.swarm.enabled", true);
			aMaster.getTCPListener().advertise(aConfig.getSwarm());
			TCPPeerAdvertisement ad = aMaster.getTCPListener().listenerForSwarm(aConfig.getSwarm()).localAd();
			
			bMaster   = ZKMaster.openBlankTestVolume("copy2");
			bAccessor = bMaster.makeAccessorForRoot(rootKey, false);
			bConfig   = ZKArchiveConfig.openExisting(bAccessor, aConfig.getArchiveId(), false, Key.blank(crypto));
			bConfig.getSwarm().addPeerAdvertisement(ad);
			bConfig.finishOpening();
	
			ZKArchiveConfig bConfig_ = bConfig;
			assertTrue(Util.waitUntil(1000, ()->bConfig_.getRevisionList().branchTips().size() > 1));
			fsb = bConfig.getRevisionList().branchTips().get(1).getFS();
			assertArrayEquals(fsa.read("immediate"), fsb.read("immediate"));
			assertArrayEquals(fsa.read("indirect"), fsb.read("indirect"));
			assertArrayEquals(fsa.read("2indirect"), fsb.read("2indirect"));
		} finally {
			if(fsa != null) fsa.close();
			if(fsb != null) fsb.close();
			if(aConfig != null) aConfig.getArchive().close();
			if(bConfig != null) bConfig.getArchive().close();
			if(aMaster != null) aMaster.close();
			if(bMaster != null) bMaster.close();
		}
	}
	
	/** N parties are created, P_0 through P_(N-1).
	 * Each P_k connects to P_(k+1 % N).
	 * After a suitable delay, each party should have N-1 connections.
	 */
	@Test
	public void testPeerDiscovery() throws IOException, UnconnectableAdvertisementException {
		int                    numPeers = 8;
		Key                    rootKey  = new Key(crypto);		
		ZKMaster[]             masters  = new ZKMaster[numPeers];
		ZKArchiveConfig[]      configs  = new ZKArchiveConfig[numPeers];
		TCPPeerAdvertisement[] ads      = new TCPPeerAdvertisement[numPeers];
		
		try {
			// initialize peers; 0 sets up the archive, everyone follow's 0's lead
			for(int i = 0; i < numPeers; i++) {
				masters[i] = ZKMaster.openBlankTestVolume("copy" + i);
				masters[i].getGlobalConfig().set("net.swarm.enabled", true);
	
				ArchiveAccessor accessor = masters[i].makeAccessorForRoot(rootKey, false);
				if(i == 0) {
					configs[i] = ZKArchiveConfig.createDefault(accessor);
					masters[i].getTCPListener().advertise(configs[i].getSwarm());
				} else {
					configs[i] = ZKArchiveConfig.openExisting(accessor, configs[0].getArchiveId(), false, Key.blank(crypto));
					masters[i].getTCPListener().advertise(configs[i].getSwarm());
					configs[i].getSwarm().addPeerAdvertisement(ads[i-1]);
					configs[i].finishOpening();
				}
				
				ads[i] = masters[i].getTCPListener().listenerForSwarm(configs[i].getSwarm()).localAd();
			}
			
			configs[0].getSwarm().addPeerAdvertisement(ads[numPeers-1]);
			
			// give everyone time to connect. We could have duplicate connections early on, so the connections might be higher than desired for a moment.
			for(int i = 0; i < numPeers; i++) {
				final int ii = i;
				assertTrue(Util.waitUntil(2000, ()->configs[ii].getSwarm().connections.size() >= numPeers-1));
			}
			
			// Give some time for duplicates to get managed, and then check to make sure everyone has exactly numPeers-1 connections.
			Util.sleep(100);
			for(int i = 0; i < numPeers; i++) {
				final int ii = i;
				assertTrue(Util.waitUntil(3000, ()->configs[ii].getSwarm().connections.size() == numPeers-1));
			}
		} finally {
			for(int i = 0; i < numPeers; i++) {
				if(configs[i] != null) configs[i].getArchive().close();
				if(masters[i] != null) masters[i].close();
			}
		}
	}
	
	/** N+1 parties are created, P_0 through P_N. The peers are connected and request sync of all pages.
	 * Each peer except P_N writes a set of files. P_N acts as a read-only peer. After a suitable delay,
	 * each party should have each set of files and revtags.
	 * 
	 * The parties then merge all revtags. After a suitable delay, the parties should arrive at a single, shared revtag.
	 * 
	 * The filesystem pointed to by this revtag should contain the files written by each party.
	 */
	@Test
	public void testManyPartySync() throws IOException, UnconnectableAdvertisementException, DiffResolutionException {
		int                    numPeers = 8;
		Key                    rootKey  = new Key(crypto);
		ZKMaster[]             masters  = new ZKMaster[numPeers];
		ZKArchiveConfig[]      configs  = new ZKArchiveConfig[numPeers];
		TCPPeerAdvertisement[] ads      = new TCPPeerAdvertisement[numPeers];
		
		try {
			for(int i = 0; i < numPeers; i++) {
				masters[i] = ZKMaster.openBlankTestVolume("copy" + i);
				masters[i].getGlobalConfig().set("net.swarm.enabled", true);
	
				ArchiveAccessor accessor = masters[i].makeAccessorForRoot(rootKey, false);
				if(i == 0) {
					configs[i] = ZKArchiveConfig.createDefault(accessor);
					masters[i].getTCPListener().advertise(configs[i].getSwarm());
				} else {
					configs[i] = ZKArchiveConfig.openExisting(accessor, configs[0].getArchiveId(), false, Key.blank(crypto));
					masters[i].getTCPListener().advertise(configs[i].getSwarm());
					configs[i].getSwarm().addPeerAdvertisement(ads[i-1]);
					configs[i].finishOpening();
				}
				
				ads[i] = masters[i].getTCPListener().listenerForSwarm(configs[i].getSwarm()).localAd();
				configs[i].getSwarm().requestAll();
	
				try(ZKFS fs = configs[i].getArchive().openBlank()) {
					fs.write("immediate-" + i, crypto.hash(Util.serializeInt(i)));
					fs.write("indirect-" + i, crypto.prng(crypto.hash(Util.serializeInt(i))).getBytes(configs[i].getPageSize()));
					fs.write("2indirect-" + i, crypto.prng(crypto.hash(Util.serializeLong(i))).getBytes(2*configs[i].getPageSize()));
					fs.commit();
				}
			}
			
			for(int i = 0; i < numPeers; i++) {
				final int ii = i;
				Util.waitUntil(10000, ()->configs[ii].getRevisionList().branchTips().size() == numPeers);
				assertEquals(numPeers, configs[ii].getRevisionList().branchTips().size());
			}
			
			for(int i = 0; i < numPeers; i++) {
				DiffSetResolver.canonicalMergeResolver(configs[i].getArchive()).resolve();
				
				assertEquals(1, configs[i].getRevisionList().branchTips().size());
				if(i > 0) {
					assertEquals(configs[i-1].getRevisionList().branchTips().get(0), configs[i].getRevisionList().branchTips().get(0));
				}
				
				try(ZKFS fs = configs[i].getRevisionList().branchTips().get(0).getFS()) {
					for(int j = 0; j < numPeers; j++) {
						assertArrayEquals(crypto.hash(Util.serializeInt(j)), fs.read("immediate-" + j));
						assertArrayEquals(crypto.prng(crypto.hash(Util.serializeInt(j))).getBytes(configs[j].getPageSize()), fs.read("indirect-" + j));
						assertArrayEquals(crypto.prng(crypto.hash(Util.serializeLong(j))).getBytes(2*configs[j].getPageSize()), fs.read("2indirect-" + j));
					}
				}
			}
		} finally {
			for(int i = 0; i < numPeers; i++) {
				configs[i].getArchive().close();
				masters[i].close();
			}
		}
	}
}
