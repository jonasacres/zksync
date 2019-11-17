package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.Collection;
import java.util.LinkedList;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.fs.zkfs.RevisionList.RevisionMonitor;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver;
import com.acrescrypto.zksync.net.PeerSwarm;
import com.acrescrypto.zksync.utility.Util;

public class RevisionListTest {
	ZKMaster master;
	ZKArchive archive;
	ZKArchiveConfig config;
	RevisionList list;
	DummyRevisionTree tree;
	CryptoSupport crypto;
	DummySwarm swarm;
	
	class DummyRevisionTree extends RevisionTree {
		public DummyRevisionTree(ZKArchiveConfig config) {
			super(config);
		}

		boolean supercede;
		
		@Override
		public boolean isSuperceded(RevisionTag tag) {
			return supercede;
		}
		
		@Override
		public boolean supercededBy(RevisionTag newTag, RevisionTag existing) throws IOException {
			return supercede;
		}
	}
	
	class DummySwarm extends PeerSwarm {
		RevisionTag requestedStructure, requestedDetails;
		
		public DummySwarm(ZKArchiveConfig config) {
			this.config = config;
		}
		
		@Override
		public void requestRevisionStructure(int priority, RevisionTag revTag) {
			this.requestedStructure = revTag;
		}
		
		@Override
		public void requestRevisionDetails(int priority, RevisionTag revTag) {
			this.requestedDetails = revTag;
		}
	}
	
	@BeforeClass	
	public static void beforeClass() throws IOException {
		TestUtils.startDebugMode();
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@SuppressWarnings("deprecation")
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		master.getGlobalConfig().set("fs.settings.revtagHasLocalCacheTimeout", 0);
		crypto = master.getCrypto();
		archive = master.createDefaultArchive();
		config = archive.getConfig();
		config.revisionTree = tree = new DummyRevisionTree(config);
		swarm = new DummySwarm(config);
		config.setSwarm(swarm);
		list = config.getRevisionList();
	}
	
	@After
	public void afterEach() {
		archive.close();
		master.close();
	}
	
	@AfterClass
	public static void afterAll() throws IOException {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	public LinkedList<RevisionTag> setupFakeRevisions(int count) throws IOException {
		LinkedList<RevisionTag> revTags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			byte[] storageTagBytes = crypto.hash(Util.serializeInt(i));
			StorageTag storageTag = new StorageTag(crypto, storageTagBytes);
			byte refType = i % 2 == 0 ? RefTag.REF_TYPE_INDIRECT : RefTag.REF_TYPE_2INDIRECT;
			int numPages = refType == RefTag.REF_TYPE_INDIRECT ? 1 : 2 + i;
			RefTag refTag = new RefTag(archive, storageTag, refType, numPages);
			RevisionTag tag = new RevisionTag(refTag, i, 1);
			revTags.add(tag);
			list.addBranchTip(tag);
		}
		
		return revTags;
	}
	
	@Test
	public void testAddBranchTipAddsBranchToListIfNotSuperceded() throws IOException {
		RevisionTag tag = setupFakeRevisions(1).get(0);
		assertTrue(list.branchTips().contains(tag));
	}
	
	@Test
	public void testAddBranchTipDoesNotAddBranchToListIfSuperceded() throws IOException {
		tree.supercede = true;
		RevisionTag tag = setupFakeRevisions(1).get(0);
		assertFalse(list.branchTips().contains(tag));
	}
	
	@Test
	public void testAddBranchTipUpdatesLatestBranchWhenAddingNewerTip() throws IOException {
		// latest is understood to mean greatest height.
		byte[] storageTagBytes = crypto.hash(Util.serializeInt(0));
		StorageTag storageTag = new StorageTag(crypto, storageTagBytes);
		RefTag refTag = new RefTag(archive, storageTag, RefTag.REF_TYPE_INDIRECT, 1);
		RevisionTag shallow = new RevisionTag(refTag, 0, 1);
		RevisionTag deep = new RevisionTag(refTag, 1, 2);
		RevisionTag shallowAgain = new RevisionTag(refTag, 3, 1);
		
		list.addBranchTip(shallow);
		assertEquals(shallow, list.latest());
		list.addBranchTip(deep);
		assertEquals(deep, list.latest());
		list.addBranchTip(shallowAgain);
		assertEquals(deep, list.latest());
	}
	
	@Test
	public void testAddBranchTipAutomaticallyMergesBranchTipsOnDelayIfAutomergeEnabled() throws IOException {
		list.setAutomerge(true);
		list.automergeDelayMs = 100;
		list.maxAutomergeDelayMs = 3*list.automergeDelayMs;
		
		RevisionTag a = archive.openBlank().commitAndClose(),
				    b = archive.openBlank().commitAndClose();
		assertEquals(2, list.branchTips().size());
		Util.sleep(2*list.automergeDelayMs + 200);
		RevisionTag m = list.branchTips().get(0);
		assertEquals(1, list.branchTips().size());
		assertNotEquals(a, m);
		assertNotEquals(b, m);
		assertEquals(2, m.getInfo().getParents().size());
		assertTrue(m.getInfo().getParents().contains(a));
		assertTrue(m.getInfo().getParents().contains(b));
	}
	
	@Test
	public void testAddBranchTipDoesNotAutomaticallyMergesBranchTipsIfAutomergeDisabled() throws IOException {
		list.setAutomerge(false);
		list.automergeDelayMs = 1;
		list.maxAutomergeDelayMs = list.automergeDelayMs;
		
		RevisionTag a = archive.openBlank().commitAndClose(),
				    b = archive.openBlank().commitAndClose();
		Util.sleep(5*list.maxAutomergeDelayMs + 200);
		
		assertEquals(2, list.branchTips().size());
		assertTrue(list.branchTips().contains(a));
		assertTrue(list.branchTips().contains(b));
	}
	
	@Test
	public void testConsolidateRemovesAnyTipsAncestralToSpecifiedBranch() throws IOException {
		LinkedList<RevisionTag> tags = new LinkedList<>();
		ZKFS fs = archive.openBlank();
		RevisionTag rev = fs.getBaseRevision();
		fs.close();
		tags.add(rev);
		for(int i = 0; i < 16; i++) {
			tags.add(tags.getLast().getFS().commitAndClose());
		}
		
		for(RevisionTag tag : tags) {
			/* calls to consolidate() in the process of commit() should have deleted the old tips, but
			** our jerry-rigging of RevisionTree will let addBranchTip add them back */
			list.addBranchTip(tag);
		}
		
		assertEquals(tags.size(), list.branchTips().size());
		list.consolidate(tags.getLast());
		assertEquals(1, list.branchTips().size());
		assertEquals(tags.getLast(), list.branchTips().get(0));
	}
	
	@Test
	public void testConsolidateRemovesAnyTipsEncompassedBySpecifiedBranch() throws IOException, DiffResolutionException {
		LinkedList<RevisionTag> baseTags = new LinkedList<>();
		for(int i = 0; i < 3; i++) {
			try(ZKFS fs = archive.openBlank()) {
				baseTags.add(fs.commit());
			}
		}
		
		try(ZKFS fs = archive.openBlank()) {
			RevisionTag smallMerge = DiffSetResolver.canonicalMergeResolver(archive).resolve();
			baseTags.add(fs.commit());
			RevisionTag largeMerge = DiffSetResolver.canonicalMergeResolver(archive).resolve();
			
			assertEquals(smallMerge.getHeight(), largeMerge.getHeight());
			list.addBranchTip(smallMerge);
			assertEquals(2, list.branchTips().size());
			assertTrue(list.branchTips().contains(smallMerge));
			list.consolidate(largeMerge);
			assertEquals(1, list.branchTips().size());
			assertEquals(largeMerge, list.branchTips().get(0));
		}
	}
	
	@Test
	public void testRemoveBranchTipRemovesIndicatedBranchFromList() throws IOException {
		LinkedList<RevisionTag> revTags = setupFakeRevisions(16);
		assertTrue(list.branchTips().contains(revTags.get(1)));
		list.removeBranchTip(revTags.get(1));
		assertFalse(list.branchTips().contains(revTags.get(1)));
	}
	
	@Test
	public void testRemoveBranchTipUpdatesLatestBranchTip() throws IOException {
		LinkedList<RevisionTag> revTags = setupFakeRevisions(16);
		RevisionTag victim = list.latest();
		assertTrue(revTags.contains(victim));
		list.removeBranchTip(victim);
		assertNotEquals(victim, list.latest());
	}
	
	@Test
	public void testClearResetsListToBlank() throws IOException {
		int count = 16;
		setupFakeRevisions(count);
		assertEquals(count, list.branchTips().size());
		list.clear();
		assertEquals(1, list.branchTips().size());
	}
	
	@Test
	public void testClearSetsLatestTagToBlank() throws IOException {
		setupFakeRevisions(16);
		assertNotNull(list.latest());
		list.clear();
		assertTrue(list.latest().equals(RevisionTag.blank(config)));
	}
	
	@Test
	public void testSerialization() throws IOException {
		LinkedList<RevisionTag> revTags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			byte[] storageTagBytes = crypto.hash(Util.serializeInt(1000 + i));
			StorageTag storageTag = new StorageTag(crypto, storageTagBytes);
			byte refType = i % 2 == 0 ? RefTag.REF_TYPE_INDIRECT : RefTag.REF_TYPE_2INDIRECT;
			int numPages = refType == RefTag.REF_TYPE_INDIRECT ? 1 : 2 + i;
			RefTag refTag = new RefTag(archive, storageTag, refType, numPages);
			RevisionTag tag = new RevisionTag(refTag, i, 1);
			revTags.add(tag);
			list.addBranchTip(tag);
		}
		
		list.write();
		
		try(RevisionList newList = new RevisionList(config)) {
			assertEquals(revTags, list.branchTips());
			assertEquals(revTags, newList.branchTips());
		}
	}
	
	@Test
	public void testMonitorsReceiveNotificationForNewRevtags() throws IOException {
		byte[] storageTagBytes = crypto.hash(Util.serializeInt(0));
		StorageTag storageTag = new StorageTag(crypto, storageTagBytes);
		
		MutableBoolean receivedTag = new MutableBoolean();
		RefTag refTag = new RefTag(archive, storageTag, RefTag.REF_TYPE_INDIRECT, 1);
		RevisionTag expectedTag = new RevisionTag(refTag, 0, 1);
		list.addMonitor((revTag)->receivedTag.setValue(revTag.equals(expectedTag)));
		list.addBranchTip(expectedTag);
		assertTrue(Util.waitUntil(1000, ()->receivedTag.booleanValue()));
	}
	
	@Test
	public void testMonitorsDoNotReceiveNotificationForSupercededRevtags() throws IOException {
		tree.supercede = true;
		MutableBoolean receivedTag = new MutableBoolean();
		list.addMonitor((revTag)->receivedTag.setValue(true));
		setupFakeRevisions(1).get(0);
		assertFalse(receivedTag.booleanValue());
	}
	
	@Test
	public void testRemovedMonitorsDoNotReceiveNotificationForNewRevtags() throws IOException {
		byte[] storageTagBytes = crypto.hash(Util.serializeInt(0));
		StorageTag storageTag = new StorageTag(crypto, storageTagBytes);

		MutableBoolean receivedTag = new MutableBoolean();
		RefTag refTag = new RefTag(archive, storageTag, RefTag.REF_TYPE_INDIRECT, 1);
		RevisionTag expectedTag = new RevisionTag(refTag, 0, 1);
		RevisionMonitor monitor = (revTag)->receivedTag.setValue(revTag.equals(expectedTag));
		list.addMonitor(monitor);
		list.removeMonitor(monitor);
		list.addBranchTip(expectedTag);
		assertFalse(receivedTag.booleanValue());
	}
	
	private RevisionTag makeRevisionTag() throws IOException {
		try(ZKFS fs = archive.openBlank()) {
			long id = CryptoSupport.defaultCrypto().defaultPrng().getLong();
			for(int i = 0; i <= fs.inodeTable.numInodesForPage(0); i++) {
				fs.write("item-" + id + "-" + i, ("foo"+i).getBytes());
			}
			
			return fs.commit();
		}
	}
	
	private String moveInodeTablePage(RevisionTag tag) throws IOException {
		PageTree tree = new PageTree(tag.getRefTag());
		String path = tree.getPageTag(0).path();
		tag.getArchive().getStorage().mv(path, path + ".moved");
		return path;
	}
	
	private String moveDirectoryPage(RevisionTag tag) throws IOException {
		try(ZKFS fs = tag.readOnlyFS()) {
			Inode rootDirInode = fs.inodeForPath("/");
			PageTree tree = new PageTree(rootDirInode);
			String path = tree.getPageTag(0).path();
			tag.getArchive().getStorage().mv(path, path + ".moved");
			return path;
		}
	}
	
	private void restorePage(String path) throws IOException {
		archive.getStorage().mv(path + ".moved", path);
	}
	
	@Test
	public void testAvailableTagsReturnsTipsWithCachedInodeTableAndDirectories() throws IOException {
		LinkedList<RevisionTag> expected = new LinkedList<>();
		for(int i = 0; i < 10; i++) {
			expected.add(makeRevisionTag());
		}
		
		Collection<RevisionTag> available = list.availableTags(list.branchTips(), 0);
		assertEquals(expected.size(), available.size());
		assertTrue(expected.containsAll(available));
		assertTrue(available.containsAll(expected));
	}
	
	@Test
	public void testAvailableTagsDoesNotReturnRevTagsWithMissingInodeTablePages() throws IOException {
		LinkedList<RevisionTag> expected = new LinkedList<>();
		for(int i = 0; i < 10; i++) {
			RevisionTag tag = makeRevisionTag();
			if(i % 2 == 0) {
				expected.add(tag);
			} else {
				moveInodeTablePage(tag);
			}
		}
		
		Collection<RevisionTag> available = list.availableTags(list.branchTips(), 0);
		assertEquals(expected.size(), available.size());
		assertTrue(expected.containsAll(available));
		assertTrue(available.containsAll(expected));
	}
	
	@Test
	public void testAvailableTagsDoesNotReturnRevTagsWithMissingDirectoryPages() throws IOException {
		LinkedList<RevisionTag> expected = new LinkedList<>();
		for(int i = 0; i < 10; i++) {
			RevisionTag tag = makeRevisionTag();
			if(i % 2 == 0) {
				expected.add(tag);
			} else {
				moveDirectoryPage(tag);
			}
		}
		
		Collection<RevisionTag> available = list.availableTags(list.branchTips(), 0);
		assertEquals(expected.size(), available.size());
		assertTrue(expected.containsAll(available));
		assertTrue(available.containsAll(expected));
	}
	
	@Test
	public void testAvailableTagsReturnsImmediatelyIfTimeoutIsZero() throws IOException {
		moveDirectoryPage(makeRevisionTag());
		
		long startTime = System.currentTimeMillis();
		list.availableTags(list.branchTips(), 0);
		long elapsed = System.currentTimeMillis() - startTime;
		
		assertTrue(elapsed <= 100); // leave plenty of room here for AvailableTags to do its job
	}
	
	@Test
	public void testAvailableTagsBlocksIndefinitelyIfTimeoutIsNegative() throws IOException {
		MutableBoolean finished = new MutableBoolean();
		String path = moveDirectoryPage(makeRevisionTag());
		
		new Thread(()->{
			try {
				list.availableTags(list.branchTips(), -1);
			} catch (IOException exc) {
				exc.printStackTrace();
				fail();
			}
			finished.setTrue();
		}).start();
		
		Util.sleep(100);
		assertFalse(finished.booleanValue());
		restorePage(path);
		config.swarm.notifyPageUpdate();
		assertTrue(Util.waitUntil(100, ()->finished.booleanValue()));
	}
	
	@Test
	public void testAvailableTagsBlocksForIntervalIfTimeoutIsPositive() throws IOException {
		long timeoutMs = 100;
		moveDirectoryPage(makeRevisionTag());
		
		long startTime = System.currentTimeMillis();
		list.availableTags(list.branchTips(), timeoutMs);
		long elapsed = System.currentTimeMillis() - startTime;
		
		assertTrue(elapsed >= timeoutMs);
		assertTrue(elapsed < timeoutMs + 100);
	}
	
	@Test
	public void testAvailableTagsReturnsImmediatelyIfAllTipsAreAvailable() throws IOException {
		long timeoutMs = 100;
		makeRevisionTag();
		
		long startTime = System.currentTimeMillis();
		list.availableTags(list.branchTips(), timeoutMs);
		long elapsed = System.currentTimeMillis() - startTime;
		
		assertTrue(elapsed < timeoutMs);
	}
	
	@Test
	public void testAvailableTagsRequestsMissingTipStructureIfTimeoutIsNegative() throws IOException {
		MutableBoolean finished = new MutableBoolean();
		RevisionTag tag = makeRevisionTag();
		String path = moveDirectoryPage(tag);
		
		new Thread(()->{
			try {
				list.availableTags(list.branchTips(), -1);
			} catch (IOException exc) {
				exc.printStackTrace();
				fail();
			}
			finished.setTrue();
		}).start();
		
		Util.sleep(100);
		restorePage(path);
		config.swarm.notifyPageUpdate();
		assertTrue(Util.waitUntil(100, ()->finished.booleanValue()));
		assertEquals(tag, swarm.requestedStructure);
	}

	@Test
	public void testAvailableTagsDoesNotRequestMissingTipStructureIfTimeoutIsZero() throws IOException {
		moveDirectoryPage(makeRevisionTag());
		list.availableTags(list.branchTips(), 0);
		assertNull(swarm.requestedStructure);
	}

	@Test
	public void testAvailableTagsRequestsMissingTipStructureIfTimeoutIsPositive() throws IOException {
		RevisionTag tag = makeRevisionTag();
		moveDirectoryPage(tag);
		list.availableTags(list.branchTips(), 1);
		assertEquals(tag, swarm.requestedStructure);
	}
	
	@Test
	public void testAvailableTagsIncludesTipsMadeAvailableDuringBlock() throws IOException {
		MutableBoolean finished = new MutableBoolean();
		RevisionTag tag = makeRevisionTag();
		String path = moveDirectoryPage(tag);
		
		new Thread(()->{
			try {
				Collection<RevisionTag> tips = list.availableTags(list.branchTips(), -1);
				assertTrue(tips.contains(tag));
			} catch (IOException exc) {
				exc.printStackTrace();
				fail();
			}
			finished.setTrue();
		}).start();
		
		Util.sleep(100);
		restorePage(path);
		config.swarm.notifyPageUpdate();
		assertTrue(Util.waitUntil(100, ()->finished.booleanValue()));
		assertEquals(tag, swarm.requestedStructure);

	}
}
