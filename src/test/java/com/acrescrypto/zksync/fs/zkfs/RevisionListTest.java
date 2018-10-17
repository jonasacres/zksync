package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.LinkedList;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver;

public class RevisionListTest {
	ZKMaster master;
	ZKArchive archive;
	ZKArchiveConfig config;
	RevisionList list;
	DummyRevisionTree tree;
	CryptoSupport crypto;
	
	class DummyRevisionTree extends RevisionTree {
		public DummyRevisionTree(ZKArchiveConfig config) {
			super(config);
		}

		boolean supercede;
		
		@Override
		public boolean isSuperceded(RevisionTag tag) {
			return supercede;
		}
	}
	
	@BeforeClass	
	public static void beforeClass() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		crypto = master.getCrypto();
		archive = master.createDefaultArchive();
		config = archive.getConfig();
		config.revisionTree = tree = new DummyRevisionTree(config);
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
		ZKFSTest.restoreArgon2Costs();
	}
	
	public LinkedList<RevisionTag> setupFakeRevisions(int count) throws IOException {
		LinkedList<RevisionTag> revTags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			RefTag refTag = new RefTag(archive, crypto.rng(config.refTagSize()));
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
		RefTag refTag = new RefTag(archive, crypto.rng(config.refTagSize()));
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
	public void testAddBranchTipAutomaticallyMergesBranchTipsIfAutomergeEnabled() throws IOException {
		list.setAutomerge(true);
		RevisionTag a = archive.openBlank().commit(),
				    b = archive.openBlank().commit(),
				    m = list.branchTips().get(0);
		
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
		RevisionTag a = archive.openBlank().commit(),
				    b = archive.openBlank().commit();
		
		assertEquals(2, list.branchTips().size());
		assertTrue(list.branchTips().contains(a));
		assertTrue(list.branchTips().contains(b));
	}
	// addBranchTip automatically merges branch tips if automerge enabled
	// addBranchTip does not automatically merge branch tips if automerge disabled
	
	@Test
	public void testConsolidateRemovesAnyTipsAncestralToSpecifiedBranch() throws IOException {
		LinkedList<RevisionTag> tags = new LinkedList<>();
		tags.add(archive.openBlank().getBaseRevision());
		for(int i = 0; i < 16; i++) {
			tags.add(tags.getLast().getFS().commit());
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
			baseTags.add(archive.openBlank().commit());
		}
		
		RevisionTag smallMerge = DiffSetResolver.canonicalMergeResolver(archive).resolve();
		baseTags.add(archive.openBlank().commit());
		RevisionTag largeMerge = DiffSetResolver.canonicalMergeResolver(archive).resolve();
		
		assertEquals(smallMerge.height, largeMerge.height);
		list.addBranchTip(smallMerge);
		assertEquals(2, list.branchTips().size());
		assertTrue(list.branchTips().contains(smallMerge));
		list.consolidate(largeMerge);
		assertEquals(1, list.branchTips().size());
		assertEquals(largeMerge, list.branchTips.get(0));
	}
	
	@Test
	public void testRemoveBranchTipRemovesIndicatedBranchFromList() throws IOException {
		LinkedList<RevisionTag> revTags = setupFakeRevisions(16);
		assertTrue(list.branchTips().contains(revTags.get(1)));
		list.removeBranchTip(revTags.get(1));
		assertFalse(list.branchTips().contains(revTags.get(1)));
	}
	
	@Test
	public void testRemoveBranchTipUpdatesLatsetBranchTip() throws IOException {
		LinkedList<RevisionTag> revTags = setupFakeRevisions(16);
		RevisionTag victim = list.latest();
		assertTrue(revTags.contains(victim));
		list.removeBranchTip(victim);
		assertNotEquals(victim, list.latest());
	}
	
	@Test
	public void testClearRemovesAllEntriesFromList() throws IOException {
		int count = 16;
		setupFakeRevisions(count);
		assertEquals(count, list.branchTips().size());
		list.clear();
		assertEquals(0, list.branchTips().size());
	}
	
	@Test
	public void testClearSetsLatestTagToNull() throws IOException {
		setupFakeRevisions(16);
		assertNotNull(list.latest());
		list.clear();
		assertNull(list.latest());
	}
	
	@Test
	public void testSerialization() throws IOException {
		LinkedList<RevisionTag> revTags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			RefTag refTag = new RefTag(archive, crypto.rng(config.refTagSize()));
			RevisionTag tag = new RevisionTag(refTag, i, 1);
			revTags.add(tag);
			list.addBranchTip(tag);
		}
		
		list.write();
		
		RevisionList newList = new RevisionList(config);
		assertEquals(revTags, list.branchTips());
		assertEquals(revTags, newList.branchTips());
	}
}
