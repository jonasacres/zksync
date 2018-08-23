package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.SearchFailedException;
import com.acrescrypto.zksync.net.PeerSwarm;
import com.acrescrypto.zksync.utility.Util;

public class RevisionTreeTest {
	class DummySwarm extends PeerSwarm {
		RevisionTag requestedTag;
		
		public DummySwarm(ZKArchiveConfig config) throws IOException {
			super(config);
		}
		
		@Override
		public void requestRevisionDetails(int priority, RevisionTag revTag) {
			requestedTag = revTag;
		}
	}

	CryptoSupport crypto;
	ZKMaster master;
	ZKArchiveConfig config;
	ZKArchive archive;
	RevisionTree tree;
	DummySwarm swarm;
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		crypto = new CryptoSupport();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createDefaultArchive();
		config = archive.config;
		config.swarm.close();
		config.swarm = swarm = new DummySwarm(config);
		tree = config.revisionTree;
	}
	
	@After
	public void afterEach() {
		config.close();
		master.close();
		Util.setCurrentTimeMillis(-1);
		RevisionTree.treeSearchTimeoutMs = RevisionTree.DEFAULT_TREE_SEARCH_TIMEOUT_MS;
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	ArrayList<RevisionTag> buildSimpleTree(RevisionTag base, int depth, int width) throws IOException {
		return buildSimpleTree(new ArrayList<>(), base, depth, width);
	}
	
	ArrayList<RevisionTag> buildSimpleTree(ArrayList<RevisionTag> list, RevisionTag base, int depth, int width) throws IOException {
		if(depth <= 0) return list;
		
		for(int i = 0; i < width; i++) {
			ZKFS fs = base.getFS();
			RevisionTag revTag = fs.commitWithTimestamp(new RevisionTag[0], i);
			if(depth == 1) list.add(revTag);
			buildSimpleTree(list, revTag, depth-1, width);
		}
		
		return list;
	}
	
	RevisionTag buildMultiparentRevision(int numParents, ArrayList<RevisionTag> createdParents) throws IOException {
		RevisionTag base = archive.openBlank().commit();
		RevisionTag[] parents = new RevisionTag[numParents-1];
		RevisionTag firstParent = base.getFS().commit();
		if(createdParents != null) createdParents.add(firstParent);
		
		for(int i = 1; i < numParents; i++) {
			parents[i-1] = base.getFS().commitWithTimestamp(new RevisionTag[0], i);
			if(createdParents != null) createdParents.add(parents[i-1]);
		}
		
		return firstParent.getFS().commit(parents);
	}
	
	@Test
	public void testConstructorLoadsExistingTreeIfExists() throws IOException {
		buildSimpleTree(RevisionTag.blank(config), 2, 2);
		config.revisionList.clear();
		RevisionTree tree2 = new RevisionTree(config);
		assertEquals(tree.map, tree2.map);
	}
	
	@Test
	public void testConstructorScansRevisionListIfNoTreeFound() throws IOException {
		buildSimpleTree(RevisionTag.blank(config), 2, 2);
		HashMap<RevisionTag, HashSet<RevisionTag>> oldMap = new HashMap<>(tree.map);
		config.localStorage.unlink(tree.getPath());
		RevisionTree tree2 = new RevisionTree(config);
		assertTrue(oldMap.equals(tree2.map));
	}
	
	@Test
	public void testConstructorScansRevisionListIfTreeUnreadable() throws IOException {
		buildSimpleTree(RevisionTag.blank(config), 2, 2);
		HashMap<RevisionTag, HashSet<RevisionTag>> oldMap = new HashMap<>(tree.map);
		byte[] data = config.getLocalStorage().read(tree.getPath());
		data[0] ^= 0x01; // twiddling any bit will cause the tree to be unreadable
		config.getLocalStorage().write(tree.getPath(), data);
		try {
			// check and make sure we really did corrupt the file
			tree.read();
			fail();
		} catch(SecurityException exc) {}
		
		RevisionTree tree2 = new RevisionTree(config);
		assertEquals(oldMap, tree2.map);
	}
	
	@Test
	public void testAutowriteDefaultsTrue() {
		assertTrue(tree.autowrite);
	}
	
	@Test
	public void testConstructorToleratesEmptyRevisionList() {
		// we're already constructing from an empty list when we initialize the archive
		assertNotNull(tree);
		assertEquals(config, tree.config);
		assertEquals(0, tree.numRevisions());
	}
	
	@Test
	public void testNumRevisionsCountsRevisions() throws IOException {
		buildSimpleTree(RevisionTag.blank(config), 4, 2);
		assertEquals(30, tree.numRevisions()); // 2 + 4 + 8 + 16 = 30
	}
	
	@Test
	public void testValdiateParentListAcceptsValidListsForSingleParents() throws IOException {
		RevisionTag baseRevTag = archive.openBlank().commit();
		ArrayList<RevisionTag> children = buildSimpleTree(baseRevTag, 1, 2);
		ArrayList<RevisionTag> parents = new ArrayList<>();
		parents.add(baseRevTag);
		
		for(RevisionTag child : children) {
			tree.validateParentList(child, parents);
		}
	}
	
	@Test
	public void testValidateParentListRejectsInvalidParentListsForSingleParentRevisions() throws IOException {
		RevisionTag baseRevTag = archive.openBlank().commit();
		ArrayList<RevisionTag> children = buildSimpleTree(baseRevTag, 1, 2);
		ArrayList<RevisionTag> parents = new ArrayList<>();
		parents.add(archive.openBlank().commit());
		
		for(RevisionTag child : children) {
			try {
				tree.validateParentList(child, parents);
				fail();
			} catch(SecurityException exc) {}
		}
	}
	
	@Test
	public void testValidateParentListRejectsEmptyParentListsForSingleParentRevisions() throws IOException {
		RevisionTag baseRevTag = archive.openBlank().commit();
		ArrayList<RevisionTag> children = buildSimpleTree(baseRevTag, 1, 2);
		ArrayList<RevisionTag> parents = new ArrayList<>();
		
		for(RevisionTag child : children) {
			try {
				tree.validateParentList(child, parents);
				fail();
			} catch(SecurityException exc) {}
		}
	}
	
	@Test
	public void testValidateParentListRejectsSupersetParentListsForSingleParentRevisions() throws IOException {
		RevisionTag baseRevTag = archive.openBlank().commit();
		ArrayList<RevisionTag> children = buildSimpleTree(baseRevTag, 1, 2);
		ArrayList<RevisionTag> parents = new ArrayList<>();
		parents.add(baseRevTag);
		parents.add(children.get(0));
		
		for(RevisionTag child : children) {
			try {
				tree.validateParentList(child, parents);
				fail();
			} catch(SecurityException exc) {}
		}
	}
	
	@Test
	public void testValidateParentListAcceptsValidListsForMultiParentRevisions() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag child = buildMultiparentRevision(8, parents);
		tree.validateParentList(child, parents);
	}
	
	@Test(expected=SecurityException.class)
	public void testValidateParentListRejectsInvalidParentListsForMultiparentRevisions() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag child = buildMultiparentRevision(8, parents);
		ArrayList<RevisionTag> evilParents = new ArrayList<>(parents.size());
		for(int i = 0; i < parents.size(); i++) {
			if(i == 2) {
				evilParents.add(RevisionTag.blank(config));
			} else {
				evilParents.add(parents.get(i));
			}
		}
		
		tree.validateParentList(child, evilParents);
	}
	
	@Test(expected=SecurityException.class)
	public void testValidateParentListRejectsEmptyParentListsForMultiparentRevisions() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag child = buildMultiparentRevision(8, parents);
		tree.validateParentList(child, new ArrayList<RevisionTag>());
	}
		
	@Test(expected=SecurityException.class)
	public void testValidateParentListRejectsSubsetParentListsForMultiparentRevisions() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag child = buildMultiparentRevision(8, parents);
		ArrayList<RevisionTag> partialParents = new ArrayList<>(parents.size());
		for(int i = 0; i < parents.size(); i++) {
			if(i != 3) {
				partialParents.add(parents.get(i));
			}
		}
		
		tree.validateParentList(child, partialParents);
	}
	
	@Test(expected=SecurityException.class)
	public void testValidateParentListRejectsSupersetParentListsForMultiparentRevisions() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag child = buildMultiparentRevision(8, parents);
		ArrayList<RevisionTag> supersetParents = new ArrayList<>(parents.size());
		for(int i = 0; i < parents.size(); i++) {
			supersetParents.add(parents.get(i));
		}
		
		supersetParents.add(child);
		
		tree.validateParentList(child, supersetParents);
	}
	
	@Test(expected=SecurityException.class)
	public void testAddParentsForTagValidatesTagParents() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag child = buildMultiparentRevision(2, parents);
		tree.clear();
		parents.remove(0);
		tree.addParentsForTag(child, parents);
	}
	
	@Test
	public void testAddParentsForTagToleratesMultipleInvocations() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag child = buildMultiparentRevision(2, parents);
		tree.addParentsForTag(child, parents);
		tree.addParentsForTag(child, parents);
	}
	
	@Test
	public void testAddParentsForTagAddsMapping() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag child = buildMultiparentRevision(2, parents);
		tree.clear();
		assertFalse(tree.hasParentsForTag(child));
		tree.addParentsForTag(child, parents);
		assertTrue(tree.hasParentsForTag(child));
		assertNotEquals(parents, tree.parentsForTagNonblocking(child));
		assertEquals(parents.size(), tree.parentsForTagNonblocking(child).size());
		assertTrue(parents.containsAll(tree.parentsForTagNonblocking(child)));
	}
	
	@Test
	public void testAddParentsForTagAutomaticallyWritesWhenAutowriteEnabled() throws IOException {
		tree.autowrite = true;
		buildSimpleTree(RevisionTag.blank(config), 1, 4); // automatically calls addParentsForTag
		assertEquals(4, tree.numRevisions());
		tree.autowrite = false; // don't want clear to erase written file
		tree.clear();
		assertEquals(0, tree.numRevisions());
		tree.read();
		assertEquals(4, tree.numRevisions());
	}
	
	@Test
	public void testAddParentsForTagDoesNotAutomaticallyWRiteWhenAutowriteNotEnabled() throws IOException {
		tree.autowrite = false;
		tree.write(); // make sure file exists; i find this more aesthetically pleasing than catching enoent
		buildSimpleTree(RevisionTag.blank(config), 1, 4); // automatically calls addParentsForTag
		assertEquals(4, tree.numRevisions());
		tree.clear();
		assertEquals(0, tree.numRevisions());
		tree.read();
		assertEquals(0, tree.numRevisions());
	}
	
	@Test
	public void testAddParentsForTagUnblocksFetchParentsForTag() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag tag = buildMultiparentRevision(2, parents);
		tree.clear();
		assertFalse(tree.fetchParentsForTag(tag, 1)); // make sure it blocks and fails
		new Thread(()->{
			tree.addParentsForTag(tag, parents);
		}).start();
		assertTrue(tree.fetchParentsForTag(tag, 1000));
	}
	
	@Test
	public void testScanRevTagDoesNotBlockForMissingRevTags() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag tag = buildMultiparentRevision(2, parents);
		config.archive.config.getStorage().purge();
		tree.clear();
		tree.scanRevTag(tag);
	}
	
	@Test
	public void testHasParentsForTagReturnsTrueIfParentsKnownForRevtag() throws IOException {
		RevisionTag tag = buildMultiparentRevision(1, null);
		assertTrue(tree.hasParentsForTag(tag));
	}
	
	@Test
	public void testHasParentsForTagReturnsFalseIfParentsNotKnownForRevtag() throws IOException {
		RevisionTag tag = buildMultiparentRevision(1, null);
		tree.clear();
		assertFalse(tree.hasParentsForTag(tag));
	}
	
	@Test
	public void testParentsForTagReturnsListOfParentsForTag() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag tag = buildMultiparentRevision(1, parents);
		assertEquals(parents.size(), tree.parentsForTag(tag, 0).size());
		assertTrue(parents.containsAll(tree.parentsForTag(tag, 0)));
	}
	
	@Test
	public void testParentsForTagBlocksUntilListObtained() throws IOException {
		MutableBoolean gotTag = new MutableBoolean();
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag tag = buildMultiparentRevision(1, parents);
		tree.clear();
		new Thread(()->{
			gotTag.setValue(tree.parentsForTag(tag, 3000) != null);
		}).start();
		
		assertFalse(Util.waitUntil(100, ()->gotTag.booleanValue()));
		tree.addParentsForTag(tag, parents);
		assertTrue(Util.waitUntil(100, ()->gotTag.booleanValue()));
	}
	
	@Test
	public void testParentsForTagReturnsAfterTimeout() throws IOException {
		RevisionTag tag = buildMultiparentRevision(1, null);
		tree.clear();
		long time = System.currentTimeMillis(), delay = 100;
		tree.parentsForTag(tag, delay);
		assertTrue(System.currentTimeMillis() >= time + delay);
	}
	
	@Test
	public void testParentsForTagReturnsNullIfTimeoutReached() throws IOException {
		RevisionTag tag = buildMultiparentRevision(1, null);
		tree.clear();
		assertNull(tree.parentsForTag(tag, 1));
	}
	
	@Test
	public void testParentsForTagBlocksWithoutLimitIfTimeoutIsNegative() throws IOException {
		MutableBoolean gotTag = new MutableBoolean();
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag tag = buildMultiparentRevision(1, parents);
		tree.clear();
		new Thread(()->{
			gotTag.setValue(tree.parentsForTag(tag, -1) != null);
		}).start();
		
		// we can't wait forever to see if it's really without limit, but we can make sure we're at least waiting
		assertFalse(Util.waitUntil(300, ()->gotTag.booleanValue()));
		tree.addParentsForTag(tag, parents);
		assertTrue(Util.waitUntil(300, ()->gotTag.booleanValue()));
	}
	
	@Test
	public void testParentsForTagRequestsRevisionDetailsFromSwarmIfNotPresent() throws IOException {
		RevisionTag tag = buildMultiparentRevision(1, null);
		tree.clear();
		assertNull(swarm.requestedTag);
		tree.parentsForTag(tag, 1);
		assertEquals(tag, swarm.requestedTag);
	}

	@Test
	public void testParentsForTagDoesNotRequestRevisionDetailsFromSwarmIfPresent() throws IOException {
		RevisionTag tag = buildMultiparentRevision(1, null);
		assertNull(swarm.requestedTag);
		tree.parentsForTag(tag, 1);
		assertNull(swarm.requestedTag);
	}

	@Test
	public void testParentsForTagNonblockingReturnsNullIfTagNotPresent() throws IOException {
		RevisionTag tag = buildMultiparentRevision(1, null);
		tree.clear();
		assertNull(tree.parentsForTagNonblocking(tag));
	}
	
	@Test
	public void testParentsForTagNonblockingDoesNotRequestRevisionDetailsFromSwarm() throws IOException {
		RevisionTag tag = buildMultiparentRevision(1, null);
		tree.clear();
		assertNull(swarm.requestedTag);
		tree.parentsForTagNonblocking(tag);
		assertNull(swarm.requestedTag);
	}
	
	@Test
	public void testParentsForTagNonblockingReturnsParentsIfTagPresent() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag tag = buildMultiparentRevision(1, parents);
		assertEquals(parents.size(), tree.parentsForTagNonblocking(tag).size());
		assertTrue(parents.containsAll(tree.parentsForTagNonblocking(tag)));
	}
	
	@Test
	public void testCommonAncestorForBlankIsBlank() throws SearchFailedException {
		RevisionTag blank = RevisionTag.blank(config);
		ArrayList<RevisionTag> list = new ArrayList<>();
		list.add(blank);
		assertEquals(blank, tree.commonAncestor(list));
	}

	@Test
	public void testCommonAncestorForOneRevisionIsTheRevisionItself() throws IOException {
		// the common ancestor of a list of one revtag is the revtag itself (yes, you are your own ancestor, it works out trust me)
		RevisionTag tag = archive.openBlank().commit();
		ArrayList<RevisionTag> list = new ArrayList<>();
		list.add(tag);
		assertEquals(tag, tree.commonAncestor(list));
	}
	
	@Test
	public void testCommonAncestorOfBlankAndNonblankIsBlank() throws IOException {
		RevisionTag blank = RevisionTag.blank(config);
		RevisionTag tag = archive.openBlank().commit();
		ArrayList<RevisionTag> list = new ArrayList<>();
		list.add(blank);
		list.add(tag);
		assertEquals(blank, tree.commonAncestor(list));
	}
	
	@Test
	public void testCommonAncestorOfSiblingsIsParent() throws IOException {
		ZKFS fs = archive.openBlank();
		RevisionTag parent = fs.commit();
		int numChildren = 8;
		
		ArrayList<RevisionTag> children = new ArrayList<>(numChildren);
		for(int i = 0; i < numChildren; i++) {
			RevisionTag child = parent.getFS().commitWithTimestamp(new RevisionTag[0], i);
			children.add(child);
		}
		
		assertEquals(parent, tree.commonAncestor(children));
	}
	
	@Test
	public void testCommonAncestorOfMixedParentSiblingsIsSharedParent() throws IOException {
		ZKFS fs = archive.openBlank();
		RevisionTag parent = fs.commit();
		int numChildren = 8;
		
		ArrayList<RevisionTag> children = new ArrayList<>(numChildren);
		for(int i = 0; i < numChildren; i++) {
			RevisionTag otherParent = archive.openBlank().commit();
			RevisionTag child = parent.getFS().commitWithTimestamp(new RevisionTag[] { otherParent }, i);
			children.add(child);
		}
		
		assertEquals(parent, tree.commonAncestor(children));
	}
	
	@Test
	public void testCommonAncestorOfSiblingsWithQuasiSharedParentsIsSharedParent() throws IOException {
		// 2 quasi parents, each belonging to half the nodes, so they're not common to all and shouldn't be the result.
		ZKFS fs = archive.openBlank();
		RevisionTag parent = fs.commit();
		RevisionTag[] quasi = { archive.openBlank().commit(), archive.openBlank().commit() };
		int numChildren = 8;
		
		ArrayList<RevisionTag> children = new ArrayList<>(numChildren);
		for(int i = 0; i < numChildren; i++) {
			RevisionTag child = parent.getFS().commitWithTimestamp(new RevisionTag[] { quasi[i%2] }, i);
			children.add(child);
		}
		
		assertEquals(parent, tree.commonAncestor(children));
	}
	
	@Test
	public void testCommonAncestorOfSiblingsWithMultipleSharedParentsReturnsLowestValued() throws IOException {
		int numParents = RevisionInfo.maxParentsForConfig(config);
		ArrayList<RevisionTag> parents = new ArrayList<>(numParents);
		RevisionTag[] revTags = new RevisionTag[numParents-1];
		for(int i = 0; i < numParents; i++) {
			RevisionTag revTag = archive.openBlank().commit();
			if(i < numParents-1) {
				revTags[i] = revTag;
			}
			parents.add(revTag);
		}
		parents.sort(null);
		
		int numChildren = 8;
		
		ArrayList<RevisionTag> children = new ArrayList<>(numChildren);
		for(int i = 0; i < numChildren; i++) {
			RevisionTag child = parents.get(numParents-1).getFS().commitWithTimestamp(revTags, i);
			children.add(child);
		}
		
		assertEquals(parents.get(0), tree.commonAncestor(children));
	}
	
	@Test
	public void testCommonAncestorOfAuntNiece() throws IOException {
		// this time the compared revisions have different heights
		// (one is the child of the sibling of the other, so it's the "niece")
		RevisionTag ancestor = archive.openBlank().commit();
		RevisionTag aunt = ancestor.getFS().commit();
		RevisionTag sister = ancestor.getFS().commitWithTimestamp(new RevisionTag[0], 0);
		RevisionTag niece = sister.getFS().commitWithTimestamp(new RevisionTag[0], 1);
		
		ArrayList<RevisionTag> revisions = new ArrayList<>(2);
		revisions.add(aunt);
		revisions.add(niece);
		assertEquals(ancestor, tree.commonAncestor(revisions));
	}
	
	@Test
	public void testCommonAncestorReturnsBlankIfNoSharedAncestor() throws IOException {
		int numTags = 4;
		ArrayList<RevisionTag> revTags = new ArrayList<>(numTags);
		for(int i = 0; i < numTags; i++) {
			RevisionTag revTag = archive
				.openBlank()
				.commitWithTimestamp(new RevisionTag[0], i)
				.getFS()
				.commitWithTimestamp(new RevisionTag[0], i);
			revTags.add(revTag);
		}
		
		assertTrue(tree.commonAncestor(revTags).refTag.isBlank());
	}
	
	@Test
	public void testCommonAncestorWithManyGenerationsOfDepth() throws IOException {
		RevisionTag parent = archive.openBlank().commit();
		ArrayList<RevisionTag> revTags = buildSimpleTree(parent, 4, 2);
		assertEquals(parent, tree.commonAncestor(revTags));
	}
	
	@Test
	public void testDescendentOfReturnsTrueIfRevTagAndAncestorAreSame() throws IOException {
		RevisionTag revTag = archive.openBlank().commit();
		assertTrue(tree.descendentOf(revTag, revTag));
	}
	
	@Test
	public void testDescendentOfReturnsTrueIfAncestorIsBlank() throws IOException {
		RevisionTag revTag = archive.openBlank().commit();
		assertTrue(tree.descendentOf(revTag, RevisionTag.blank(config)));
	}
	
	@Test
	public void testDescendentOfReturnsTrueIfAncestorIsImmediateParentOfTag() throws IOException {
		RevisionTag ancestor = archive.openBlank().commit();
		RevisionTag revTag = ancestor.getFS().commit();
		assertTrue(tree.descendentOf(revTag, ancestor));
	}
	
	@Test
	public void testDescendentOfReturnsTrueIfAncestorIsImmediateParentOfTagInMultiparentList() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		RevisionTag revTag = buildMultiparentRevision(8, parents);
		for(RevisionTag parent : parents) {
			assertTrue(tree.descendentOf(revTag, parent));
		}
	}
	
	@Test
	public void testDescendentOfReturnsTrueIfAncestorIsGrandparentOfTag() throws IOException {
		int generations = 8;
		RevisionTag ancestor = archive.openBlank().commit(), revTag = ancestor;
		for(int i = 0; i < generations; i++) {
			revTag = ancestor.getFS().commit();
			assertTrue(tree.descendentOf(revTag, ancestor));
		}
	}
	
	@Test
	public void testDescendentOfReturnsFalseIfRevtagIsBlankAndAncestorIsNot() throws IOException {
		RevisionTag revTag = archive.openBlank().commit();
		assertFalse(tree.descendentOf(RevisionTag.blank(config), revTag));
	}
	
	@Test
	public void testDescendentOfReturnsFalseIfRootRevtagDoesNotHaveIndicatedAncestor() throws IOException {
		RevisionTag revTagA = archive.openBlank().commitWithTimestamp(new RevisionTag[0], 0);
		RevisionTag revTagB = archive.openBlank().commitWithTimestamp(new RevisionTag[0], 1);
		assertFalse(tree.descendentOf(revTagB, revTagA));
		assertFalse(tree.descendentOf(revTagA, revTagB));
	}

	@Test
	public void testDescendentOfReturnsFalseIfNonRootRevtagDoesNotHaveIndicatedAncestor() throws IOException {
		RevisionTag revTagA = archive.openBlank().commitWithTimestamp(new RevisionTag[0], 0).getFS().commit();
		RevisionTag revTagB = archive.openBlank().commitWithTimestamp(new RevisionTag[0], 1).getFS().commit();
		assertFalse(tree.descendentOf(revTagB, revTagA));
		assertFalse(tree.descendentOf(revTagA, revTagB));
	}
}
