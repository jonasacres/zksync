package com.acrescrypto.zksync.fs.zkfs.resolver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.Arrays;
import org.junit.*;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.fs.zkfs.*;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver.InodeDiffResolver;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolver.PathDiffResolver;
import com.acrescrypto.zksync.utility.Util;

public class DiffSetResolverTest {
	ZKFS fs;
	ZKArchive archive;
	RevisionTag base;
	static ZKMaster master;
	
	@BeforeClass
	public static void beforeClass() {
		TestUtils.startDebugMode();
		Security.addProvider(new BouncyCastleProvider());
		try {
			master = ZKMaster.openBlankTestVolume();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@AfterClass
	public static void afterClass() {
		master.close();
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master.purge();
		Util.setCurrentTimeNanos(-1);
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "unit test");
		fs = archive.openBlank();
		base = fs.commit();
	}
	
	@After
	public void afterEach() throws IOException {
		if(!fs.isClosed()) fs.close();
		archive.close();
	}
	
	@Test
	public void testMergesInheritFromAllParents() throws IOException, DiffResolutionException {
		// do merges correctly list all their parents in their RevisionInfo?
		ZKFS fs = archive.openRevision(base);
		RevisionTag parent = fs.commitAndClose();
		RevisionTag[] children = new RevisionTag[8];
		
		for(int i = 0; i < 8; i++) {
			fs = parent.getFS();
			fs.write("file", ("contents " + i).getBytes());
			children[i] = fs.commitAndClose();
		}
		
		ArrayList<RevisionTag> leaves = fs.getArchive().getConfig().getRevisionList().branchTips();
		RevisionTag[] leavesArray = new RevisionTag[leaves.size()];
		for(int i = 0; i < leaves.size(); i++) {
			leavesArray[i] = leaves.get(i);

		}
		
		assertEquals(children.length, leaves.size());

		RevisionTag merge = DiffSetResolver.latestVersionResolver(new DiffSet(leavesArray)).resolve();
		HashSet<RevisionTag> mergedParents = new HashSet<RevisionTag>(merge.getInfo().getParents());
		assertEquals(children.length, mergedParents.size());
		for(RevisionTag child : children) assertTrue(mergedParents.contains(child));
		ArrayList<RevisionTag> newLeaves = fs.getArchive().getConfig().getRevisionList().branchTips();
		
		assertEquals(1, newLeaves.size());
		assertEquals(merge, newLeaves.get(0));
	}

	@Test
	public void testInodeRuleIsApplied() throws IOException, DiffResolutionException {
		// if the resolver lambda picks an inode to use in the merge, does DiffSetResolver actually honor that?
		RevisionTag[] versions = new RevisionTag[2];
		int numFiles = 64;
		
		// create all the files so we have a common inode id
		ZKFS baseFs = archive.openRevision(RevisionTag.blank(archive.getConfig()));
		for(int i = 0; i < numFiles; i++) {
			baseFs.write("file" + i, "base version".getBytes());
		}
		RevisionTag baseRev = baseFs.commitAndClose();
		
		for(int i = 0; i < versions.length; i++) {
			ZKFS fs = archive.openRevision(baseRev);
			for(int j = 0; j < numFiles; j++) {
				fs.write("file" + j, ("version " + i).getBytes());
			}
			versions[i] = fs.commitAndClose();
		}
		
		ArrayList<Inode> solutions = new ArrayList<Inode>();
		InodeDiffResolver inodeResolver = (DiffSetResolver r, InodeDiff diff) -> {
			Inode best = null;
			for(Inode candidate : diff.resolutions.keySet()) {
				if(best == null || Arrays.compareUnsigned(best.getRefTag().getBytes(), candidate.getRefTag().getBytes()) < 0) {
					best = candidate;
				}
			}
			
			solutions.add(best);
			return best;
		};
		
		PathDiffResolver pathResolver = (DiffSetResolver r, PathDiff diff) -> {
			Long best = null;
			for(Long candidate : diff.resolutions.keySet()) {
				if(best == null || candidate.compareTo(best) < 0) {
					best = candidate;
				}
			}
			
			return best;
		};
		
		DiffSet diffset = new DiffSet(versions);
		DiffSetResolver resolver = new DiffSetResolver(diffset, inodeResolver, pathResolver);
		
		RevisionTag merge = resolver.resolve();
		assertEquals(numFiles, solutions.size());
		try(ZKFS fs = merge.readOnlyFS()) {
			for(Inode inode : solutions) {
				Inode mergedVersion = fs.getInodeTable().inodeWithId(inode.getStat().getInodeId());
				assertEquals(inode, mergedVersion);
			}
		}
	}
		
	@Test
	public void testDeletedFilesSelected() throws IOException, DiffResolutionException {
		// if we prefer a version in which a file is deleted, is that honored?
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "testDeletedFilesSelected");
		for(int i = 0; i < 10; i++) { // we might pass by chance, so re-run n times to make that highly unlikely
			if(archive.getStorage().exists("/")) archive.getStorage().rmrf("/");
			ZKFS fs = archive.openBlank();
			
			fs.write("file", "now you see me".getBytes());
			RevisionTag base = fs.commit();
			
			fs.unlink("file");
			RevisionTag revDeleted = fs.commitAndClose();
			
			fs = archive.openRevision(base);
			RevisionTag revNotDeleted = fs.commitAndClose();
			
			DiffSet diffset = new DiffSet(new RevisionTag[] { revDeleted, revNotDeleted });
			InodeDiffResolver inodeResolver = (DiffSetResolver r, InodeDiff diff) -> {
				if(diff.resolutions.containsKey(null)) return null;
				for(Inode inode : diff.resolutions.keySet()) return inode;
				throw new RuntimeException("somehow got a diff with no solutions :(");
			};
			
			PathDiffResolver pathResolver = (DiffSetResolver r, PathDiff diff) -> {
				if(diff.resolutions.containsKey(null)) return null;
				for(Long inodeId : diff.resolutions.keySet()) return inodeId;
				throw new RuntimeException("somehow got a diff with no solutions :(");
			};

			RevisionTag tag = diffset.resolver(inodeResolver, pathResolver).resolve();
			
			fs = archive.openRevision(tag);
			assertFalse(fs.exists("file"));
			fs.close();
		}
		archive.close();
	}
	
	@Test
	public void testDefaultPicksLatestInode() throws IOException, DiffResolutionException {
		// if a merge conflict between siblings, we should pick the latest version of an inode (by timestamp)
		int numChildren = RevisionInfo.USABLE_PARENT_SIZE/RevisionTag.sizeForConfig(archive.getConfig()) - 1,
				r = (int) (numChildren*Math.random());
		
		for(int i = 0; i < numChildren; i++) {
			int n = (i + r) % numChildren;
			ZKFS child = base.getFS();
			Util.setCurrentTimeNanos(n);
			child.write("file", ("version " + n).getBytes());
			child.commitAndClose();
		}
		
		RevisionTag merge = DiffSetResolver.canonicalMergeResolver(archive).resolve();
		try(ZKFS fs = merge.readOnlyFS()) {
			assertEquals("version " + (numChildren-1), new String(fs.read("file")));
		}
	}
	
	@Test
	public void testDefaultPicksLatestPathLink() throws IOException, DiffResolutionException {
		// in a merge conflict between siblings, we should pick the latest version of a path
		int numChildren = RevisionInfo.maxParentsForConfig(archive.getConfig()),
				r = (int) (numChildren*Math.random());
		
		for(int i = 0; i < numChildren; i++) {
			Util.setCurrentTimeNanos(i);
			fs.write("file"+i, ("file"+i).getBytes());
		}
		
		base = fs.commit();
		for(int i = 0; i < numChildren; i++) {
			int n = (i + r) % numChildren;
			ZKFS child = base.getFS();
			child.link("file" + n, "file");
			child.commitAndClose();
		}
		
		RevisionTag merge = DiffSetResolver.canonicalMergeResolver(archive).resolve();
		try(ZKFS mergeFs = merge.readOnlyFS()) {
			long linkedId = mergeFs.inodeForPath("file").getStat().getInodeId(),
				 expectedId = fs.inodeForPath("file"+(numChildren-1)).getStat().getInodeId();
			assertEquals(expectedId, linkedId);
		}
	}
	
	@Test
	public void testDirectoriesMergeContents() throws IOException, DiffResolutionException {
		// Make /a in one rev, /b in another. Merge them. Should have /a and /b in a merged directory.
		for(byte i = 0; i < 4; i++) {
			fs.write(""+i, (""+i).getBytes());
			fs.commit();
			if(i == 1) {
				fs.close();
				fs = base.getFS();
			}
		}
		
		RevisionTag merge = DiffSetResolver.canonicalMergeResolver(archive).resolve();
		try(ZKFS mergeFs = merge.readOnlyFS()) {
			for(byte i = 0; i < 4; i++) {
				assertEquals(""+i, new String(mergeFs.read(""+i)));	
			}
		}
	}
	
	@Test
	public void testDefaultPrefersPathExistence() throws IOException, DiffResolutionException {
		// Prefer to keep a path in a merge conflict vs. deleting it
		fs.close();
		
		for(int i = 0; i < 8; i++) {
			fs = archive.openBlank();
			fs.write("file", "foo".getBytes());
			base = fs.commit();
			
			if(i % 2 == 0) {
				fs.write("file", "bar".getBytes());
				fs.commitAndClose();
				base.getFS().commitAndClose();
			} else {
				fs.write("file", "bar".getBytes());
				fs.commitAndClose();
				fs = base.getFS();
				fs.unlink("file");
				fs.commitAndClose();
			}
			
			try(ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS()) {
				assertEquals("bar", new String(mergeFs.read("file")));
			}
		}
	}
	
	@Test
	public void testDefaultAllowsDeletionWhenDeleteComesSecond() throws IOException, DiffResolutionException {
		// If a file is created then deleted, and merged with a branch that never had the file, it should not be created
		fs.write("file", "foo".getBytes());
		base = fs.commit();
		
		fs.commitAndClose(); // tip 1 (empty)
		fs = base.getFS();
		fs.unlink("file");
		fs.commitAndClose(); // tip 2 (contains deletion)
		
		try(ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS()) {
			assertFalse(mergeFs.exists("file"));
		}
	}
	
	@Test
	public void testDefaultAllowsDeletionWhenDeleteComesFirst() throws IOException, DiffResolutionException {
		// If a file is created, then deleted from a branch, then another branch is created with the original file the merge should not have the file 
		fs.write("file", "foo".getBytes());
		base = fs.commit();
		
		fs.unlink("file");
		fs.commitAndClose(); // tip 1 (contains deletion)
		base.getFS().commitAndClose(); // tip 2 (empty)
		
		try(ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS()) {
			assertFalse(mergeFs.exists("file"));
		}
	}
	
	@Test
	public void testChildPreservationPreemptsParentDeletion() throws IOException, DiffResolutionException {
		// a merge should contain parents of kept children, even if the parents would otherwise be deleted
		String dirName = "dir", fileName = dirName + "/file", dotdot = dirName + "/..";
		byte[] content = "keep me".getBytes();
		
		fs.mkdir(dirName);
		base = fs.commit();
		
		fs.write(fileName, content);
		long inodeId = fs.stat(fileName).getInodeId();
		fs.commitAndClose();
		
		fs = base.getFS();
		fs.rmdir(dirName);
		fs.commitAndClose();
		
		PathDiffResolver pathResolver = (DiffSetResolver setResolver, PathDiff diff) -> {
			if(diff.path.equals(dirName)) return null;
			if(diff.path.equals(dirName + "/.")) return null;
			if(diff.path.equals(dotdot)) return null;
			if(diff.path.equals(fileName)) return inodeId;
			throw new RuntimeException("shouldn't get here");
		};
		
		DiffSetResolver resolver = new DiffSetResolver(DiffSet.withCollection(archive.getConfig().getRevisionList().branchTips()),
				DiffSetResolver.latestInodeResolver(),
				pathResolver);
		
		try(ZKFS mergeFs = resolver.resolve().readOnlyFS()) {
			assertTrue(mergeFs.exists(dirName));
			assertTrue(mergeFs.exists(fileName));
			assertTrue(mergeFs.exists(dotdot));
			assertArrayEquals(content, mergeFs.read(fileName));
			
			try(ZKDirectory dir = mergeFs.opendir(dirName)) {
				long dotDotInodeId = dir.getEntries().get("..");
				assertEquals(InodeTable.INODE_ID_ROOT_DIRECTORY, dotDotInodeId);
			}
		}
	}
	
	@Test
	public void testChangedFromIsFirstTiebreaker() throws IOException, DiffResolutionException {
		Util.setCurrentTimeNanos(0);
		fs.write("file", "foo".getBytes());
		base = fs.commit();
		RevisionTag[] revs = new RevisionTag[4];
		
		for(int j = 0; j < 4; j++) {
			if(j == 2) {
				fs.close();
				fs = base.getFS();
			}
			Util.setCurrentTimeNanos(1+j%2);
			fs.write("file", (""+j).getBytes());
			revs[j] = fs.commit();
		}
		
		try(ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS()) {
			if(Arrays.compareUnsigned(revs[0].getBytes(), revs[2].getBytes()) < 0) {
				assertEquals("3", new String(mergeFs.read("file")));
			} else {
				assertEquals("1", new String(mergeFs.read("file")));
			}
		}
	}
	
	@Test
	public void testSerializedInodeIsSecondTiebraker() throws IOException, DiffResolutionException {
		Util.setCurrentTimeNanos(0);
		fs.write("file", "foo".getBytes());
		base = fs.commitAndClose();
		byte[][] serializations = new byte[2][];
		
		for(int j = 0; j < 2; j++) {
			fs = base.getFS();
			Util.setCurrentTimeNanos(1);
			fs.write("file", (""+j).getBytes());
			fs.commit();
			serializations[j] = fs.inodeForPath("file").serialize();
			fs.close();
		}
		
		try(ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS()) {
			if(Arrays.compareUnsigned(serializations[0], serializations[1]) < 0) {
				assertEquals("1", new String(mergeFs.read("file")));
			} else {
				assertEquals("0", new String(mergeFs.read("file")));
			}
		}
	}
	
	@Test
	public void testNlinksConsistentWhenAddingLinks() throws IOException, DiffResolutionException {
		fs.write("file", "foo".getBytes());
		
		base = fs.commit();
		fs.link("file", "link-a");
		fs.commitAndClose();
		
		fs = base.getFS();
		fs.link("file", "link-b");
		fs.commitAndClose();
		
		try(ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS()) {
			assertEquals(mergeFs.inodeForPath("file"), mergeFs.inodeForPath("link-a"));
			assertEquals(mergeFs.inodeForPath("file"), mergeFs.inodeForPath("link-b"));
			assertEquals(3, mergeFs.inodeForPath("file").getNlink());
		}
	}
	
	@Test
	public void testNlinksConsistentWhenRemovingLinks() throws IOException, DiffResolutionException {
		fs.write("file", "foo".getBytes());
		fs.link("file", "link-a");
		fs.link("file", "link-b");
		
		assertEquals(fs.inodeForPath("file"), fs.inodeForPath("link-a"));
		assertEquals(fs.inodeForPath("file"), fs.inodeForPath("link-b"));
		assertEquals(3, fs.inodeForPath("file").getNlink());
		base = fs.commit();
		
		fs.unlink("link-a");
		fs.commitAndClose();
		
		fs = base.getFS();
		fs.unlink("link-b");
		fs.commitAndClose();
		
		try(ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS()) {
			assertFalse(mergeFs.exists("link-a"));
			assertFalse(mergeFs.exists("link-b"));
			assertEquals(1, mergeFs.inodeForPath("file").getNlink());
		}
	}
	
	@Test
	public void testMoveEditedFile() throws IOException, DiffResolutionException {
		// shared file. A moves, B edits. We want B's edit in A's new location, with the old location gone.
		fs.write("file", "a".getBytes());
		base = fs.commit();
		
		fs.mv("file", "new");
		fs.commitAndClose();
		
		fs = base.getFS();
		fs.write("file", "bbb".getBytes());
		fs.commitAndClose();
		
		try(ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS()) {
			assertFalse(mergeFs.exists("file"));
			assertTrue(mergeFs.exists("new"));
			assertArrayEquals("bbb".getBytes(), mergeFs.read("new"));
		}
	}
	
	@Test
	public void testMoveAndUnlinkDoesntUnlinkNewLocation() throws IOException, DiffResolutionException {
		// shared file. A moves it, B unlinks it. Want the file to exist in A's new location.
		fs.write("file", "a".getBytes());
		base = fs.commit();
		
		fs.mv("file", "new");
		fs.commitAndClose();
		
		fs = base.getFS();
		fs.unlink("file");
		fs.commitAndClose();
		
		try(ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS()) {
			assertFalse(mergeFs.exists("file"));
			assertTrue(mergeFs.exists("new"));
			assertArrayEquals("a".getBytes(), mergeFs.read("new"));
		}
	}
	
	@Test
	public void testCompetingMovesCreatesHardlinks() throws IOException, DiffResolutionException {
		fs.write("file", "a".getBytes());
		base = fs.commit();
		
		fs.mv("file", "newA");
		fs.commitAndClose();
		
		fs = base.getFS();
		fs.mv("file", "newB");
		fs.commitAndClose();
		
		try(ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS()) {
			assertFalse(mergeFs.exists("file"));
			assertTrue(mergeFs.exists("newA"));
			assertTrue(mergeFs.exists("newB"));
			assertEquals(mergeFs.inodeForPath("newA"), mergeFs.inodeForPath("newB"));
			assertArrayEquals("a".getBytes(), mergeFs.read("newA"));
		}
	}
	
	@Test
	public void testWriteOverridesUnlink() throws IOException, DiffResolutionException {
		fs.write("file", "a".getBytes());
		base = fs.commit();
		
		fs.write("file", "bb".getBytes());
		fs.commitAndClose();
		
		fs = base.getFS();
		fs.unlink("file");
		fs.commitAndClose();
		
		try(ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS()) {
			assertTrue(mergeFs.exists("file"));
			assertArrayEquals("bb".getBytes(), mergeFs.read("file"));
		}
	}
	
	@Test
	public void testRemakeFileOverridesUnlink() throws IOException, DiffResolutionException {
		fs.write("file", "a".getBytes());
		base = fs.commit();
		
		fs.unlink("file");
		fs.commitAndClose();
		
		fs = base.getFS();
		fs.unlink("file");
		fs.write("file", "b".getBytes());
		fs.commitAndClose();

		try(ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS()) {
			assertTrue(mergeFs.exists("file"));
			assertArrayEquals("b".getBytes(), mergeFs.read("file"));
		}
	}
	
	@Test
	public void testNlinksConsistentWhenMakingMixedChanges() throws IOException, DiffResolutionException {
		fs.write("file", "foo".getBytes());
		fs.link("file", "orig-link");
		base = fs.commit();
		
		fs.unlink("orig-link");
		fs.link("file", "link-a");
		fs.commitAndClose();
		
		fs = base.getFS();
		fs.link("file", "link-b");
		fs.commitAndClose();
		
		base.getFS().commitAndClose();
		
		try(ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS()) {
			assertFalse(mergeFs.exists("orig-link"));
			assertEquals(mergeFs.inodeForPath("link-a"), mergeFs.inodeForPath("file"));
			assertEquals(mergeFs.inodeForPath("link-a"), mergeFs.inodeForPath("link-b"));
			assertEquals(3, mergeFs.inodeForPath("file").getNlink());
		}
	}
	
	@Test
	public void testResolverConsistency() throws IOException, DiffResolutionException {
		RevisionTag[] revs = new RevisionTag[2], merges = new RevisionTag[2];
		fs.write("file1", "some data".getBytes());
		revs[0] = fs.commitAndClose();
		
		fs = archive.openBlank();
		fs.write("file2", "more data".getBytes());
		revs[1] = fs.commitAndClose();
		
		for(int i = 0; i < 2; i++) {
			DiffSet diffset = new DiffSet(revs);
			DiffSetResolver resolver = DiffSetResolver.latestVersionResolver(diffset);
			merges[i] = resolver.resolve();
		}
		
		assertEquals(merges[0], merges[1]);
	}
	
	@Test
	public void testMergeObjectivity() throws IOException, DiffResolutionException {
		/* Let S be a set of revtags.
		 * Let A be a subset of S, and merge(X) be a function returning the revtag of the merge of set X of revtags.
		 * Then, for any decomposition A = A1 u A2, merge(A1, A2) = merge(A).
		 */
		
		try(ZKArchive archive = master.createDefaultArchive()) {
			ArrayList<RevisionTag> directTags = new ArrayList<>();
			
			for(int i = 0; i < 8; i++) {
				try(ZKFS fs = archive.openBlank()) {
					fs.write("file"+i, ("contents " + i).getBytes());
					directTags.add(fs.commit());
				}
				
				RevisionTag merge = DiffSetResolver.canonicalMergeResolver(archive).resolve();
				DiffSet directSet = DiffSet.withCollection(directTags);
				RevisionTag directTag = DiffSetResolver.latestVersionResolver(directSet).resolve();
				assertEquals(directTag, merge);
				
				try(ZKFS fs = merge.getFS()) {
					for(int j = 0; j <= i; j++) {
						assertArrayEquals(("contents " + i).getBytes(), fs.read("file" + i));
					}
					
					assertFalse(fs.exists("file"+(i+1)));
				}
			}
		}
	}
	
	@Test
	public void testDirectoryObjectivity() throws IOException, DiffResolutionException {
		/* Directories get modified in path merges, creating danger of inconsistent mtimes. */
		fs.mkdir("dir");
		RevisionTag base = fs.commit();
		RevisionTag[] revs = new RevisionTag[2];
		
		fs.mkdir("dir/sub1");
		revs[0] = fs.commit();
		
		fs.rebase(base);
		fs.mkdir("dir/sub2");
		revs[1] = fs.commit();
		
		RevisionTag[] merges = new RevisionTag[2];
		for(int i = 0; i < merges.length; i++) {
			if(i > 0) Util.sleep(1);
			
			DiffSet diffSet = new DiffSet(revs);
			merges[i] = DiffSetResolver.latestVersionResolver(diffSet).resolve();
			if(i > 0) {
				assertEquals(merges[0], merges[i]);
			}
		}
	}
	
	@Test
	public void testMergeParentsAppearInSortedOrder() throws IOException, DiffResolutionException {
		LinkedList<RevisionTag> revs = new LinkedList<>();
		int max = RevisionInfo.maxParentsForConfig(archive.getConfig());
		for(int i = 0; i < max; i++) {
			revs.add(archive.openBlank().commitAndClose());
		}
		
		revs.sort((a,b)->b.compareTo(a));
		DiffSet diffset = DiffSet.withCollection(revs);
		
		DiffSetResolver resolver = new DiffSetResolver(diffset,
				DiffSetResolver.latestInodeResolver(),
				DiffSetResolver.latestPathResolver());
		RevisionTag merge = resolver.resolve();
		
		revs.sort(null);
		assertEquals(2, merge.getHeight());
		assertEquals(revs, merge.getInfo().getParents());
	}
	
	@Test
	public void testResolveCreatesPartialRevisionsIfParentListDoesNotFitIntoInfoSection() throws IOException, DiffResolutionException {
		LinkedList<RevisionTag> revs = new LinkedList<>();
		int max = RevisionInfo.maxParentsForConfig(archive.getConfig());
		for(int i = 0; i < 2*max; i++) {
			revs.add(archive.openBlank().commitAndClose());
		}
		
		DiffSet diffset = DiffSet.withCollection(revs);
		
		DiffSetResolver resolver = new DiffSetResolver(diffset,
				DiffSetResolver.latestInodeResolver(),
				DiffSetResolver.latestPathResolver());
		RevisionTag merge = resolver.resolve();
		
		revs.sort(null);
		LinkedList<LinkedList<RevisionTag>> halves = new LinkedList<>();
		for(int i = 0; i < 2; i++) {
			LinkedList<RevisionTag> half = new LinkedList<>();
			halves.add(half);
			for(int j = i*max; j < (i+1)*max; j++) {
				half.add(revs.get(j));
			}
		}
		
		assertEquals(2, merge.getInfo().getNumParents());
		assertEquals(3, merge.getHeight());
		for(RevisionTag parent : merge.getInfo().getParents()) {
			Collection<RevisionTag> grandparents = parent.getInfo().getParents();
			for(LinkedList<RevisionTag> half : halves) {
				if(half.equals(grandparents)) {
					halves.remove(half);
					break;
				}
			}
		}
		
		assertEquals(0, halves.size());
	}
	
	@Test
	public void testResolveCreatesMultipleTiersOfPartialRevisionsIfNeeded() throws IOException, DiffResolutionException {
		ZKArchive archive = master.createArchive(4096, "unit test");
		LinkedList<RevisionTag> revs = new LinkedList<>();
		int max = RevisionInfo.maxParentsForConfig(archive.getConfig());
		for(int i = 0; i < max*(max+1); i++) {
			revs.add(archive.openBlank().commitAndClose());
		}
		
		DiffSet diffset = DiffSet.withCollection(revs);
		
		DiffSetResolver resolver = new DiffSetResolver(diffset,
				DiffSetResolver.latestInodeResolver(),
				DiffSetResolver.latestPathResolver());
		RevisionTag merge = resolver.resolve();
		
		revs.sort(null);
		assertEquals(4, merge.getHeight());
		assertEquals(2, merge.getInfo().getNumParents());
		for(RevisionTag tag : revs) {
			assertTrue(archive.getConfig().getRevisionTree().descendentOf(merge, tag));
		}
		archive.close();
	}
	
	@Test
	public void testInodeDiffRenumberingCausesRemapOfDotDotReferences() throws IOException, DiffResolutionException {
		fs.write("a", "".getBytes());
		fs.inodeForPath("a").setIdentity(1); // guarantee that this version gets inode 16 in the resolution
		assertEquals(InodeTable.USER_INODE_ID_START, fs.inodeForPath("a").getStat().getInodeId());
		RevisionTag revWithFile = fs.commitAndClose();
		
		fs = base.getFS();
		fs.mkdir("b");
		assertEquals(InodeTable.USER_INODE_ID_START, fs.inodeForPath("b").getStat().getInodeId());
		fs.mkdir("b/c");
		RevisionTag revWithDir = fs.commit();
		
		ArrayList<RevisionTag> tags = new ArrayList<>();
		tags.add(revWithFile);
		tags.add(revWithDir);
		DiffSetResolver resolver = DiffSetResolver.canonicalMergeResolver(tags);
		RevisionTag merge = resolver.resolve();
		
		try(ZKFS mergedFs = merge.getFS()) {
			assertEquals(InodeTable.USER_INODE_ID_START, mergedFs.inodeForPath("a").getStat().getInodeId());
			long bInodeId = mergedFs.inodeForPath("b").getStat().getInodeId();
			try(ZKDirectory dir = mergedFs.opendir("b/c")) {
				assertEquals(bInodeId, dir.getEntries().get("..").longValue());
			}
		}
	}
	
	@Test
	public void testDotDotExistsIfParentDoes() throws IOException, DiffResolutionException {
		/* a merge should not delete .. from a directory if it would otherwise NOT delete the directory
		 * (test design comes from observed issue)
		 */
		fs.mkdirp("a/b");
		base = fs.commit();
		
		fs.write("a/b/c", new byte[0]);
		fs.commitAndClose();
		
		fs = base.getFS();
		fs.rmrf("a");
		fs.commitAndClose();
		
		DiffSetResolver resolver = DiffSetResolver.canonicalMergeResolver(archive);
		RevisionTag merge = resolver.resolve();
		
		try(ZKFS fs = merge.getFS()) {
			long parentInodeId = fs.stat("a").getInodeId();
			try(ZKDirectory dir = fs.opendir("a/b")) {
				assertEquals(parentInodeId, dir.getEntries().get("..").longValue());
			}
		}
	}
}
