package com.acrescrypto.zksync.fs.zkfs.resolver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;
import java.util.HashSet;

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
		ZKFSTest.cheapenArgon2Costs();
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
		ZKFSTest.restoreArgon2Costs();
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
		fs.close();
		archive.close();
	}
	
	@Test
	public void testMergesInheritFromAllParents() throws IOException, DiffResolutionException {
		// do merges correctly list all their parents in their RevisionInfo?
		ZKFS fs = archive.openRevision(base);
		RevisionTag parent = fs.commit();
		RevisionTag[] children = new RevisionTag[8];
		
		for(int i = 0; i < 8; i++) {
			fs = parent.getFS();
			fs.write("file", ("contents " + i).getBytes());
			children[i] = fs.commit();
		}
		
		ArrayList<RevisionTag> leaves = fs.getArchive().getConfig().getRevisionTree().branchTips();
		RevisionTag[] leavesArray = new RevisionTag[leaves.size()];
		for(int i = 0; i < leaves.size(); i++) {
			leavesArray[i] = leaves.get(i);

		}
		
		assertEquals(children.length, leaves.size());

		RevisionTag merge = DiffSetResolver.latestVersionResolver(new DiffSet(leavesArray)).resolve();
		HashSet<RevisionTag> mergedParents = new HashSet<RevisionTag>(merge.getInfo().getParents());
		assertEquals(children.length, mergedParents.size());
		for(RevisionTag child : children) assertTrue(mergedParents.contains(child));
		ArrayList<RevisionTag> newLeaves = fs.getArchive().getConfig().getRevisionTree().branchTips();
		
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
		RevisionTag baseRev = baseFs.commit();
		
		for(int i = 0; i < versions.length; i++) {
			ZKFS fs = archive.openRevision(baseRev);
			for(int j = 0; j < numFiles; j++) {
				fs.write("file" + j, ("version " + i).getBytes());
			}
			versions[i] = fs.commit();
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
		for(Inode inode : solutions) {
			Inode mergedVersion = merge.readOnlyFS().getInodeTable().inodeWithId(inode.getStat().getInodeId());
			assertEquals(inode, mergedVersion);
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
			RevisionTag revDeleted = fs.commit();
			
			fs.close();
			fs = archive.openRevision(base);
			RevisionTag revNotDeleted = fs.commit();
			
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
			
			fs.close();
			fs = archive.openRevision(tag);
			assertFalse(fs.exists("file"));
			fs.close();
		}
		archive.close();
	}
	
	@Test
	public void testDefaultPicksLatestInode() throws IOException, DiffResolutionException {
		int numChildren = 50, r = (int) (numChildren*Math.random());
		
		for(int i = 0; i < numChildren; i++) {
			int n = (i + r) % numChildren;
			ZKFS child = base.getFS();
			Util.setCurrentTimeNanos(n);
			child.write("file", ("version " + n).getBytes());
			child.commit();
		}
		
		RevisionTag merge = DiffSetResolver.canonicalMergeResolver(archive).resolve();
		assertEquals("version " + (numChildren-1), new String(merge.readOnlyFS().read("file")));
	}
	
	@Test
	public void testDefaultPicksLatestPathLink() throws IOException, DiffResolutionException {
		int numChildren = 50, r = (int) (numChildren*Math.random());
		
		for(int i = 0; i < numChildren; i++) {
			Util.setCurrentTimeNanos(i);
			fs.write("file"+i, ("file"+i).getBytes());
		}
		
		base = fs.commit();
		for(int i = 0; i < numChildren; i++) {
			int n = (i + r) % numChildren;
			ZKFS child = base.getFS();
			child.link("file" + n, "file");
			child.commit();
		}
		
		RevisionTag merge = DiffSetResolver.canonicalMergeResolver(archive).resolve();
		ZKFS mergeFs = merge.readOnlyFS();
		long linkedId = mergeFs.inodeForPath("file").getStat().getInodeId(),
			 expectedId = fs.inodeForPath("file"+(numChildren-1)).getStat().getInodeId();
		assertEquals(expectedId, linkedId);
	}
	
	@Test
	public void testDirectoriesMergeContents() throws IOException, DiffResolutionException {
		for(byte i = 0; i < 4; i++) {
			fs.write(""+i, (""+i).getBytes());
			fs.commit();

			if(i == 1) fs = base.getFS();
		}
		
		RevisionTag merge = DiffSetResolver.canonicalMergeResolver(archive).resolve();
		ZKFS mergeFs = merge.readOnlyFS();
		
		for(byte i = 0; i < 4; i++) assertEquals(""+i, new String(mergeFs.read(""+i)));
	}
	
	@Test
	public void testDefaultPrefersPathExistence() throws IOException, DiffResolutionException {
		for(int i = 0; i < 8; i++) {
			fs = archive.openBlank();
			fs.write("file", "foo".getBytes());
			base = fs.commit();
			
			if(Math.random() < 0.5) {
				fs.write("file", "bar".getBytes());
				fs.commit();
				base.getFS().commit();
			} else {
				fs.write("file", "bar".getBytes());
				fs.commit();
				fs = base.getFS();
				fs.unlink("file");
				fs.commit();
			}
			
			ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS();
			assertEquals("bar", new String(mergeFs.read("file")));
		}
	}
	
	@Test
	public void testDefaultAllowsDeletion() throws IOException, DiffResolutionException {
		fs.write("file", "foo".getBytes());
		base = fs.commit();
		
		if(Math.random() < 0.5) {
			fs.unlink("file");
			fs.commit(); // tip 1 (contains deletion)
			base.getFS().commit(); // tip 2 (empty)
		} else {
			fs.commit(); // tip 1 (empty)
			fs = base.getFS();
			fs.unlink("file");
			fs.commit(); // tip 2 (contains deletion)
		}
		
		ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS();
		assertFalse(mergeFs.exists("file"));
	}
	
	@Test
	public void testParentDeletionPreemptsChildren() throws IOException, DiffResolutionException {
		fs.mkdir("dir");
		base = fs.commit();
		
		fs.write("dir/file", "should get deleted".getBytes());
		fs.commit();
		fs = base.getFS();
		fs.rmdir("dir");
		fs.commit();
		
		PathDiffResolver pathResolver = (DiffSetResolver setResolver, PathDiff diff) -> {
			if(diff.path.equals("dir")) return null;
			return DiffSetResolver.latestPathResolver().resolve(setResolver, diff);
		};
		
		DiffSetResolver resolver = new DiffSetResolver(DiffSet.withCollection(archive.getConfig().getRevisionTree().branchTips()),
				DiffSetResolver.latestInodeResolver(),
				pathResolver);
		
		ZKFS mergeFs = resolver.resolve().readOnlyFS();
		assertFalse(mergeFs.exists("dir"));
		assertFalse(mergeFs.exists("dir/file"));
	}
	
	@Test
	public void testChangedFromIsFirstTiebreaker() throws IOException, DiffResolutionException {
		Util.setCurrentTimeNanos(0);
		fs.write("file", "foo".getBytes());
		base = fs.commit();
		RevisionTag[] revs = new RevisionTag[4];
		
		for(int j = 0; j < 4; j++) {
			if(j == 2) fs = base.getFS();
			Util.setCurrentTimeNanos(1+j%2);
			fs.write("file", (""+j).getBytes());
			revs[j] = fs.commit();
		}
		
		ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS();
		if(Arrays.compareUnsigned(revs[0].getBytes(), revs[2].getBytes()) < 0) {
			assertEquals("3", new String(mergeFs.read("file")));
		} else {
			assertEquals("1", new String(mergeFs.read("file")));
		}
	}
	
	@Test
	public void testSerializedInodeIsSecondTiebraker() throws IOException, DiffResolutionException {
		Util.setCurrentTimeNanos(0);
		fs.write("file", "foo".getBytes());
		base = fs.commit();
		byte[][] serializations = new byte[2][];
		
		for(int j = 0; j < 2; j++) {
			fs = base.getFS();
			Util.setCurrentTimeNanos(1);
			fs.write("file", (""+j).getBytes());
			fs.commit();
			serializations[j] = fs.inodeForPath("file").serialize();
		}
		
		ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS();
		if(Arrays.compareUnsigned(serializations[0], serializations[1]) < 0) {
			assertEquals("1", new String(mergeFs.read("file")));
		} else {
			assertEquals("0", new String(mergeFs.read("file")));
		}
	}
	
	@Test
	public void testNlinksConsistentWhenAddingLinks() throws IOException, DiffResolutionException {
		fs.write("file", "foo".getBytes());
		base = fs.commit();
		fs.link("file", "link-a");
		fs.commit();
		fs = base.getFS();
		fs.link("file", "link-b");
		fs.commit();
		
		ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS();
		assertEquals(mergeFs.inodeForPath("file"), mergeFs.inodeForPath("link-a"));
		assertEquals(mergeFs.inodeForPath("file"), mergeFs.inodeForPath("link-b"));
		assertEquals(3, mergeFs.inodeForPath("file").getNlink());
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
		fs.commit();
		fs = base.getFS();
		fs.unlink("link-b");
		fs.commit();
		
		ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS();
		assertFalse(mergeFs.exists("link-a"));
		assertFalse(mergeFs.exists("link-b"));
		assertEquals(1, mergeFs.inodeForPath("file").getNlink());
	}
	
	@Test
	public void testNlinksConsistentWhenMakingMixedChanges() throws IOException, DiffResolutionException {
		fs.write("file", "foo".getBytes());
		fs.link("file", "orig-link");
		base = fs.commit();
		fs.unlink("orig-link");
		fs.link("file", "link-a");
		fs.commit();
		fs = base.getFS();
		fs.link("file", "link-b");
		fs.commit();
		base.getFS().commit();
		
		ZKFS mergeFs = DiffSetResolver.canonicalMergeResolver(archive).resolve().readOnlyFS();
		assertFalse(mergeFs.exists("orig-link"));
		assertEquals(mergeFs.inodeForPath("link-a"), mergeFs.inodeForPath("file"));
		assertEquals(mergeFs.inodeForPath("link-a"), mergeFs.inodeForPath("link-b"));
		assertEquals(3, mergeFs.inodeForPath("file").getNlink());
	}
	
	@Test
	public void testResolverConsistency() throws IOException, DiffResolutionException {
		RevisionTag[] revs = new RevisionTag[2], merges = new RevisionTag[2];
		fs.write("file1", "some data".getBytes());
		revs[0] = fs.commit();
		
		fs = archive.openBlank();
		fs.write("file2", "more data".getBytes());
		revs[1] = fs.commit();
		
		for(int i = 0; i < 2; i++) {
			DiffSet diffset = new DiffSet(revs);
			DiffSetResolver resolver = DiffSetResolver.latestVersionResolver(diffset);
			merges[i] = resolver.resolve();
		}
		
		assertEquals(merges[0], merges[1]);
	}
}
