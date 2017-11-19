package com.acrescrypto.zksync.fs.zkfs.resolver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.Arrays;
import org.junit.*;

import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.fs.zkfs.*;

public class DiffSetResolverTest {
	@BeforeClass
	public static void beforeClass() {
		ZKFSTest.cheapenArgon2Costs();
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@AfterClass
	public static void afterClass() {
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Test
	public void testMergesInheritFromAllParents() throws IOException, DiffResolutionException {
		// do merges correctly list all their parents in their RevisionInfo?
		ZKFS fs = ZKFS.blankArchive("/tmp/diffset-resolver-merges-inherit-all-parents", "zksync".toCharArray());
		RefTag parent = fs.commit();
		RefTag[] children = new RefTag[8];
		
		for(int i = 0; i < 8; i++) {
			fs = parent.getFS();
			fs.write("file", ("contents " + i).getBytes());
			children[i] = fs.commit();
		}
		
		ArrayList<RefTag> leaves = fs.getArchive().getRevisionTree().branchTips();
		RefTag[] leavesArray = new RefTag[leaves.size()];
		for(int i = 0; i < leaves.size(); i++) leavesArray[i] = leaves.get(i);
		
		assertEquals(children.length, leaves.size());

		RefTag merge = DiffSetResolver.defaultResolver(new DiffSet(leavesArray)).resolve();
		HashSet<RefTag> mergedParents = new HashSet<RefTag>(merge.getInfo().getParents());
		assertEquals(children.length, mergedParents.size());
		for(RefTag child : children) assertTrue(mergedParents.contains(child));
		ArrayList<RefTag> newLeaves = fs.getArchive().getRevisionTree().branchTips();
		
		assertEquals(1, newLeaves.size());
		assertEquals(merge, newLeaves.get(0));
	}

	// TODO: merges should be deterministic
	
	@Test
	public void testRuleIsApplied() throws IOException, DiffResolutionException {
		// if the resolver lambda picks an inode to use in the merge, does DiffSetResolver actually honor that?
		RefTag[] versions = new RefTag[2];
		ZKArchive archive = ZKArchive.archiveAtPath("/tmp/zksync-test/diffset-resolver-rule-is-applied-consistently", "zksync".toCharArray());
		int numFiles = 50;
		
		for(int i = 0; i < versions.length; i++) {
			ZKFS fs = archive.openRevision(RefTag.blank(archive).getBytes());
			for(int j = 0; j < numFiles; j++) {
				fs.write("file" + j, ("version " + i).getBytes());
			}
			versions[i] = fs.commit();
		}
		
		DiffSet diffset = new DiffSet(versions);
		ArrayList<Inode> solutions = new ArrayList<Inode>();
		DiffSetResolver resolver = new DiffSetResolver(diffset, (DiffSetResolver r, FileDiff diff) -> {
			Collection<Inode> inodes = diff.getVersions().keySet();
			Inode best = null;
			for(Inode candidate : inodes) {
				if(best == null || Arrays.compareUnsigned(best.getRefTag().getBytes(), candidate.getRefTag().getBytes()) < 0) {
					best = candidate;
				}
			}
			
			solutions.add(best);
			return best;
		});
		
		RefTag merge = resolver.resolve();
		assertEquals(numFiles+1, solutions.size()); // +1 for root directory
		for(Inode inode : solutions) {
			Inode mergedVersion = merge.readOnlyFS().getInodeTable().inodeWithId(inode.getStat().getInodeId());
			assertEquals(inode, mergedVersion);
		}
	}
	
	// TODO: directories that turn into files
	// TODO: files that turn into directories
	// TODO: directories that are deleted
	// TODO: directories that are created
	
	@Test
	public void testDeletedFilesSelected() throws IOException, DiffResolutionException {
		// if we prefer a version in which a file is deleted, is that honored?
		// TODO: loop this test a few times
		ZKArchive archive = ZKArchive.archiveAtPath("/tmp/zksync-test/diffset-deleted-files-selected", "zksync".toCharArray());
		for(int i = 0; i < 10; i++) {
			archive.getStorage().rmrf("/");
			ZKFS fs = archive.openBlank();
			
			fs.write("file", "now you see me".getBytes());
			RefTag base = fs.commit();
			
			fs.unlink("file");
			RefTag revDeleted = fs.commit();
			
			fs = archive.openRevision(base);
			RefTag revNotDeleted = fs.commit();
			
			DiffSet diffset = new DiffSet(new RefTag[] { revDeleted, revNotDeleted });
			RefTag tag = diffset.resolver((DiffSetResolver resolver, FileDiff diff) -> {
				if(diff.getVersions().containsKey(null)) return null;
				for(Inode inode : diff.getVersions().keySet()) return inode;
				throw new RuntimeException("somehow got a diff with no solutions :(");
			}).resolve();
			
			fs = archive.openRevision(tag);
			assertFalse(fs.exists("file"));
		}
	}
	
	// TODO: files that are deleted
	// TODO: files that are conflicted
	// TODO: files that are created
	
	// TODO: directories that need merging (ensure mtime is plausible, nlinks are correct)
	
	// TODO: exception for unresolved diffs
	// TODO: exception for inconsistent resolution (path must be a file and a directory)
}
