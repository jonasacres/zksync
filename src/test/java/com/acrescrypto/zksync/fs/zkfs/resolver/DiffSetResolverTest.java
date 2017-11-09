package com.acrescrypto.zksync.fs.zkfs.resolver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.ArrayList;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
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
	
	@Ignore // TODO: Disabled for now while we revisit revision files
	public void testMergesInheritFromAllParents() throws IOException, DiffResolutionException {
		ZKFS fs = ZKFS.blankArchive("/tmp/diffset-resolver-merges-inherit-all-parents", "zksync".toCharArray());
		Revision parent = fs.commit();
		Revision[] children = new Revision[8];
		
		for(int i = 0; i < 8; i++) {
			fs = new ZKFS(fs.getStorage(), "zksync".toCharArray(), parent);
			fs.write("file", ("contents " + i).getBytes());
			children[i] = fs.commit();
		}
		
		ArrayList<RevisionTag> leaves = fs.getRevisionTree().leaves();
		Revision[] leavesArray = new Revision[leaves.size()];
		for(int i = 0; i < leaves.size(); i++) leavesArray[i] = new Revision(leaves.get(i));
		
		assertEquals(children.length, leaves.size());
		DiffSet diffset = new DiffSet(leavesArray);
		
		DiffSetResolver resolver = new DiffSetResolver(diffset,
				(DiffSetResolver setResolver, FileDiff diff) ->
		{
			return diff.latestVersion();
		});
		
		Revision merge = resolver.resolve();
		assertEquals(children.length, merge.getNumParents());
		// TODO: individually check each parent to ensure there's a one-to-one correspondence
		ArrayList<RevisionTag> newLeaves = fs.getRevisionTree().leaves();
		assertEquals(1, newLeaves.size());
		assertEquals(merge.getTag(), newLeaves.get(0));
	}
	// merges should inherit from all parents and be the only leaf
	// merges should be deterministic
	
	// directories that turn into files
	// files that turn into directories
	// directories that are deleted
	// directories that are created
	
	// files that are deleted
	// files that are conflicted
	// files that are created
	
	// directories that need merging (ensure mtime is plausible, nlinks are correct)
	
	// exception for unresolved diffs
	// exception for inconsistent resolution (path must be a file and a directory)
}
