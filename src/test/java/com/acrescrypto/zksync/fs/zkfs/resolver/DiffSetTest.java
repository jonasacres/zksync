package com.acrescrypto.zksync.fs.zkfs.resolver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKFile;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSet;


public class DiffSetTest {
	private interface DiffExampleLambda {
		public int diff(ZKFS fs, RefTag[] revs, String filename) throws IOException;
	}
	
	LocalFS storage;
	RefTag parent;
	RefTag[] children;
	char[] password = "zksync".toCharArray();
	
	public final static int NUM_CHILDREN = 4;
	
	@BeforeClass
	public static void beforeClass() {
		ZKFSTest.cheapenArgon2Costs();
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@AfterClass
	public static void afterClass() {
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		storage = new LocalFS("/tmp/zksync-diffset");
		if(storage.exists("/")) storage.rmrf("/");
		ZKFS fs = ZKFS.fsForStorage(storage, password);
		fs.write("unmodified", "parent".getBytes());
		fs.write("modified", "replaceme".getBytes());
		fs.squash("modified");
		parent = fs.commit();
		children = new RefTag[NUM_CHILDREN];
		
		for(int i = 0; i < NUM_CHILDREN; i++) {
			fs = ZKFS.fsForStorage(storage, password, parent.getBytes());
			fs.write("modified", "replaced!".getBytes());
			fs.write("child", ("text " + i).getBytes()); // don't forget -- making this means / is also changed, so factor that in
			fs.squash("modified");
			children[i] = fs.commit();
		}
	}
	
	@After
	public void afterEach() throws IOException {
		storage.rmrf("/");
	}
	
	@Test
	public void testDetectsDifferencesBetweenSiblings() throws IOException {
		RefTag[] list = new RefTag[] { children[0], children[1] };
		DiffSet diffset = new DiffSet(list);
		
		assertEquals(4, diffset.inodeDiffs.size()); // revinfo, root directory, child, modified
		assertEquals(1, diffset.pathDiffs.size()); // /child
	}
	
	@Test
	public void testDetectsFakeDifferencesBetweenSiblingsForNonImmediates() throws IOException {
		LocalFS storage = new LocalFS("/tmp/zksync-diffset-nonimmediate");
		if(storage.exists("/")) storage.rmrf("/");
		ZKFS fs = ZKFS.fsForStorage(storage, password);
		byte[] buf = new byte[fs.getArchive().getPrivConfig().getPageSize()+1];
		fs.write("unmodified", buf);
		fs.write("modified", buf);
		RefTag parent = fs.commit();
		RefTag[] children = new RefTag[2];
		
		for(int i = 0; i < children.length; i++) {
			buf[0] = (byte) i;
			fs = parent.getFS();
			fs.write("modified", buf);
			fs.setAtime("modified", 12345l);
			fs.setMtime("modified", 12345l);
			children[i] = fs.commit();
		}

		DiffSet diffset = new DiffSet(children);
		storage.rmrf("/");
		
		assertEquals(2, diffset.inodeDiffs.size()); // modified, RevisionInfo
		assertEquals(0, diffset.pathDiffs.size());
	}
	
	@Test
	public void testDetectsParentChildDifferences() throws IOException {
		RefTag[] list = new RefTag[] { parent, children[0] };
		DiffSet diffset = new DiffSet(list);
		
		assertEquals(4, diffset.inodeDiffs.size()); // revinfo, root directory, child, modified
		assertEquals(1, diffset.pathDiffs.size()); // child only
	}
	
	@Test
	public void testConsidersOriginalAVersionWhenExplicitlyInSet() throws IOException {
		/* if we look at the diffs between two of the children, we should see only one diff, because they both
		 * modify one of the files in the same way.
		 */
		assertEquals(4, (new DiffSet(new RefTag[] { children[0], children[1] })).inodeDiffs.size());
		
		/* but if we include the parent, we should see two diffs, because it has the original unmodified file. */
		assertEquals(5, (new DiffSet(new RefTag[] { parent, children[0], children[1] })).inodeDiffs.size());
	}
	
	@Test
	public void testUnlinksAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			revs[0] = fs.commit();
			fs.unlink(filename);
			return 2;
		});
	}
	
	@Test
	public void testCreationsAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			revs[0] = fs.commit();
			fs.write(filename, "blah".getBytes());
			return 2;
		});
	}
	
	@Test
	public void testMtimesAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.setMtime(filename, 31337l);
			return 1;
		});
	}
	
	@Test
	public void testAtimesAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.setAtime(filename, 31337l);
			fs.setMtime(filename, 0l);
			return 1;
		});
	}
	
	@Test
	public void testCtimesAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			Inode inode = fs.inodeForPath(filename);
			inode.getStat().setCtime(31337l);
			fs.setMtime(filename, 0l);
			return 1;
		});
	}
	
	@Test
	public void testModesAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.chmod(filename, 0004);
			fs.setMtime(filename, 0l);
			return 1;
		});
	}
	
	@Test
	public void testUidsAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.chown(filename, 007);
			fs.setMtime(filename, 0l);
			return 1;
		});
	}
	
	@Test
	public void testUsernamesAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.chown(filename, "bond");
			fs.setMtime(filename, 0l);
			return 1;
		});
	}
	
	@Test
	public void testGidsAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.chown(filename, 6);
			fs.setMtime(filename, 0l);
			return 1;
		});
	}
	
	@Test
	public void testGroupNamesAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.chown(filename, "MI6");
			fs.setMtime(filename, 0l);
			return 1;
		});
	}
	
	@Test
	public void testAppendsAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			ZKFile file = fs.open(filename, ZKFile.O_APPEND|ZKFile.O_RDWR);
			file.write("a".getBytes());
			file.close();
			fs.setMtime(filename, 0l);
			return 1;
		});
	}
	
	@Test
	public void testTruncatesAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			ZKFile file = fs.open(filename, ZKFile.O_APPEND|ZKFile.O_RDWR);
			file.truncate(file.getStat().getSize()-1);
			file.close();
			fs.setMtime(filename, 0l);
			return 1;
		});
	}
	
	@Test
	public void testMajorDevsAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.mknod(filename, Stat.TYPE_BLOCK_DEVICE, 0, 0);
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.inodeForPath(filename).getStat().setDevMajor(1);
			return 1;
		});
	}
	
	@Test
	public void testMinorDevsAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.mknod(filename, Stat.TYPE_BLOCK_DEVICE, 0, 0);
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.inodeForPath(filename).getStat().setDevMinor(1);
			return 1;
		});
	}
	
	@Test
	public void testNlinksAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			revs[0] = fs.commit();
			fs.inodeForPath(filename).addLink();
			return 1;
		});
	}
	
	@Test
	public void testFileTypesAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RefTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			revs[0] = fs.commit();
			fs.inodeForPath(filename).getStat().setType(Stat.TYPE_BLOCK_DEVICE);
			return 1;
		});
	}
	
	@Test
	public void testDirectoryEntriesAreADifference() throws IOException {
		String filename = "scratch";
		LocalFS storage = new LocalFS("/tmp/zksync-diffset-" + filename);
		if(storage.exists("/")) storage.rmrf("/");
		ZKFS fs = ZKFS.fsForStorage(storage, password);
		
		RefTag[] revs = new RefTag[2];
		
		fs.write("file0", "blah".getBytes());
		fs.write("file1", "blah".getBytes());
		revs[0] = fs.commit();
		
		fs.unlink("file1");
		fs.link("file0", "file1"); // 1 path diff and 2 inode diffs (nlinks changed)
		revs[1] = fs.commit();

		DiffSet diffset = new DiffSet(revs);
		storage.rmrf("/");
		assertEquals(4, diffset.inodeDiffs.size()); // file0, file1, /, RevisionInfo
		assertEquals(1, diffset.pathDiffs.size()); // file1
		assertTrue(diffset.pathDiffs.containsKey("file1"));
	}
	
	protected void trivialInodeDiffTest(DiffExampleLambda meat) throws IOException {
		String filename = "scratch";
		LocalFS storage = new LocalFS("/tmp/zksync-diffset-" + filename);
		if(storage.exists("/")) storage.rmrf("/");
		ZKFS fs = ZKFS.fsForStorage(storage, password);
		
		RefTag[] revs = new RefTag[2];
		int numDiffs = meat.diff(fs, revs, filename);
		
		revs[1] = fs.commit();
		
		DiffSet diffset = new DiffSet(revs);
		assertEquals(1+numDiffs, diffset.inodeDiffs.size()); // add one for RevisionInfo
		storage.rmrf("/");
	}
}
