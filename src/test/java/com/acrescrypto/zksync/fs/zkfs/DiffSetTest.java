package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.Security;
import java.util.ArrayList;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.localfs.LocalFS;


public class DiffSetTest {
	private interface DiffExampleLambda {
		public void diff(ZKFS fs, Revision[] revs, String filename) throws IOException;
	}
	
	LocalFS storage;
	Revision parent;
	Revision[] children;
	char[] password = "zksync".toCharArray();
	
	public final static int NUM_CHILDREN = 8;
	
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
		if(storage.equals("/")) storage.rmrf("/");
		ZKFS fs = new ZKFS(storage, password);
		fs.write("unmodified", "parent".getBytes());
		fs.write("modified", "replaceme".getBytes());
		parent = fs.commit();
		children = new Revision[NUM_CHILDREN];
		
		for(int i = 0; i < NUM_CHILDREN; i++) {
			fs = new ZKFS(storage, password, parent);
			fs.write("modified", "replaced!".getBytes());
			fs.setMtime("modified", 12345l);
			fs.setAtime("modified", 12345l);
			fs.write("child", ("text " + i).getBytes());
			children[i] = fs.commit();
		}
	}
	
	@Test
	public void testDetectsDifferencesBetweenSiblings() throws IOException {
		Revision[] list = new Revision[] { children[0], children[1] };
		DiffSet diffset = new DiffSet(list);
		ArrayList<FileDiff> diffs = diffset.getDiffs();
		
		assertEquals(1, diffs.size());
		
		/* TODO:
		 * This really makes me think deterministic storage should come back. /modified has the same content for
		 * each sibling. It doesn't show up as a difference, and it probably shouldn't, because they're the same.
		 * But if /modified were bigger than the immediate threshold, it would get paged out, and wrappedEncrypt's
		 * nondeterministic behavior will mean that the inodes will get different reftags. This causes a difference
		 * even when none exists.
		 * 
		 * On the other hand, in real life, we would also have differing metadata like mtimes. This would need to
		 * be reconciled. But, with nondeterministic storage, we have no way of knowing it is only the metadata
		 * that differs without decrypting the entirety of both files.
		 */
	}
	
	@Test
	public void testDetectsFakeDifferencesBetweenSiblingsForNonImmediates() throws IOException {
		LocalFS storage = new LocalFS("/tmp/zksync-diffset-nonimmediate");
		if(storage.equals("/")) storage.rmrf("/");
		ZKFS fs = new ZKFS(storage, password);
		byte[] buf = new byte[fs.privConfig.getPageSize()+1];
		fs.write("unmodified", buf);
		fs.write("modified", buf);
		Revision parent = fs.commit();
		Revision[] children = new Revision[2];
		
		for(int i = 0; i < children.length; i++) {
			buf[0] = (byte) i;
			fs = new ZKFS(storage, password, parent);
			fs.write("modified", buf);
			fs.setAtime("modified", 12345l);
			fs.setMtime("modified", 12345l);
			children[i] = fs.commit();
		}

		DiffSet diffset = new DiffSet(children);
		ArrayList<FileDiff> diffs = diffset.getDiffs();
		
		assertEquals(1, diffs.size());
	}
	
	@Test
	public void testDetectsParentChildDifferences() throws IOException {
		Revision[] list = new Revision[] { parent, children[0] };
		DiffSet diffset = new DiffSet(list);
		ArrayList<FileDiff> diffs = diffset.getDiffs();
		
		assertEquals(2, diffs.size());
	}
	
	@Test
	public void testConsidersOriginalAVersionWhenExplicitlyInSet() throws IOException {
		/* if we look at the diffs between two of the children, we should see only one diff, because they both
		 * modify one of the files in the same way.
		 */
		assertEquals(1, (new DiffSet(new Revision[] { children[0], children[1] })).getDiffs().size());
		
		/* but if we include the parent, we should see two diffs, because it has the original unmodified file. */
		assertEquals(2, (new DiffSet(new Revision[] { parent, children[0], children[1] })).getDiffs().size());
	}
	
	@Test
	public void testUnlinksAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			revs[0] = fs.commit();
			fs.unlink(filename);
		});
	}
	
	@Test
	public void testCreationsAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			revs[0] = fs.commit();
			fs.write(filename, "blah".getBytes());
		});
	}
	
	@Test
	public void testMtimesAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.setMtime(filename, 31337l);
		});
	}
	
	@Test
	public void testAtimesAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.setAtime(filename, 31337l);
			fs.setMtime(filename, 0l);
		});
	}
	
	@Test
	public void testCtimesAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			Inode inode = fs.inodeForPath(filename);
			inode.getStat().setCtime(31337l);
			fs.setMtime(filename, 0l);
		});
	}
	
	@Test
	public void testModesAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.chmod(filename, 0004);
			fs.setMtime(filename, 0l);
		});
	}
	
	@Test
	public void testUidsAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.chown(filename, 007);
			fs.setMtime(filename, 0l);
		});
	}
	
	@Test
	public void testUsernamesAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.chown(filename, "bond");
			fs.setMtime(filename, 0l);
		});
	}
	
	@Test
	public void testGidsAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.chown(filename, 6);
			fs.setMtime(filename, 0l);
		});
	}
	
	@Test
	public void testGroupNamesAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.chown(filename, "MI6");
			fs.setMtime(filename, 0l);
		});
	}
	
	@Test
	public void testAppendsAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			ZKFile file = fs.open(filename, ZKFile.O_APPEND|ZKFile.O_RDWR);
			file.write("a".getBytes());
			file.close();
			fs.setMtime(filename, 0l);
		});
	}
	
	@Test
	public void testTruncatesAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			ZKFile file = fs.open(filename, ZKFile.O_APPEND|ZKFile.O_RDWR);
			file.truncate(file.getStat().getSize()-1);
			file.close();
			fs.setMtime(filename, 0l);
		});
	}
	
	@Test
	public void testMajorDevsAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.mknod(filename, Stat.TYPE_BLOCK_DEVICE, 0, 0);
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.inodeForPath(filename).getStat().setDevMajor(1);
		});
	}
	
	@Test
	public void testMinorDevsAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.mknod(filename, Stat.TYPE_BLOCK_DEVICE, 0, 0);
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.inodeForPath(filename).getStat().setDevMinor(1);
		});
	}
	
	@Test
	public void testNlinksAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			revs[0] = fs.commit();
			fs.inodeForPath(filename).nlink++; // painful to look at, isn't it?
		});
	}
	
	@Test
	public void testFileTypesAreADifference() throws IOException {
		trivialDiffTest( (ZKFS fs, Revision[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			revs[0] = fs.commit();
			fs.inodeForPath(filename).getStat().setType(Stat.TYPE_BLOCK_DEVICE);
		});
	}
	
	@Test
	public void testDirectoryEntriesAreADifference() throws IOException {
		String filename = "scratch";
		LocalFS storage = new LocalFS("/tmp/zksync-diffset-" + filename);
		if(storage.equals("/")) storage.rmrf("/");
		ZKFS fs = new ZKFS(storage, password);
		
		Revision[] revs = new Revision[2];
		
		fs.write("file0", "blah".getBytes());
		fs.write("file1", "blah".getBytes());
		fs.inodeForPath("/").stat.setMtime(0);
		fs.inodeForPath("/").stat.setAtime(0);
		revs[0] = fs.commit();
		
		fs.unlink("file1");
		fs.link("file0", "file1");
		fs.inodeForPath("/").stat.setMtime(0);
		fs.inodeForPath("/").stat.setAtime(0);
		revs[1] = fs.commit();

		DiffSet diffset = new DiffSet(revs);
		assertEquals(3, diffset.diffs.size());
	}
	
	// TODO: untested: inode id
	
	protected void trivialDiffTest(DiffExampleLambda meat) throws IOException {
		String filename = "scratch";
		LocalFS storage = new LocalFS("/tmp/zksync-diffset-" + filename);
		if(storage.equals("/")) storage.rmrf("/");
		ZKFS fs = new ZKFS(storage, password);
		
		Revision[] revs = new Revision[2];
		meat.diff(fs, revs, filename);
		revs[1] = fs.commit();

		DiffSet diffset = new DiffSet(revs);
		assertEquals(1, diffset.diffs.size());
		assertEquals(filename, diffset.diffs.get(0).path);
	}
	
	// TODO: applyResolution should create a new revision that is descended from all its parents
	// TODO: applyResolution should be deterministic
	// TODO: isResolved reflects if all diffs are resolved
}
