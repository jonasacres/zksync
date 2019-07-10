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

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFile;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSet;


public class DiffSetTest {
	private interface DiffExampleLambda {
		public int diff(ZKFS fs, RevisionTag[] revs, String filename) throws IOException;
	}
	
	RevisionTag parent;
	RevisionTag[] children;
	ZKMaster master;
	char[] password = "zksync".toCharArray();
	
	public final static int NUM_CHILDREN = 4;
	
	@BeforeClass
	public static void beforeClass() {
		TestUtils.startDebugMode();
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@AfterClass
	public static void afterClass() {
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		ZKFS fs = archive.openBlank();
		fs.write("unmodified", "parent".getBytes());
		fs.write("modified", "replaceme".getBytes());
		fs.squash("modified");
		parent = fs.commitAndClose();
		children = new RevisionTag[NUM_CHILDREN];
		
		for(int i = 0; i < NUM_CHILDREN; i++) {
			fs = archive.openRevision(parent);
			fs.write("modified", "replaced!".getBytes());
			fs.write("child", ("text " + i).getBytes()); // don't forget -- making this means / is also changed, so factor that in
			fs.squash("modified");
			children[i] = fs.commit();
			fs.close();
		}
	}
	
	@After
	public void afterEach() throws IOException {
		for(RevisionTag child : children) {
			child.getArchive().close();
		}
		master.close();		
	}
	
	@Test
	public void testDetectsDifferencesBetweenSiblings() throws IOException {
		RevisionTag[] list = new RevisionTag[] { children[0], children[1] };
		DiffSet diffset = new DiffSet(list);
		
		assertEquals(3, diffset.inodeDiffs.size()); // root directory, child, modified
		assertEquals(1, diffset.pathDiffs.size()); // /child
	}
	
	@Test
	public void testDetectsFakeDifferencesBetweenSiblingsForNonImmediates() throws IOException {
		ZKFS fs = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").openBlank();
		byte[] buf = new byte[fs.getArchive().getConfig().getPageSize()+1];
		fs.write("unmodified", buf);
		fs.write("modified", buf);
		RevisionTag parent = fs.commitAndClose();
		RevisionTag[] children = new RevisionTag[2];
		
		for(int i = 0; i < children.length; i++) {
			buf[0] = (byte) i;
			fs = parent.getFS();
			fs.write("modified", buf);
			fs.setAtime("modified", 12345l);
			fs.setMtime("modified", 12345l);
			children[i] = fs.commitAndClose();
		}

		DiffSet diffset = new DiffSet(children);
	
		assertEquals(1, diffset.inodeDiffs.size()); // modified
		assertEquals(0, diffset.pathDiffs.size());
		fs.getArchive().close();
	}
	
	@Test
	public void testDetectsParentChildDifferences() throws IOException {
		RevisionTag[] list = new RevisionTag[] { parent, children[0] };
		DiffSet diffset = new DiffSet(list);
		
		assertEquals(3, diffset.inodeDiffs.size()); // root directory, child, modified
		assertEquals(1, diffset.pathDiffs.size()); // child only
	}
	
	@Test
	public void testConsidersOriginalAVersionWhenExplicitlyInSet() throws IOException {
		/* if we look at the diffs between two of the children, we should see only one diff, because they both
		 * modify one of the files in the same way.
		 */
		assertEquals(3, (new DiffSet(new RevisionTag[] { children[0], children[1] })).inodeDiffs.size());
		
		/* but if we include the parent, we should see two diffs, because it has the original unmodified file. */
		assertEquals(4, (new DiffSet(new RevisionTag[] { parent, children[0], children[1] })).inodeDiffs.size());
	}
	
	@Test
	public void testUnlinksAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			revs[0] = fs.commit();
			fs.unlink(filename);
			return 2;
		});
	}
	
	@Test
	public void testCreationsAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
			revs[0] = fs.commit();
			fs.write(filename, "blah".getBytes());
			return 2;
		});
	}
	
	@Test
	public void testMtimesAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.setMtime(filename, 31337l);
			return 1;
		});
	}
	
	@Test
	public void testAtimesAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
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
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
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
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
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
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
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
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
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
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
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
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
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
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
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
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
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
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
			fs.mknod(filename, Stat.TYPE_BLOCK_DEVICE, 0, 0);
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.inodeForPath(filename).getStat().setDevMajor(1);
			return 1;
		});
	}
	
	@Test
	public void testMinorDevsAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
			fs.mknod(filename, Stat.TYPE_BLOCK_DEVICE, 0, 0);
			fs.setMtime(filename, 0l);
			revs[0] = fs.commit();
			fs.inodeForPath(filename).getStat().setDevMinor(1);
			return 1;
		});
	}
	
	@Test
	public void testFileTypesAreADifference() throws IOException {
		trivialInodeDiffTest( (ZKFS fs, RevisionTag[] revs, String filename) -> {
			fs.write(filename, "blah".getBytes());
			revs[0] = fs.commit();
			fs.inodeForPath(filename).getStat().setType(Stat.TYPE_SYMLINK);
			return 1;
		});
	}
	
	@Test
	public void testDirectoryEntriesAreADifference() throws IOException {
		try(ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		    ZKFS fs = archive.openBlank()
		) {
			RevisionTag[] revs = new RevisionTag[2];
			
			fs.write("file0", "blah".getBytes());
			fs.write("file1", "blah".getBytes());
			revs[0] = fs.commit();
			
			fs.unlink("file1");
			fs.link("file0", "file1"); // 1 path diff and 2 inode diff (file1 and /)
			revs[1] = fs.commit();
	
			DiffSet diffset = new DiffSet(revs);
			assertEquals(2, diffset.inodeDiffs.size()); // file1, /
			assertEquals(1, diffset.pathDiffs.size()); // file1
			assertTrue(diffset.pathDiffs.containsKey("file1"));
		}
	}
	
	protected void trivialInodeDiffTest(DiffExampleLambda meat) throws IOException {
		ZKFS fs = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").openBlank();
		
		RevisionTag[] revs = new RevisionTag[2];
		int numDiffs = meat.diff(fs, revs, "scratch");
		
		revs[1] = fs.commit();
		
		DiffSet diffset = new DiffSet(revs);
		assertEquals(numDiffs, diffset.inodeDiffs.size());
		
		fs.close();
		fs.getArchive().close();
	}
}
