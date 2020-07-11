package com.acrescrypto.zksync.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import com.acrescrypto.zksync.exceptions.EEXISTSException;

public class DirectoryTestBase {
	protected FS scratch;
	
	@Test
	public void testList() throws IOException {
		scratch.mkdir("list-test");
		scratch.write("list-test/a", "a".getBytes());
		scratch.write("list-test/b", "b".getBytes());
		scratch.write("list-test/c", "c".getBytes());
		scratch.mkdir("list-test/d");
		
		Directory dir = scratch.opendir("list-test");
		ArrayList<String> entries = new ArrayList<String>();
		for(String entry : dir.list()) entries.add(entry);
		assertEquals(4, entries.size());
		assertTrue(entries.contains("a"));
		assertTrue(entries.contains("b"));
		assertTrue(entries.contains("c"));
		assertTrue(entries.contains("d"));
		dir.close();
	}
	
	@Test
	public void testListOmitDirectories() throws IOException {
		scratch.mkdir("list-omitdir-test");
		scratch.write("list-omitdir-test/a", "a".getBytes());
		scratch.write("list-omitdir-test/b", "b".getBytes());
		scratch.write("list-omitdir-test/c", "c".getBytes());
		scratch.mkdir("list-omitdir-test/d");
		
		Directory dir = scratch.opendir("list-omitdir-test");
		ArrayList<String> entries = new ArrayList<String>();
		for(String entry : dir.list(Directory.LIST_OPT_OMIT_DIRECTORIES)) entries.add(entry);
		assertEquals(3, entries.size());
		assertTrue(entries.contains("a"));
		assertTrue(entries.contains("b"));
		assertTrue(entries.contains("c"));
		dir.close();
	}
	
	@Test
	public void testListIncludeDotAndDotDot() throws IOException {
		scratch.mkdir("list-.-..-test");
		scratch.write("list-.-..-test/a", "a".getBytes());
		scratch.write("list-.-..-test/b", "b".getBytes());
		scratch.write("list-.-..-test/c", "c".getBytes());
		scratch.mkdir("list-.-..-test/d");
		
		Directory dir = scratch.opendir("list-.-..-test");
		ArrayList<String> entries = new ArrayList<String>();
		for(String entry : dir.list(Directory.LIST_OPT_INCLUDE_DOT_DOTDOT)) entries.add(entry);

		assertEquals(6, entries.size());
		assertTrue(entries.contains("."));
		assertTrue(entries.contains(".."));
		assertTrue(entries.contains("a"));
		assertTrue(entries.contains("b"));
		assertTrue(entries.contains("c"));
		assertTrue(entries.contains("d"));
		dir.close();
	}
	
	@Test
	public void testListRecursive() throws IOException {
		String[] files = { "listr/files/a/1", "listr/files/a/2", "listr/files/a/3", "listr/files/b/1", "listr/files/b/2", "listr/files/b/3" };
		String[] expected = { "files", "files/a", "files/a/1", "files/a/2", "files/a/3", "files/b", "files/b/1", "files/b/2", "files/b/3" };
		
		for(String file : files) {
			scratch.write(file, "foo".getBytes());
		}
		
		Collection<String> listed = scratch.opendir("listr").listRecursive();
		Set<String> listSet = new HashSet<String>();
		for(String listItem : listed) listSet.add(listItem);
		
		assertEquals(expected.length, listSet.size());
		for(String expectedItem : expected) assertTrue(listSet.contains(expectedItem));
	}

	@Test
	public void testListRecursiveOmitDirectories() throws IOException {
		String[] files = { "listr/files/a/1", "listr/files/a/2", "listr/files/a/3", "listr/files/b/1", "listr/files/b/2", "listr/files/b/3" };
		String[] expected = { "files/a/1", "files/a/2", "files/a/3", "files/b/1", "files/b/2", "files/b/3" };
		
		for(String file : files) {
			scratch.write(file, "foo".getBytes());
		}
		
		Collection<String> listed = scratch.opendir("listr").listRecursive(Directory.LIST_OPT_OMIT_DIRECTORIES);
		Set<String> listSet = new HashSet<String>();
		for(String listItem : listed) listSet.add(listItem);
		
		assertEquals(expected.length, listSet.size());
		for(String expectedItem : expected) assertTrue(listSet.contains(expectedItem));
	}
	
	@Test
	public void testListRecursiveIncludesDotAndDotDot() throws IOException {
		String[] files = { "listr/files/a/1", "listr/files/a/2", "listr/files/a/3", "listr/files/b/1", "listr/files/b/2", "listr/files/b/3" };
		String[] expected = { ".", "..", "files", "files/.", "files/..", "files/a", "files/a/.", "files/a/..", "files/a/1", "files/a/2", "files/a/3", "files/b", "files/b/.", "files/b/..", "files/b/1", "files/b/2", "files/b/3" };
		
		for(String file : files) {
			scratch.write(file, "foo".getBytes());
		}
		
		Collection<String> listed = scratch.opendir("listr").listRecursive(Directory.LIST_OPT_INCLUDE_DOT_DOTDOT);
		Set<String> listSet = new HashSet<String>();
		for(String listItem : listed) listSet.add(listItem);
		
		assertEquals(listSet.size(), listed.size()); // no dupes
		assertEquals(expected.length, listSet.size());
		
		for(String expectedItem : expected) {
			assertTrue(listSet.contains(expectedItem));
		}
	}
	
	@Test
	public void testWalkCallsBackForEachEntry() throws IOException {
		String[] files = { "listr/files/a/1", "listr/files/a/2", "listr/files/a/3", "listr/files/b/1", "listr/files/b/2", "listr/files/b/3" };
		String[] expectedArray = { "files", "files/a", "files/a/1", "files/a/2", "files/a/3", "files/b", "files/b/1", "files/b/2", "files/b/3" };
		HashSet<String> expected = new HashSet<>();
		
		for(String expectedPath : expectedArray) {
			expected.add(expectedPath);
		}
		
		for(String file : files) {
			scratch.write(file, "foo".getBytes());
		}
		
		try(Directory dir = scratch.opendir("listr")) {
			dir.walk((path, stat, isBrokenSymlink, parent)->{
				assertTrue(expected.contains(path));
				assertFalse(isBrokenSymlink);
				assertEquals(stat, scratch.stat("listr/" + path));
				expected.remove(path);
			});
		}
		
		assertTrue(expected.isEmpty());
	}
	
	@Test
	public void testWalkCallsBackForEachEntryExceptDirectoriesWhenOmitDirectoriesSpecified() throws IOException {
		String[] files = { "listr/files/a/1", "listr/files/a/2", "listr/files/a/3", "listr/files/b/1", "listr/files/b/2", "listr/files/b/3" };
		String[] expectedArray = { "files/a/1", "files/a/2", "files/a/3", "files/b/1", "files/b/2", "files/b/3" };
		HashSet<String> expected = new HashSet<>();
		
		for(String expectedPath : expectedArray) {
			expected.add(expectedPath);
		}
		
		for(String file : files) {
			scratch.write(file, "foo".getBytes());
		}
		
		try(Directory dir = scratch.opendir("listr")) {
			dir.walk(Directory.LIST_OPT_OMIT_DIRECTORIES, (path, stat, isBrokenSymlink, parent)->{
				assertTrue(expected.contains(path));
				assertFalse(isBrokenSymlink);
				assertEquals(stat, scratch.stat("listr/" + path));
				expected.remove(path);
			});
		}
		
		assertTrue(expected.isEmpty());
	}
	
	@Test
	public void testWalkCallsBackForEachEntryIncludingDotAndDotDotIfRequested() throws IOException {
		String[] files = { "listr/files/a/1", "listr/files/a/2", "listr/files/a/3", "listr/files/b/1", "listr/files/b/2", "listr/files/b/3" };
		String[] expectedArray = { ".", "..", "files", "files/.", "files/..", "files/a", "files/a/.", "files/a/..", "files/a/1", "files/a/2", "files/a/3", "files/b", "files/b/.", "files/b/..", "files/b/1", "files/b/2", "files/b/3" };
		HashSet<String> expected = new HashSet<>();
		
		for(String expectedPath : expectedArray) {
			expected.add(expectedPath);
		}
		
		for(String file : files) {
			scratch.write(file, "foo".getBytes());
		}
		
		try(Directory dir = scratch.opendir("listr")) {
			dir.walk(Directory.LIST_OPT_INCLUDE_DOT_DOTDOT, (path, stat, isBrokenSymlink, parent)->{
				assertTrue(expected.contains(path));
				assertFalse(isBrokenSymlink);
				assertEquals(stat, scratch.stat("listr/" + path));
				expected.remove(path);
			});
		}
		
		assertTrue(expected.isEmpty());
	}
	
	@Test
	public void testWalkSetsBrokenSymlinkFlagWhenAppropriate() throws IOException {
		scratch.write("foo", "yadda".getBytes());
		scratch.symlink("foo", "valid");
		scratch.symlink("bar", "invalid");
		
		try(Directory dir = scratch.opendir("/")) {
			dir.walk(0, (path, stat, isBrokenSymlink, parent)->{
				assertEquals(path.equals("invalid"), isBrokenSymlink);
			});
		}
	}
	
	@Test
	public void testWalkFollowsSymlinksByDefault() throws IOException {
		scratch.write("a/1", "yadda".getBytes());
		scratch.symlink("a", "b");
		
		HashSet<String> seen = new HashSet<>();
		
		try(Directory dir = scratch.opendir("/")) {
			dir.walk(0, (path, stat, isBrokenSymlink, parent)->{
				assertFalse(isBrokenSymlink);
				seen.add(path);
			});
		}
		
		assertTrue(seen.contains("a"));
		assertTrue(seen.contains("b"));
		assertTrue(seen.contains("a/1"));
		assertTrue(seen.contains("b/1"));
	}
	
	@Test
	public void testWalkDoesNotFollowSymlinksWhenDontFollowSymlinksSpecified() throws IOException {
		scratch.write("a/1", "yadda".getBytes());
		scratch.symlink("a", "b");
		
		HashSet<String> seen = new HashSet<>();
		
		try(Directory dir = scratch.opendir("/")) {
			dir.walk(Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS, (path, stat, isBrokenSymlink, parent)->{
				assertFalse(isBrokenSymlink);
				seen.add(path);
			});
		}
		
		assertTrue(seen.contains("a"));
		assertTrue(seen.contains("b"));
		assertTrue(seen.contains("a/1"));
		assertFalse(seen.contains("b/1"));
	}
	
	@Test
	public void testWalkIncludesParentDirectory() throws IOException {
		scratch.write("a/1", "yadda".getBytes());
		
		try(Directory dir = scratch.opendir("/")) {
			dir.walk(Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS, (path, stat, isBrokenSymlink, parent)->{
				FSPath normPath       = scratch.absolutePath(scratch.dirname(path)),
					   normParentPath = scratch.absolutePath(parent.getPath());
				
				assertEquals(
						normPath,
						normParentPath);
			});
		}
	}

	@Test
	public void testMkdir() throws IOException {
		Directory dir = scratch.opendir(".");
		assertFalse(scratch.exists("dir-mkdir"));
		dir.mkdir("dir-mkdir");
		dir.close();
		assertTrue(scratch.stat("dir-mkdir").isDirectory());
	}
	
	@Test
	public void testLinkByFileHandle() throws IOException {
		scratch.mkdir("linkbydir");
		Directory dir = scratch.opendir("linkbydir");
		try(File file = scratch.open("linkbydir/a", File.O_WRONLY|File.O_CREAT)) {
			assertFalse(scratch.exists("linkbydir/b"));
			dir.link(file, "b");
			dir.close();
			assertTrue(scratch.exists("linkbydir/b"));
		}
	}
	
	@Test
	public void testLinkByPath() throws IOException {
		scratch.mkdir("linkbypath");
		scratch.write("linkbypath/a", "a".getBytes());
		Directory dir = scratch.opendir("linkbypath");
		dir.link("linkbypath/a", "b");
		dir.close();
		assertTrue(scratch.stat("linkbypath/a").getInodeId() == scratch.stat("linkbypath/b").getInodeId());
	}
	
	@Test(expected=EEXISTSException.class)
	public void testLinkThrowsEEXISTS() throws IOException {
		scratch.write("linkeexists/a", "a".getBytes());
		Directory dir = scratch.opendir("linkeexists");
		dir.link("linkeexists/a", "b");
		dir.link("linkeexists/a", "b");
	}
	
	@Test
	public void testUnlink() throws IOException {
		scratch.mkdir("dir-unlink");
		scratch.write("dir-unlink/doomed", "O! Cruel world!".getBytes());
		assertTrue(scratch.exists("dir-unlink/doomed"));
		
		Directory dir = scratch.opendir("dir-unlink");
		dir.unlink("doomed");
		dir.close();
		
		assertFalse(scratch.exists("dir-unlink/doomed"));
	}
	
	@Test
	public void testContainsPositive() throws IOException {
		scratch.write("dir-contains-positive/a", "a".getBytes());
		Directory dir = scratch.opendir("dir-contains-positive");
		assertTrue(dir.contains("a"));
		dir.close();
	}
	
	@Test
	public void testContainsNegative() throws IOException {
		scratch.write("dir-contains-negative/a", "a".getBytes());
		Directory dir = scratch.opendir("dir-contains-negative");
		assertFalse(dir.contains("b"));
		dir.close();
	}
}
