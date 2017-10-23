package com.acrescrypto.zksync.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
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
		
		String[] listed = scratch.opendir("listr").listRecursive();
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
		
		String[] listed = scratch.opendir("listr").listRecursive(Directory.LIST_OPT_OMIT_DIRECTORIES);
		Set<String> listSet = new HashSet<String>();
		for(String listItem : listed) listSet.add(listItem);
		
		assertEquals(expected.length, listSet.size());
		for(String expectedItem : expected) assertTrue(listSet.contains(expectedItem));
	}
	
	// TODO: recursive list include . ..

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
		File file = scratch.open("linkbydir/a", File.O_WRONLY|File.O_CREAT);
		assertFalse(scratch.exists("linkbydir/b"));
		dir.link(file, "b");
		dir.close();
		assertTrue(scratch.exists("linkbydir/b"));
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
	
	// TODO: Test EEXISTS on collisions
	
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

}
