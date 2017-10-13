package com.acrescrypto.zksync.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.Test;

public class DirectoryTestBase {
	protected FS scratch;
	
	@Test
	public void testList() throws IOException {
		scratch.mkdir("list-test");
		scratch.write("list-test/a", "a".getBytes());
		scratch.write("list-test/b", "b".getBytes());
		scratch.write("list-test/c", "c".getBytes());
		
		Directory dir = scratch.opendir("list-test");
		ArrayList<String> entries = new ArrayList<String>();
		for(String entry : dir.list()) entries.add(entry);
		assertEquals(3, entries.size());
		assertTrue(entries.contains("a"));
		assertTrue(entries.contains("b"));
		assertTrue(entries.contains("c"));
	}
	
	@Test
	public void testMkdir() throws IOException {
		Directory dir = scratch.opendir(".");
		assertFalse(scratch.exists("dir-mkdir"));
		dir.mkdir("dir-mkdir");
		assertTrue(scratch.stat("dir-mkdir").isDirectory());
	}
	
	@Test
	public void testLinkByFileHandle() throws IOException {
		scratch.mkdir("linkbydir");
		Directory dir = scratch.opendir("linkbydir");
		File file = scratch.open("linkbydir/a", File.O_WRONLY|File.O_CREAT);
		assertFalse(scratch.exists("linkbydir/b"));
		dir.link(file, "b");
		assertTrue(scratch.exists("linkbydir/b"));
	}
	
	@Test
	public void testLinkByPath() throws IOException {
		scratch.mkdir("linkbypath");
		scratch.write("linkbypath/a", "a".getBytes());
		Directory dir = scratch.opendir("linkbypath");
		dir.link("linkbypath/a", "b");
		assertTrue(scratch.stat("linkbypath/a").getInodeId() == scratch.stat("linkbypath/b").getInodeId());
	}
	
	@Test
	public void testUnlink() throws IOException {
		scratch.mkdir("dir-unlink");
		scratch.write("dir-unlink/doomed", "O! Cruel world!".getBytes());
		assertTrue(scratch.exists("dir-unlink/doomed"));
		scratch.opendir("dir-unlink").unlink("doomed");
		assertFalse(scratch.exists("dir-unlink/doomed"));
	}

}
