package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.Stack;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.zkfs.FreeList.FreeListExhaustedException;

public class FreeListTest {
	static ZKMaster master;
	ZKArchive archive;
	ZKFS fs;
	
	@BeforeClass
	public static void beforeClass() throws IOException {
		TestUtils.startDebugMode();
		master = ZKMaster.openBlankTestVolume();
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@Before
	public void beforeEach() throws IOException {
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		if(archive.storage.exists("/")) archive.storage.rmrf("/");
		fs = archive.openBlank();
	}
	
	@After
	public void afterEach() throws IOException {
		fs.close();
		archive.close();
	}
	
	@AfterClass
	public static void afterAll() {
		master.close();
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Test
	public void testUnlinkedInodeIdsAddToFreelist() throws IOException {
		fs.write("test", "foo".getBytes());
		fs.write("placeholder", "bar".getBytes()); // need an inode ID bigger than the one we unlink
		long inodeId = fs.stat("test").getInodeId();
		fs.unlink("test");
		assertEquals((Long) inodeId, fs.inodeTable.freelist.available.pop());
	}
	
	@Test
	public void testIssuedInodeIdsDrawFromFreelist() throws IOException {
		fs.write("test", "foo".getBytes());
		long inodeId = fs.stat("test").getInodeId();
		fs.unlink("test");
		fs.write("test2", "bar".getBytes());
		assertEquals(inodeId, fs.stat("test2").getInodeId());
	}
	
	@Test
	public void testFreelistEmptyAtInit() throws IOException {
		assertEquals(0, fs.inodeTable.freelist.getStat().getSize());
		assertTrue(fs.inodeTable.freelist.available.empty());
	}
	
	@Test
	public void testFreelistDeserialized() throws IOException {
		Stack<Long> witnessedIds = new Stack<Long>();
		int numFiles = 10000; // force a multipage freelist
		for(int i = 0; i < numFiles; i++) fs.write("file"+i, (""+i).getBytes());
		fs.write("placeholder", "placeholder".getBytes()); // ensure the last inode ID is not unlinked
		
		for(int i = 0; i < numFiles; i++) {
			if(i % 2 == 0) continue;
			witnessedIds.push(fs.stat("file"+i).getInodeId());
			fs.unlink("file"+i);
		}
		
		RevisionTag tag = fs.commitAndClose();
		
		fs = tag.getFS();
		for(int i = numFiles-1; i >= 0; i--) {
			if(i % 2 == 0) continue;
			assertEquals(witnessedIds.pop(), (Long) fs.inodeTable.freelist.issueInodeId());
		}
		
		try {
			fs.inodeTable.freelist.issueInodeId();
			throw new RuntimeException("expected empty freelist");
		} catch(FreeListExhaustedException exc) { }
	}
}
