package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.security.Security;
import java.util.Stack;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.fs.zkfs.FreeList.FreeListExhaustedException;

public class FreeListTest {
	static ZKMaster master;
	ZKArchive archive;
	ZKFS fs;
	
	@BeforeClass
	public static void beforeClass() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		ZKFSTest.cheapenArgon2Costs();
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@Before
	public void before() throws IOException {
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		if(archive.storage.exists("/")) archive.storage.rmrf("/");
		fs = archive.openBlank();
	}
	
	@Test
	public void testUnlinkedInodeIdsAddToFreelist() throws IOException {
		fs.write("test", "foo".getBytes());
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
		int size = 10000; // force a multipage freelist
		for(int i = 0; i < size; i++) fs.write("file"+i, (""+i).getBytes());
		for(int i = 0; i < size; i++) {
			if(i % 2 == 0) continue;
			witnessedIds.push(fs.stat("file"+i).getInodeId());
			fs.unlink("file"+i);
		}
		
		RefTag tag = fs.commit();
		
		fs = tag.getFS();
		for(int i = size-1; i >= 0; i--) {
			if(i % 2 == 0) continue;
			assertEquals(witnessedIds.pop(), (Long) fs.inodeTable.freelist.issueInodeId());
		}
		
		try {
			fs.inodeTable.freelist.issueInodeId();
			throw new RuntimeException("expected empty freelist");
		} catch(FreeListExhaustedException exc) { }
	}
}
