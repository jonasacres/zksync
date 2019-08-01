package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;

public class BlockManagerTest {
	CryptoSupport crypto;
	ZKMaster master;
	ZKArchive archive;
	BlockManager mgr;
	int maxOpenBlocks = 4;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createDefaultArchive();
		crypto = master.getCrypto();
		mgr = new BlockManager(archive, maxOpenBlocks);
	}
	
	@After
	public void afterEach() throws IOException {
		archive.close();
		master.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	
	@Test
	public void testAddDataCreatesNewBlockIfPendingListIsEmpty() throws IOException {
		byte[] data = new byte[crypto.hashLength()];
		assertEquals(0, mgr.pendingBlocks.size());
		Block block = mgr.addData(0, 0, Block.INDEX_TYPE_PAGE, data, 0, data.length);
		assertEquals(1, mgr.pendingBlocks.size());
		assertNotNull(block.readData(0, 0, Block.INDEX_TYPE_PAGE));
	}
	
	@Test
	public void testAddDataCreatesImmediateBlockWhenDataLessThanHashLength() throws IOException {
		byte[] data = new byte[crypto.hashLength()-1]; // needs to be bigger than an immediate
		Block blockA = mgr.addData(0, 0, Block.INDEX_TYPE_PAGE, data, 0, data.length);
		Block blockB = mgr.addData(1, 0, Block.INDEX_TYPE_PAGE, data, 0, data.length);
		int expectedAvailable = Block.initialCapacity(archive.getConfig())
				- Block.fixedHeaderLength()
				- Block.indexEntryLength()
				- data.length;
		
		assertFalse(blockA.isWritable());
		assertFalse(blockB.isWritable());
		assertNotEquals(blockA, blockB);
		assertEquals(expectedAvailable, blockB.getRemainingCapacity());
		assertNotNull(blockA.readData(0, 0, Block.INDEX_TYPE_PAGE));
		assertNotNull(blockB.readData(1, 0, Block.INDEX_TYPE_PAGE));
		assertEquals(0, mgr.pendingBlocks.size());
	}
	
	@Test
	public void testAddDataAddsToExistingBlockIfSuitableBlockAvailable() throws IOException {
		byte[] data = new byte[crypto.hashLength()]; // needs to be bigger than an immediate
		Block blockA = mgr.addData(0, 0, Block.INDEX_TYPE_PAGE, data, 0, data.length);
		Block blockB = mgr.addData(0, 1, Block.INDEX_TYPE_PAGE, data, 0, data.length);
		int expectedAvailable = Block.initialCapacity(archive.getConfig())
				- Block.fixedHeaderLength()
				- 2*Block.indexEntryLength()
				- 2*data.length;
		
		assertEquals(1, mgr.pendingBlocks.size());
		assertTrue(blockA == blockB);
		assertEquals(expectedAvailable, blockB.getRemainingCapacity());
		assertNotNull(blockA.readData(0, 0, Block.INDEX_TYPE_PAGE));
		assertNotNull(blockA.readData(0, 1, Block.INDEX_TYPE_PAGE));
	}
	
	@Test
	public void testAddDataCreatesNewBlockIfNoExistingBlockHasCapacity() throws IOException {
		byte[] data = new byte[archive.getConfig().getPageSize()/2];
		Block blockA = mgr.addData(0, 0, Block.INDEX_TYPE_PAGE, data, 0, data.length);
		Block blockB = mgr.addData(0, 1, Block.INDEX_TYPE_PAGE, data, 0, data.length);
		
		assertEquals(2, mgr.pendingBlocks.size());
		assertNotEquals(blockA, blockB);
		assertNotNull(blockA.readData(0, 0, Block.INDEX_TYPE_PAGE));
		assertNotNull(blockB.readData(0, 1, Block.INDEX_TYPE_PAGE));
	}
	
	@Test
	public void testAddDataEvictsFullestExistingBlockIfListFullAndNewBlockNeeded() throws IOException {
		byte[] dataA = new byte[archive.getConfig().getPageSize()/2];
		byte[] dataB = new byte[archive.getConfig().getPageSize()/2 + 64];
		byte[] dataC = new byte[archive.getConfig().getPageSize()/2 + 1024]; // block C fullest
		byte[] dataD = new byte[archive.getConfig().getPageSize()/2 + 128];
		byte[] dataE = new byte[archive.getConfig().getPageSize()/2];
		
		Block blockA = mgr.addData(0, 0, Block.INDEX_TYPE_PAGE, dataA, 0, dataA.length);
		Block blockB = mgr.addData(0, 1, Block.INDEX_TYPE_PAGE, dataB, 0, dataB.length);
		Block blockC = mgr.addData(0, 2, Block.INDEX_TYPE_PAGE, dataC, 0, dataC.length);
		Block blockD = mgr.addData(0, 3, Block.INDEX_TYPE_PAGE, dataD, 0, dataD.length);
		Block blockE = mgr.addData(0, 4, Block.INDEX_TYPE_PAGE, dataE, 0, dataE.length);
		
		assertEquals(maxOpenBlocks, mgr.pendingBlocks.size());
		assertTrue(blockA.isWritable());
		assertTrue(blockB.isWritable());
		assertTrue(blockD.isWritable());
		assertTrue(blockE.isWritable());
		assertFalse(blockC.isWritable());
		assertFalse(mgr.pendingBlocks.contains(blockC));
		assertTrue(mgr.pendingBlocks.contains(blockE));
	}
	
	@Test
	public void testAddDataRemovesExistingEntryBeforeCreatingNewBlock() throws IOException {
		byte[] data = new byte[archive.getConfig().getPageSize()/2];
		
		Block blockA = mgr.addData(0, 0, Block.INDEX_TYPE_PAGE, data, 0, data.length);
		Block blockB = mgr.addData(0, 0, Block.INDEX_TYPE_PAGE, data, 0, data.length);
		
		assertTrue(blockA == blockB);
		assertEquals(1, mgr.pendingBlocks.size());
	}
	
	@Test
	public void testWriteAllEvictsAllPendingBlocks() throws IOException {
		byte[] data = new byte[archive.getConfig().getPageSize()/2];
		LinkedList<Block> blocks = new LinkedList<>();
		
		for(int i = 0; i < maxOpenBlocks; i++) {
			blocks.add(mgr.addData(0, i, Block.INDEX_TYPE_PAGE, data, 0, data.length));
		}
		
		mgr.writeAll();
		assertEquals(0, mgr.pendingBlocks.size());
		for(Block block : blocks) {
			assertFalse(block.isWritable());
		}
	}
	
	@Test
	public void testSetMaxOpenBlocksEvictsFullestBlocksAsNeeded() throws IOException {
		Block[] blocks = new Block[maxOpenBlocks];
		byte[][] data = new byte[maxOpenBlocks][];
		
		for(int i = 0; i < maxOpenBlocks; i++) {
			data[i] = new byte[archive.getConfig().getPageSize()/2 + i];
			blocks[i] = mgr.addData(0, i, Block.INDEX_TYPE_PAGE, data[i], 0, data[i].length);
		}
		
		assertEquals(maxOpenBlocks, mgr.pendingBlocks.size());
		mgr.setMaxOpenBlocks(2);
		
		for(int i = 0; i < maxOpenBlocks; i++) {
			if(i < 2) {
				assertTrue(blocks[i].isWritable());
				assertTrue(mgr.pendingBlocks.contains(blocks[i]));
			} else {
				assertFalse(blocks[i].isWritable());
				assertFalse(mgr.pendingBlocks.contains(blocks[i]));
			}
		}
	}
}
