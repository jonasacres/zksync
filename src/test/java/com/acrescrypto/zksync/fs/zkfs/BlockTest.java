package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.BlockDoesNotContainPageException;
import com.acrescrypto.zksync.exceptions.ClosedException;
import com.acrescrypto.zksync.exceptions.InsufficientCapacityException;
import com.acrescrypto.zksync.fs.zkfs.Block.BlockEntryIndex;
import com.acrescrypto.zksync.utility.Shuffler;
import com.acrescrypto.zksync.utility.Util;

public class BlockTest {
	CryptoSupport crypto;
	ZKMaster master;
	ZKArchive archive;
	Block block;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createDefaultArchive();
		crypto = master.getCrypto();
		block = new Block(archive);
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
	
	public void addNonImmediateData() throws IOException {
		byte[] data = crypto.hash(Util.serializeInt(0));
		block.addData(0, 0, Block.INDEX_TYPE_PAGE, data, 0, data.length);
	}
	
	@Test
	public void testAddDataBuffersEntryForStorage() throws IOException {
		byte[] data = crypto.symNonce(0);
		block.addData(0, 0, Block.INDEX_TYPE_PAGE, data, 0, data.length);
		byte[] readBack = block.readData(0, 0, Block.INDEX_TYPE_PAGE);
		
		assertArrayEquals(data, readBack);
	}

	@Test
	public void testAddDataHonorsOffsetAndLength() throws IOException {
		byte[] data = crypto.prng(crypto.symNonce(0)).getBytes(32);
		byte[] slice = new byte[16];
		int offset = 8;
		
		System.arraycopy(data, offset, slice, 0, slice.length);
		
		block.addData(0, 0, Block.INDEX_TYPE_PAGE, data, offset, slice.length);
		byte[] readBack = block.readData(0, 0, Block.INDEX_TYPE_PAGE);
		
		assertArrayEquals(slice, readBack);
	}
	
	@Test
	public void testAddDataDecrementsAvailableCapacity() throws IOException {
		int initRemaining = block.getRemainingCapacity();
		byte[] data = crypto.prng(crypto.symNonce(0)).getBytes(32);
		block.addData(0, 0, Block.INDEX_TYPE_PAGE, data, 0, data.length);
		
		int expectedRemaining = initRemaining - data.length - Block.indexEntryLength();
		assertEquals(expectedRemaining, block.getRemainingCapacity());
	}
	
	@Test(expected=ClosedException.class)
	public void testAddDataThrowsExceptionAfterWriteCalled() throws IOException {
		addNonImmediateData();
		block.write();
		byte[] data = new byte[0];
		block.addData(0, 0, Block.INDEX_TYPE_PAGE, data, 0, data.length);
	}
	
	@Test(expected=InsufficientCapacityException.class)
	public void testAddDataThrowsExceptionIfDataDoesNotFit() throws IOException {
		byte[] data = crypto.prng(crypto.symNonce(0)).getBytes(block.getRemainingCapacity()+1);
		block.addData(0, 0, Block.INDEX_TYPE_PAGE, data, 0, data.length);
	}
	
	@Test
	public void testAddDataOverwritesPreviousEntry() throws IOException {
		byte[] dataA = crypto.prng(crypto.symNonce(0)).getBytes(64);
		byte[] dataB = crypto.prng(crypto.symNonce(1)).getBytes(32);
		int expectedRemaining = block.getRemainingCapacity()
				- Block.indexEntryLength()
				- dataB.length;
		block.addData(0, 0, Block.INDEX_TYPE_PAGE, dataA, 0, dataA.length);
		block.addData(0, 0, Block.INDEX_TYPE_PAGE, dataB, 0, dataB.length);
		assertEquals(expectedRemaining, block.getRemainingCapacity());
		assertArrayEquals(dataB, block.readData(0, 0, Block.INDEX_TYPE_PAGE));
	}
	
	@Test
	public void testAddDataDoesNotOverwriteEntriesDifferingByIdentity() throws IOException {
		byte[] dataA = crypto.prng(crypto.symNonce(0)).getBytes(64);
		byte[] dataB = crypto.prng(crypto.symNonce(1)).getBytes(32);
		int expectedRemaining = block.getRemainingCapacity()
				- 2*Block.indexEntryLength()
				- dataA.length
				- dataB.length;
		block.addData(0, 0, Block.INDEX_TYPE_PAGE, dataA, 0, dataA.length);
		block.addData(1, 0, Block.INDEX_TYPE_PAGE, dataB, 0, dataB.length);
		assertEquals(expectedRemaining, block.getRemainingCapacity());
		assertArrayEquals(dataA, block.readData(0, 0, Block.INDEX_TYPE_PAGE));
		assertArrayEquals(dataB, block.readData(1, 0, Block.INDEX_TYPE_PAGE));
	}
	
	@Test
	public void testAddDataDoesNotOverwriteEntriesDifferingByIndex() throws IOException {
		byte[] dataA = crypto.prng(crypto.symNonce(0)).getBytes(64);
		byte[] dataB = crypto.prng(crypto.symNonce(1)).getBytes(32);
		int expectedRemaining = block.getRemainingCapacity()
				- 2*Block.indexEntryLength()
				- dataA.length
				- dataB.length;
		block.addData(0, 0, Block.INDEX_TYPE_PAGE, dataA, 0, dataA.length);
		block.addData(0, 1, Block.INDEX_TYPE_PAGE, dataB, 0, dataB.length);
		assertEquals(expectedRemaining, block.getRemainingCapacity());
		assertArrayEquals(dataA, block.readData(0, 0, Block.INDEX_TYPE_PAGE));
		assertArrayEquals(dataB, block.readData(0, 1, Block.INDEX_TYPE_PAGE));
	}
	
	@Test
	public void testAddDataDoesNotOverwriteEntriesDifferingByType() throws IOException {
		byte[] dataA = crypto.prng(crypto.symNonce(0)).getBytes(64);
		byte[] dataB = crypto.prng(crypto.symNonce(1)).getBytes(32);
		int expectedRemaining = block.getRemainingCapacity()
				- 2*Block.indexEntryLength()
				- dataA.length
				- dataB.length;
		block.addData(0, 0, Block.INDEX_TYPE_PAGE, dataA, 0, dataA.length);
		block.addData(0, 0, Block.INDEX_TYPE_CHUNK, dataB, 0, dataB.length);
		assertEquals(expectedRemaining, block.getRemainingCapacity());
		assertArrayEquals(dataA, block.readData(0, 0, Block.INDEX_TYPE_PAGE));
		assertArrayEquals(dataB, block.readData(0, 0, Block.INDEX_TYPE_CHUNK));
	}
	
	@Test(expected=BlockDoesNotContainPageException.class)
	public void testReadDataThrowsExceptionIfEntryNotInBufferWhenWritable() throws IOException {
		block.readData(0, 0, Block.INDEX_TYPE_PAGE);
	}
	
	@Test(expected=BlockDoesNotContainPageException.class)
	public void testReadDataThrowsExceptionIfEntryNotInBufferWhenConstructedFromTag() throws IOException {
		addNonImmediateData();
		byte[] data = crypto.prng(crypto.symNonce(0)).getBytes(32);
		block.addData(0, 0, Block.INDEX_TYPE_CHUNK, data, 0, data.length);
		block.write();
		
		Block fromTag = new Block(archive, block.getStorageTag(), false);
		fromTag.readData(1, 0, Block.INDEX_TYPE_CHUNK);
	}
	
	@Test
	public void testReadDataReturnsDataFromWhenConstructedFromTag() throws IOException {
		addNonImmediateData();
		byte[] data = crypto.prng(crypto.symNonce(0)).getBytes(32);
		block.addData(0, 0, Block.INDEX_TYPE_CHUNK, data, 0, data.length);
		block.write();
		
		Block fromTag = new Block(archive, block.storageTag, false);
		assertArrayEquals(data, fromTag.readData(0, 0, Block.INDEX_TYPE_CHUNK));
	}
	
	@Test
	public void testSerializeOrdersItemsByIdentityTypeAndIndex() throws IOException {
		BlockEntryIndex[] indices = new BlockEntryIndex[] {
			block.new BlockEntryIndex(0, 0,  Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 2,  Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 10, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 0,  Block.INDEX_TYPE_CHUNK),
			block.new BlockEntryIndex(0, 1,  Block.INDEX_TYPE_CHUNK),
			block.new BlockEntryIndex(0, -20, Block.INDEX_TYPE_CHUNK),
			block.new BlockEntryIndex(0, 0, (byte) -1),
			block.new BlockEntryIndex(10, 0,  Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(-10, 2,  Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(-10, 10, Block.INDEX_TYPE_PAGE),
		};
		
		Shuffler shuffler = Shuffler.fixedShuffler(indices.length);
		
		byte[] data = new byte[0];
		while(shuffler.hasNext()) {
			BlockEntryIndex idx = indices[shuffler.next()];
			block.addData(idx.identity, idx.pageNum, idx.type, data, 0, data.length);
		}
		
		ByteBuffer buf = ByteBuffer.wrap(block.serialize());
		assertEquals(indices.length, buf.getLong());
		assertEquals(0, buf.getLong());
		for(BlockEntryIndex idx : indices) {
			byte[] reserved = new byte[7];
			assertEquals(idx.identity, buf.getLong());
			buf.get(reserved);
			assertArrayEquals(new byte[7], reserved);
			assertEquals(idx.type, buf.get());
			assertEquals(idx.pageNum, buf.getLong());
			buf.getLong(); // not validating length here
		}
	}
	
	@Test
	public void testCanFitDataReturnsTrueIfBufferHasCapacityForEntry() {
		assertTrue(block.canFitData(block.remainingCapacity - Block.indexEntryLength()));
	}
	
	@Test
	public void testCanFitDataReturnsFalseIfBufferDoesNotHaveCapacityForEntry() {
		assertFalse(block.canFitData(block.remainingCapacity+1));
	}

	@Test
	public void testCanFitDataReturnsFalseIfBufferHasCapacityForEntryButNotHeader() {
		assertFalse(block.canFitData(block.remainingCapacity - Block.indexEntryLength() + 1));
	}
	
	@Test
	public void testWriteCreatesEncryptedFileOnDisk() throws IOException {
		addNonImmediateData();
		block.write();
		assertTrue(archive.getStorage().exists(block.getStorageTag().path()));
	}
	
	@Test
	public void testWriteCreatesFileOfEqualLengthToArchiveConfig() throws IOException {
		addNonImmediateData();
		block.write();
		String configPath = archive.getConfig().tag().path();
		long blockSize = archive.getStorage().stat(block.getStorageTag().path()).getSize();
		long configSize = archive.getStorage().storageSize(configPath);
		assertEquals(configSize, blockSize);
	}

	@Test
	public void testWriteOutputDoesNotDependOnContentLength() throws IOException {
		int len = block.getRemainingCapacity() - Block.indexEntryLength();
		byte[] data = crypto.prng(crypto.symNonce(0)).getBytes(len);
		block.addData(0, 0, Block.INDEX_TYPE_PAGE, data, 0, data.length);
		block.write();
		String configPath = archive.getConfig().tag().path();
		long blockSize = archive.getStorage().stat(block.getStorageTag().path()).getSize();
		long configSize = archive.getStorage().storageSize(configPath);
		assertEquals(configSize, blockSize);
	}
	
	@Test
	public void testWriteSetsBytesInDeferrableTag() throws IOException {
		assertFalse(block.getStorageTag().isFinalized());
		block.write();
		assertTrue(block.getStorageTag().isFinalized());
	}
	
	@Test
	public void testIsWritableReturnsFalseAfterWriteCalled() throws IOException {
		addNonImmediateData();
		assertTrue(block.isWritable());
		block.write();
		assertFalse(block.isWritable());
	}
	
	@Test
	public void testIsWritableReturnsFalseWhenInitializedFromTag() throws IOException {
		addNonImmediateData();
		block.write();
		Block fromTag = new Block(archive, block.getStorageTag(), false);
		assertFalse(fromTag.isWritable());
	}
	
	@Test
	public void testIsWritableReturnsFalseAfterDeferrableTagFinalized() throws IOException {
		addNonImmediateData();
		block.getStorageTag().getTagBytes();
		assertFalse(block.isWritable());
	}
	
	@Test
	public void testEqualsReturnsTrueForAbsoluteEquality() throws IOException {
		assertTrue(block.equals(block));
	}

	@Test
	public void testEqualsReturnsTrueIfBlockContainsIdenticalEntries() throws IOException {
		BlockEntryIndex[] indices = new BlockEntryIndex[] {
			block.new BlockEntryIndex(0, 0, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 1, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 2, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 0, Block.INDEX_TYPE_CHUNK),
			block.new BlockEntryIndex(1, 0, Block.INDEX_TYPE_PAGE),
		};
		
		byte[] data = new byte[0];
		Block other = new Block(archive);
		for(BlockEntryIndex idx : indices) {
			block.addData(idx.identity, idx.pageNum, idx.type, data, 0, data.length);
			other.addData(idx.identity, idx.pageNum, idx.type, data, 0, data.length);
		}
		
		assertTrue(other.equals(block));
		assertTrue(block.equals(other));
	}

	@Test
	public void testEqualsReturnsFalseIfBlockEntryDiffersByContent() throws IOException {
		BlockEntryIndex[] indices = new BlockEntryIndex[] {
			block.new BlockEntryIndex(0, 0, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 1, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 2, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 0, Block.INDEX_TYPE_CHUNK),
			block.new BlockEntryIndex(1, 0, Block.INDEX_TYPE_PAGE),
		};
		
		byte[] dataA = new byte[1], dataB = new byte[] { 0x01 };
		Block other = new Block(archive);
		boolean substituted = false;
		for(BlockEntryIndex idx : indices) {
			block.addData(idx.identity, idx.pageNum, idx.type, dataA, 0, dataA.length);
			byte[] oData = substituted ? dataA : dataB;
			substituted = true;
			other.addData(idx.identity, idx.pageNum, idx.type, oData, 0, oData.length);
		}
		
		assertFalse(other.equals(block));
		assertFalse(block.equals(other));
	}
	
	@Test
	public void testEqualsReturnsFalseIfBlockEntryDiffersByAdditionalEntry() throws IOException {
		BlockEntryIndex[] indices = new BlockEntryIndex[] {
			block.new BlockEntryIndex(0, 0, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 1, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 2, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 0, Block.INDEX_TYPE_CHUNK),
			block.new BlockEntryIndex(1, 0, Block.INDEX_TYPE_PAGE),
		};
		
		byte[] data = new byte[0];
		Block other = new Block(archive);
		boolean skipped = false;
		for(BlockEntryIndex idx : indices) {
			block.addData(idx.identity, idx.pageNum, idx.type, data, 0, data.length);
			if(skipped) {
				other.addData(idx.identity, idx.pageNum, idx.type, data, 0, data.length);
			}
			skipped = true;
		}
		
		assertFalse(other.equals(block));
		assertFalse(block.equals(other));
	}
	
	@Test
	public void testEqualsReturnsFalseIfBlockEntryDiffersByType() throws IOException {
		BlockEntryIndex[] indices = new BlockEntryIndex[] {
			block.new BlockEntryIndex(0, 0, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 1, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 2, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 0, Block.INDEX_TYPE_CHUNK),
			block.new BlockEntryIndex(1, 0, Block.INDEX_TYPE_PAGE),
		};
		
		byte[] data = new byte[0];
		Block other = new Block(archive);
		boolean swapped = false;
		for(BlockEntryIndex idx : indices) {
			byte type = -1;
			block.addData(idx.identity, idx.pageNum, idx.type, data, 0, data.length);
			if(swapped) {
				type = idx.type;
			}

			other.addData(idx.identity, idx.pageNum, type, data, 0, data.length);
			swapped = true;
		}
		
		assertFalse(other.equals(block));
		assertFalse(block.equals(other));
	}
	
	@Test
	public void testEqualsReturnsFalseIfBlockEntryDiffersByIdentity() throws IOException {
		BlockEntryIndex[] indices = new BlockEntryIndex[] {
			block.new BlockEntryIndex(0, 0, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 1, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 2, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 0, Block.INDEX_TYPE_CHUNK),
			block.new BlockEntryIndex(1, 0, Block.INDEX_TYPE_PAGE),
		};
		
		byte[] data = new byte[0];
		Block other = new Block(archive);
		boolean swapped = false;
		for(BlockEntryIndex idx : indices) {
			long identity = -1;
			block.addData(idx.identity, idx.pageNum, idx.type, data, 0, data.length);
			if(swapped) {
				identity = idx.identity;
			}

			other.addData(identity, idx.pageNum, idx.type, data, 0, data.length);
			swapped = true;
		}
		
		assertFalse(other.equals(block));
		assertFalse(block.equals(other));
	}
	
	@Test
	public void testEqualsReturnsFalseIfBlockEntryDiffersByIndex() throws IOException {
		BlockEntryIndex[] indices = new BlockEntryIndex[] {
			block.new BlockEntryIndex(0, 0, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 1, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 2, Block.INDEX_TYPE_PAGE),
			block.new BlockEntryIndex(0, 0, Block.INDEX_TYPE_CHUNK),
			block.new BlockEntryIndex(1, 0, Block.INDEX_TYPE_PAGE),
		};
		
		byte[] data = new byte[0];
		Block other = new Block(archive);
		boolean swapped = false;
		for(BlockEntryIndex idx : indices) {
			long index = -1;
			block.addData(idx.identity, idx.pageNum, idx.type, data, 0, data.length);
			if(swapped) {
				index = idx.pageNum;
			}

			other.addData(idx.identity, index, idx.type, data, 0, data.length);
			swapped = true;
		}
		
		assertFalse(other.equals(block));
		assertFalse(block.equals(other));
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEqualsReturnsFalseForNonBlock() {
		assertFalse(block.equals(null));
		assertFalse(block.equals(""));
	}
}
