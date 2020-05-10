package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PrivateSigningKey;
import com.acrescrypto.zksync.crypto.SignedSecureFile;
import com.acrescrypto.zksync.utility.Util;

public class PageTreeChunkTest {
	class DummyPageTree extends PageTree {
		PageTreeChunk dirtyChunk;
		long requestedIndex = -1;
		
		public DummyPageTree() {
			try {
				archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
			} catch (IOException e) {
				e.printStackTrace();
				fail();
			}
			
			dirtyChunks = new LinkedList<>();
		}
		
		@Override
		public long indexForParent(long index) {
			return parent.index; // return a magic id so we can tell if we're requesting the result of this method
		}
		
		@Override
		public void markDirty(PageTreeChunk chunk) {
			this.dirtyChunk = chunk;
		}
		
		@Override
		public PageTreeChunk chunkAtIndex(long index) {
			requestedIndex = index;
			if(index == chunk.index) return chunk;
			return parent;
		}
	}
	
	CryptoSupport crypto;
	ZKMaster master;
	DummyPageTree tree;
	PageTreeChunk chunk;
	PageTreeChunk parent;
	StorageTag blank;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		crypto = CryptoSupport.defaultCrypto();
		master = ZKMaster.openBlankTestVolume();
		tree = new DummyPageTree();
		
		blank = new StorageTag(crypto, new byte[crypto.hashLength()]);
		parent = new PageTreeChunk(tree, blank, 1234, false);
		chunk = new PageTreeChunk(tree, blank, 1, false);
	}
	
	@After
	public void afterEach() {
		tree.archive.close();
		master.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	StorageTag makeTag(int i) {
		return new StorageTag(crypto, crypto.hash(Util.serializeInt(i)));
	}
	
	@Test
	public void testConstructorInitializesFields() {
		assertEquals(tree, chunk.tree);
		assertEquals(1, chunk.index);
		assertTrue(chunk.chunkTag.isBlank());
		assertNotNull(chunk.tags);
	}
	
	@Test
	public void testInitializesBlankIfBlankTagProvided() {
		for(int i = 0; i < tree.tagsPerChunk(); i++) {
			assertTrue(chunk.getTag(i).isBlank());
		}
	}
	
	@Test
	public void testInitializesWithExistingContentsIfNonBlankTagProvided() throws IOException {
		for(int i = 0; i < tree.tagsPerChunk(); i++) {
			chunk.setTag(i, makeTag(i));
		}
		chunk.write();
		
		PageTreeChunk chunk2 = new PageTreeChunk(tree, chunk.chunkTag, chunk.index, false);
		for(int i = 0; i < tree.tagsPerChunk(); i++) {
			assertArrayEquals(crypto.hash(Util.serializeInt(i)), chunk2.getTag(i).getPaddedTagBytes());
		}
	}
	
	@Test
	public void testHasTagReturnsFalseIfTagForOffsetIsBlank() {
		assertFalse(chunk.hasTag(0));
	}
	
	@Test
	public void testHasTagReturnsTrueIfTagForOffsetIsNonBlank() {
		chunk.setTag(0, makeTag(0));
		assertTrue(chunk.hasTag(0));
	}
	
	@Test
	public void testSetTagUpdatesTag() { // also tests getTag
		for(int i = 0; i < tree.tagsPerChunk(); i++) {
			chunk.setTag(i, makeTag(i));
		}

		for(int i = 0; i < tree.tagsPerChunk(); i++) {
			assertEquals(makeTag(i), chunk.getTag(i));
		}
	}
	
	@Test
	public void testSetTagMarksPageDirtyIfTagDiffersFromPreviousValue() {
		chunk.setTag(0, makeTag(0));
		assertTrue(chunk.dirty);
		assertEquals(chunk, tree.dirtyChunk);
	}
	
	@Test
	public void testSetTagDoesNotMarkPageDirtyIfTagMatchesPreviousValue() {
		chunk.setTag(0, chunk.getTag(0));
		assertFalse(chunk.dirty);
		assertNull(tree.dirtyChunk);
	}
	
	@Test
	public void testMarkDirtySetsDirtyFlag() {
		assertFalse(chunk.dirty);
		chunk.markDirty();
		assertTrue(chunk.dirty);
	}
	
	@Test
	public void testMarkDirtyNotifiesTreeOfDirtyStatus() {
		assertNull(tree.dirtyChunk);
		chunk.markDirty();
		assertNotNull(tree.dirtyChunk);
	}
	
	@Test
	public void testParentReturnsNullIfChunkIsRoot() throws IOException {
		PageTreeChunk root = new PageTreeChunk(tree, blank, 0, false);
		assertNull(root.parent());
	}
	
	@Test
	public void testParentReturnsParentChunkIfNonRoot() throws IOException {
		assertEquals(-1, tree.requestedIndex);
		assertEquals(parent, chunk.parent());
		assertEquals(tree.indexForParent(chunk.index), tree.requestedIndex);
	}
	
	@Test
	public void testWriteClearsDirtyFlag() throws IOException {
		chunk.setTag(0, makeTag(0));
		assertTrue(chunk.dirty);
		chunk.write();
		assertFalse(chunk.dirty);
	}
	
	@Test
	public void testContentsCanBeVerifiedAsSignedSecureFile() throws IOException {
		chunk.setTag(0, makeTag(0));
		chunk.write();
		byte[] chunkData = tree.archive.storage.read(chunk.chunkTag.path());
		assertTrue(tree.archive.config.validatePage(chunk.chunkTag, chunkData));
	}
	
	@Test
	public void testTamperedContentsDoNotValidate() throws IOException {
		chunk.setTag(0, makeTag(0));
		chunk.write();
		byte[] chunkData = tree.archive.storage.read(chunk.chunkTag.path());
		chunkData[1923] ^= 0x04;
		assertFalse(tree.archive.config.validatePage(chunk.chunkTag, chunkData));
	}
	
	@Test
	public void testMismatchedTagsDoNotValidate() throws IOException {
		chunk.setTag(0, makeTag(0));
		chunk.write();
		byte[] oldData = tree.archive.storage.read(chunk.chunkTag.path());
		StorageTag oldTag = chunk.chunkTag;
		
		chunk.setTag(1, makeTag(0));
		chunk.write();
		byte[] newData = tree.archive.storage.read(chunk.chunkTag.path());
		StorageTag newTag = chunk.chunkTag;
		
		assertFalse(tree.archive.config.validatePage(newTag, oldData));
		assertFalse(tree.archive.config.validatePage(oldTag, newData));
	}
	
	@Test(expected=SecurityException.class)
	public void testInitializeWithTamperedFileThrowsException() throws IOException {
		chunk.setTag(0, makeTag(0));
		chunk.write();
		byte[] chunkData = tree.archive.storage.read(chunk.chunkTag.path());
		chunkData[23219] ^= 0x08; // flip some arbitrary bit
		tree.archive.storage.write(chunk.chunkTag.path(), chunkData);
		
		// need to copy the tag to avoid block cache
		StorageTag newTag = new StorageTag(crypto, chunk.chunkTag.getTagBytes());
		new PageTreeChunk(tree, newTag, chunk.index, false).hashCode();
	}
	
	@Test
	public void validatesChunkSignatureIfInitializedWithVerifyTrue() throws IOException {
		chunk.write();
		PrivateSigningKey fakeKey = crypto.makePrivateSigningKey();
		Block block = chunk.chunkTag.loadBlock(tree.archive, false);
		byte[] data = block.serialize();
		StorageTag tag = SignedSecureFile
				  .withParams(tree.archive.storage, block.textKey(), block.saltKey(), block.authKey(), fakeKey)
				  .write(data, tree.archive.config.pageSize + Block.fixedHeaderLength() + Block.indexEntryLength());

		try {
			new PageTreeChunk(tree, tag, chunk.index, true).hashCode();
			fail("Expected SecurityException");
		} catch(SecurityException exc) {}
	}
	
	@Test
	public void doesNotValidateChunkSignatureIfInitializedWithVerifyFalse() throws IOException {
		chunk.write();
		PrivateSigningKey fakeKey = crypto.makePrivateSigningKey();
		Block block = chunk.chunkTag.loadBlock(tree.archive, false);
		byte[] data = block.serialize();
		StorageTag tag = SignedSecureFile
				  .withParams(tree.archive.storage, block.textKey(), block.saltKey(), block.authKey(), fakeKey)
				  .write(data, tree.archive.config.pageSize + Block.fixedHeaderLength() + Block.indexEntryLength());

		new PageTreeChunk(chunk.tree, tag, chunk.index, false).hashCode();
	}
}
