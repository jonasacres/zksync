package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
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
	ZKArchive archive;
	DummyPageTree tree;
	PageTreeChunk chunk;
	PageTreeChunk parent;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		crypto = CryptoSupport.defaultCrypto();
		master = ZKMaster.openBlankTestVolume();
		tree = new DummyPageTree();
		archive = tree.archive;
		
		parent = new PageTreeChunk(tree, DeferrableTag.blank(archive), 1234, false);
		chunk = new PageTreeChunk(tree, DeferrableTag.blank(archive), 1, false);
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
	
	public DeferrableTag makeTag(int i) {
		return new DeferrableTag(archive, crypto.hash(Util.serializeInt(i)));
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
			chunk.setTag(i, DeferrableTag.withBytes(archive, crypto.hash(Util.serializeInt(i))));
		}
		chunk.write();
		
		PageTreeChunk chunk2 = new PageTreeChunk(tree, chunk.chunkTag, chunk.index, false);
		for(int i = 0; i < tree.tagsPerChunk(); i++) {
			assertArrayEquals(crypto.hash(Util.serializeInt(i)), chunk2.getTag(i).getBytes());
		}
	}
	
	@Test
	public void testHasTagReturnsFalseIfTagForOffsetIsBlank() {
		assertFalse(chunk.hasTag(0));
	}
	
	@Test
	public void testHasTagReturnsTrueIfTagForOffsetIsNonBlank() throws IOException {
		chunk.setTag(0, DeferrableTag.withBytes(archive, crypto.hash(Util.serializeInt(0))));
		assertTrue(chunk.hasTag(0));
	}
	
	@Test
	public void testSetTagUpdatesTag() throws IOException { // also tests getTag
		for(int i = 0; i < tree.tagsPerChunk(); i++) {
			chunk.setTag(i, new DeferrableTag(archive, crypto.hash(Util.serializeInt(i))));
		}

		for(int i = 0; i < tree.tagsPerChunk(); i++) {
			assertArrayEquals(crypto.hash(Util.serializeInt(i)), chunk.getTag(i).getBytes());
		}
	}
	
	@Test
	public void testSetTagMarksPageDirtyIfTagDiffersFromPreviousValue() throws IOException {
		chunk.setTag(0, new DeferrableTag(archive, crypto.hash(Util.serializeInt(0))));
		assertTrue(chunk.dirty);
		assertEquals(chunk, tree.dirtyChunk);
	}
	
	@Test
	public void testSetTagDoesNotMarkPageDirtyIfTagMatchesPreviousValue() throws IOException {
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
		DeferrableTag tag = DeferrableTag.blank(archive);
		PageTreeChunk root = new PageTreeChunk(tree, tag, 0, false);
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
		assertTrue(tree.archive.config.validatePage(chunk.chunkTag.getBytes(), chunkData));
	}
	
	@Test
	public void testTamperedContentsDoNotValidate() throws IOException {
		chunk.setTag(0, makeTag(0));
		chunk.write();
		byte[] chunkData = tree.archive.storage.read(chunk.chunkTag.path());
		chunkData[1923] ^= 0x04;
		assertFalse(tree.archive.config.validatePage(chunk.chunkTag.getBytes(), chunkData));
	}
	
	@Test
	public void testMismatchedTagsDoNotValidate() throws IOException {
		chunk.setTag(0, makeTag(0));
		chunk.write();
		byte[] oldData = tree.archive.storage.read(chunk.chunkTag.path());
		byte[] oldTag = chunk.chunkTag.getBytes();
		
		chunk.setTag(1, makeTag(1));
		chunk.write();
		byte[] newData = tree.archive.storage.read(chunk.chunkTag.path());
		byte[] newTag = chunk.chunkTag.getBytes();
		
		assertFalse(tree.archive.config.validatePage(newTag, oldData));
		assertFalse(tree.archive.config.validatePage(oldTag, newData));
	}
	
	@Test(expected=SecurityException.class)
	public void testInitializeWithTamperedFileThrowsException() throws IOException {
		chunk.setTag(0, makeTag(0));
		chunk.write();
		byte[] chunkData = tree.archive.storage.read(chunk.chunkTag.path());
		chunkData[23219] ^= 0x08;
		tree.archive.storage.write(chunk.chunkTag.path(), chunkData);
		
		new PageTreeChunk(tree, chunk.chunkTag, chunk.index, false);
	}
	
	@Test
	public void testTextKeyConsistent() throws IOException {
		PageTreeChunk chunk2 = new PageTreeChunk(tree, chunk.chunkTag, chunk.index, false);
		assertArrayEquals(chunk.textKey().getRaw(), chunk2.textKey().getRaw());
	}
	
	@Test
	public void testTextKeyDependsOnInodeIdentity() {
		Key oldKey = chunk.textKey();
		tree.inodeIdentity++;
		assertFalse(Arrays.equals(oldKey.getRaw(), chunk.textKey().getRaw()));
	}
	
	@Test
	public void testTextKeyDependsOnChunkIndex() throws IOException {
		PageTreeChunk chunk2 = new PageTreeChunk(tree, chunk.chunkTag, chunk.index+1, false);
		assertFalse(Arrays.equals(chunk.textKey().getRaw(), chunk2.textKey().getRaw()));
	}
	
	@Test
	public void validatesChunkSignatureIfInitializedWithVerifyTrue() throws IOException {
		byte[] data = chunk.serialize();
		PrivateSigningKey fakeKey = crypto.makePrivateSigningKey();
		byte[] tagBytes = SignedSecureFile
				  .withParams(tree.archive.storage, chunk.textKey(), chunk.saltKey(), chunk.authKey(), fakeKey)
				  .write(data, tree.archive.config.pageSize);
		DeferrableTag tag = DeferrableTag.withBytes(archive, tagBytes);

		try {
			new PageTreeChunk(tree, tag, chunk.index, true);
			fail("Expected SecurityException");
		} catch(SecurityException exc) {}
	}
	
	@Test
	public void doesNotValidateChunkSignatureIfInitializedWithVerifyFalse() throws IOException {
		byte[] data = chunk.serialize();
		PrivateSigningKey fakeKey = crypto.makePrivateSigningKey();
		byte[] tagBytes = SignedSecureFile
				  .withParams(tree.archive.storage, chunk.textKey(), chunk.saltKey(), chunk.authKey(), fakeKey)
				  .write(data, tree.archive.config.pageSize);
		DeferrableTag tag = DeferrableTag.withBytes(archive, tagBytes);

		new PageTreeChunk(chunk.tree, tag, chunk.index, false);
	}
}
