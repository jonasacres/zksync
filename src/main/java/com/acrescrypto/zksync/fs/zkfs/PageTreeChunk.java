package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.SignedSecureFile;
import com.acrescrypto.zksync.utility.Util;

public class PageTreeChunk {
	protected PageTree tree;
	byte[] chunkTag;
	ArrayList<byte[]> tags;
	long index;
	protected boolean dirty;
	
	public PageTreeChunk(PageTree tree, byte[] chunkTag, long index, boolean verify) throws IOException {
		this.index = index;
		this.chunkTag = chunkTag;
		this.tree = tree;
		this.tags = new ArrayList<>(tree.tagsPerChunk());
		
		if(isZero(chunkTag)) {
			initBlank();
		} else {
			read(verify);
		}
	}
	
	public boolean hasTag(long offset) {
		return !isZero(getTag(offset));
	}
	
	public void setTag(long offset, byte[] tag) {
		if(Arrays.equals(tag, tags.get((int) offset))) return;
		
		loadTag(offset, tag);
		if(tag.length >= tree.archive.getCrypto().hashLength() && !isZero(tag)) {
			tree.archive.addPageTag(tag);
		}
		
		markDirty();
	}
	
	public void loadTag(long offset, byte[] tag) {
		tags.set((int) offset, tag);
	}
	
	public byte[] getTag(long offset) {
		return tags.get((int) offset);
	}
	
	public void markDirty() {
		dirty = true;
		tree.markDirty(this);
	}
	
	public PageTreeChunk parent() throws IOException {
		if(index == 0) return null;
		return tree.chunkAtIndex(tree.indexForParent(index));
	}
	
	public void dump() {
		for(int i = 0; i < tree.tagsPerChunk(); i++) {
			int childOffset = (int) (tree.tagsPerChunk()*tree.offsetOfChunkId(index) + i);
			long childId = tree.chunkIdAtPosition(tree.levelOfChunkId(index)+1, childOffset);
			System.out.println("\t" + i + " ("+ childId +"): " + Util.bytesToHex(tags.get(i)));
		}
	}
	
	protected void initBlank() {
		// This is a hot spot. Anything we can do to speed this up will be a big help.
		int tagsPerChunk = tree.tagsPerChunk(), hashLen = tree.archive.crypto.hashLength();
		byte[] blankTag = new byte[hashLen];
		for(int i = 0; i < tagsPerChunk; i++) {
			tags.add(blankTag);
		}
	}
	
	protected void write() throws IOException {
		byte[] serialized = serialize();
		chunkTag = SignedSecureFile
				  .withParams(tree.archive.storage, textKey(), saltKey(), authKey(), tree.archive.config.privKey)
				  .write(serialized, tree.archive.config.pageSize);
		
		if(index != 0) {
			PageTreeChunk parent = parent();
			long offset = (index-1) % tree.tagsPerChunk();
			parent.setTag(offset, chunkTag);
		} else {
			tree.setTag(chunkTag);
		}
		
		dirty = false;
		tree.markClean(this);
	}
	
	protected void read(boolean verify) throws IOException {
		tree.getArchive().getConfig().waitForPageReady(chunkTag);
		try {
			byte[] serialized = SignedSecureFile
					  .withTag(chunkTag, tree.archive.storage, textKey(), saltKey(), authKey(), tree.archive.config.pubKey)
					  .read(verify);
			deserialize(ByteBuffer.wrap(serialized));
		} catch(SecurityException exc) {
			System.out.println("Failed to read page tree chunk " + index + " of file with inode " + tree.getInodeId() + ", identity " + tree.inodeIdentity);
			throw exc;
		}
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(tree.tagsPerChunk() * tree.archive.crypto.hashLength());
		for(byte[] tag : tags) {
			buf.put(tag);
		}
		
		return buf.array();
	}
	
	protected void deserialize(ByteBuffer serialized) {
		tags.clear();
		int hashLength = tree.archive.crypto.hashLength();
		while(serialized.remaining() >= hashLength) {
			byte[] tag = new byte[tree.archive.crypto.hashLength()];
			serialized.get(tag);
			tags.add(tag);
		}
	}
	
	protected boolean isZero(byte[] tag) {
		for(int i = 0; i < tag.length; i++) {
			if(tag[i] != 0) return false;
		}
		
		return true;
	}
	
	protected Key textKey() {
		byte[] archiveId = tree.getArchive().getConfig().getArchiveId();
		ByteBuffer buf = ByteBuffer.allocate(8 + 8 + archiveId.length);
		buf.putLong(tree.inodeIdentity);
		buf.putLong(index);
		buf.put(archiveId);
		
		return tree.archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE,
				"easysafe-page-text-key",
				buf.array());
	}
	
	protected Key saltKey() {
		byte[] archiveId = tree.getArchive().getConfig().getArchiveId();
		ByteBuffer buf = ByteBuffer.allocate(8 + 8 + archiveId.length);
		buf.putLong(tree.inodeIdentity);
		buf.putLong(index);
		buf.put(archiveId);
		
		return tree.archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE,
				"easysafe-page-salt-key",
				buf.array());
	}
	
	protected Key authKey() {
		return tree.archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED,
				"easysafe-page-auth-key");
	}
}
