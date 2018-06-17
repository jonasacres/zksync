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
	
	public PageTreeChunk(PageTree tree, byte[] chunkTag, long index) throws IOException {
		System.out.println("Chunk " + index + ": " + Util.bytesToHex(chunkTag));
		this.index = index;
		this.chunkTag = chunkTag;
		this.tree = tree;
		this.tags = new ArrayList<>(tree.tagsPerChunk());
		
		if(isZero(chunkTag)) {
			initBlank();
		} else {
			read();
		}
	}
	
	public boolean hasTag(long offset) {
		return !isZero(getTag(offset));
	}
	
	public void setTag(long offset, byte[] tag) {
		if(Arrays.equals(tag, tags.get((int) offset))) return;
		
		tags.set((int) offset, tag);
		markDirty();
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
	
	protected void initBlank() {
		for(int i = 0; i < tree.tagsPerChunk(); i++) {
			tags.add(new byte[tree.archive.crypto.hashLength()]);
		}
	}
	
	protected void write() throws IOException {
		byte[] serialized = serialize();
		chunkTag = SignedSecureFile
				  .withParams(tree.archive.storage, textKey(), authKey(), tree.archive.config.privKey)
				  .write(serialized, tree.archive.config.pageSize);
		if(index != 0) {
			parent().setTag((index-1) % tree.tagsPerChunk(), chunkTag);
		} else {
			tree.setTag(chunkTag);
		}
		
		dirty = false;
	}
	
	protected void read() throws IOException {
		byte[] serialized = SignedSecureFile
				  .withTag(chunkTag, tree.archive.storage, textKey(), authKey(), tree.archive.config.pubKey)
				  .read();
		deserialize(ByteBuffer.wrap(serialized));
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
		while(serialized.remaining() >= tree.archive.crypto.hashLength()) {
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
		ByteBuffer buf = ByteBuffer.allocate(24);
		buf.putLong(tree.inodeId);
		buf.putLong(tree.inodeIdentity);
		buf.putLong(index);

		return tree.archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE,
				ArchiveAccessor.KEY_TYPE_CIPHER,
				ArchiveAccessor.KEY_INDEX_PAGE,
				buf.array());
	}
	
	protected Key authKey() {
		return tree.archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED,
				ArchiveAccessor.KEY_TYPE_AUTH,
				ArchiveAccessor.KEY_INDEX_PAGE);
	}
}
