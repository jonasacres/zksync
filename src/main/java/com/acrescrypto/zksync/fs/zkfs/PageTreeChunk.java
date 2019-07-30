package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.SignedSecureFile;

public class PageTreeChunk {
	protected PageTree tree;
	StorageTag chunkTag;
	StorageTag[] tags;
	long index;
	protected boolean dirty;
	
	public PageTreeChunk(PageTree tree, StorageTag chunkTag, long index, boolean verify) throws IOException {
		this.index = index;
		this.chunkTag = chunkTag;
		this.tree = tree;
		this.tags = new StorageTag[tree.tagsPerChunk()];
		
		if(chunkTag.isBlank()) {
			initBlank();
		} else {
			read(verify);
		}
	}
	
	public boolean hasTag(long offset) {
		return !getTag(offset).isBlank();
	}
	
	public void setTag(long offset, StorageTag tag) {
		if(tag.equals(tags[(int) offset])) return;
		
		loadTag(offset, tag);
		if(tag.isStored()) {
			tree.archive.addPageTag(tag);
		}
		
		markDirty();
	}
	
	public void loadTag(long offset, StorageTag tag) {
		tags[(int) offset] = tag;
	}
	
	public StorageTag getTag(long offset) {
		return tags[(int) offset];
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
			System.out.println("\t" + i + " ("+ childId +"): " + tags[i]);
		}
	}
	
	protected void initBlank() {
		int tagsPerChunk = tree.tagsPerChunk();
		for(int i = 0; i < tagsPerChunk; i++) {
			tags[i] = tree.archive.getBlankStorageTag();
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
		tree.getArchive().getConfig().waitForPageReady(chunkTag,
				tree.getReadTimeoutMs());
		byte[] serialized = SignedSecureFile
				  .withTag(chunkTag, tree.archive.storage, textKey(), saltKey(), authKey(), tree.archive.config.pubKey)
				  .read(verify);
		deserialize(ByteBuffer.wrap(serialized));
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(tree.tagsPerChunk() * tree.archive.crypto.hashLength());
		for(StorageTag tag : tags) {
			buf.put(tag.getTagBytes());
		}
		
		return buf.array();
	}
	
	protected void deserialize(ByteBuffer serialized) {
		tags = new StorageTag[tree.tagsPerChunk()];
		int hashLength = tree.archive.crypto.hashLength();
		int i = 0;
		while(serialized.remaining() >= hashLength) {
			byte[] tagBytes = new byte[tree.archive.crypto.hashLength()];
			serialized.get(tagBytes);
			StorageTag tag = new StorageTag(tree.getArchive().getCrypto(), tagBytes);
			tags[i++] = tag;
		}
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
	
	public String toString() {
		return String.format("PageTreeChunk %d (%d) %s%s",
				index,
				tree.inodeId,
				chunkTag == null ? "null" : chunkTag,
				dirty ? "*" : "");
	}
}
