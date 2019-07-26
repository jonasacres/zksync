package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.Key;

public class PageTreeChunk {
	protected PageTree tree;
	DeferrableTag chunkTag;
	DeferrableTag[] tags;
	long index;
	protected boolean dirty;
	
	public PageTreeChunk(PageTree tree, DeferrableTag chunkTag, long index, boolean verify) throws IOException {
		this.index = index;
		this.chunkTag = chunkTag;
		this.tree = tree;
		this.tags = new DeferrableTag[tree.tagsPerChunk()];
		
		if(chunkTag.isBlank()) {
			initBlank();
		} else {
			read(verify);
		}
	}
	
	public boolean hasTag(long offset) {
		return !getTag(offset).isBlank();
	}
	
	public void setTag(long offset, DeferrableTag tag) throws IOException {
		if(tag.equals(tags[(int) offset])) return;
		
		loadTag(offset, tag);
		if(!tag.isImmediate() && !tag.isPending()) {
			tree.archive.addPageTag(tag.getBytes());
		}
		
		markDirty();
	}
	
	public void loadTag(long offset, DeferrableTag tag) {
		tags[(int) offset] = tag;
	}
	
	public DeferrableTag getTag(long offset) {
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
		// This is a hot spot. Anything we can do to speed this up will be a big help.
		int tagsPerChunk = tree.tagsPerChunk();
		DeferrableTag blank = DeferrableTag.blank(tree.archive);
		for(int i = 0; i < tagsPerChunk; i++) {
			tags[i] = blank;
		}
	}
	
	protected void write() throws IOException {
		// TODO: redo this; should live inside a Block
		byte[] serialized = serialize();
		Block block = tree.fs.getBlockManager().addData(tree.inodeIdentity,
				index,
				Block.INDEX_TYPE_CHUNK,
				serialized,
				0,
				serialized.length);
		
		if(index != 0) {
			PageTreeChunk parent = parent();
			long offset = (index-1) % tree.tagsPerChunk();
			parent.setTag(offset, block.getDeferrableTag());
		} else {
			tree.setTag(block.getDeferrableTag());
		}
		
		dirty = false;
		tree.markClean(this);
	}
	
	protected void read(boolean verify) throws IOException {
		// TODO: redo this; should live inside a Block
		byte[] serialized = chunkTag.getBlock().readData(tree.inodeIdentity,
				index,
				Block.INDEX_TYPE_CHUNK);
		tree.getArchive().getConfig().waitForPageReady(chunkTag.getBytes(),
				tree.getReadTimeoutMs());
		deserialize(ByteBuffer.wrap(serialized));
	}
	
	protected byte[] serialize() throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(tree.tagsPerChunk() * tree.archive.crypto.hashLength());
		for(DeferrableTag tag : tags) {
			buf.put(tag.getBytes());
		}
		
		return buf.array();
	}
	
	protected void deserialize(ByteBuffer serialized) {
		tags = new DeferrableTag[tree.tagsPerChunk()];
		int hashLength = tree.archive.crypto.hashLength();
		int i = 0;
		while(serialized.remaining() >= hashLength) {
			byte[] tagBytes = new byte[tree.archive.crypto.hashLength()];
			serialized.get(tagBytes);
			tags[i++] = DeferrableTag.withBytes(tree.getArchive(), tagBytes);
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
	
	public String toString() {
		return String.format("PageTreeChunk %d (%d) %s%s",
				index,
				tree.inodeId,
				chunkTag == null ? "null" : chunkTag,
				dirty ? "*" : "");
	}
}
