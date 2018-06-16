package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.utility.HashCache;
import com.acrescrypto.zksync.utility.Util;

public class PageTree {
	protected ZKArchive archive;
	protected RefTag refTag;
	protected HashCache<Long,PageTreeChunk> chunkCache;
	protected Queue<PageTreeChunk> dirtyChunks = new LinkedList<>();
	protected long inodeId, inodeIdentity;
	protected long numChunks, maxNumPages, numPages;
	
	public PageTree(RefTag tag) throws IOException {
		this(tag, InodeTable.INODE_ID_INODE_TABLE, 0);
	}
	
	public PageTree(RefTag tag, long inodeId, long inodeIdentity) throws IOException {
		this.archive = tag.getArchive();
		assert(0 < tagsPerChunk() && tagsPerChunk() <= Integer.MAX_VALUE);
		this.refTag = tag;
		this.inodeId = inodeId;
		this.inodeIdentity = inodeIdentity;

		int numLeafChunks = (int) Math.ceil(tag.numPages / tagsPerChunk());
		numChunks = numLeafChunks + chunkIndexForPageNum(0);
		this.maxNumPages = numLeafChunks * tagsPerChunk();
		this.numPages = tag.numPages;
		
		this.chunkCache = new HashCache<>(8,
				(index)->chunkAtIndex(index),
				(index, chunk)->{ if(chunk.dirty) chunk.write(); }
				);
	}
	
	public boolean exists() throws IOException {
		switch(refTag.getRefType()) {
		case RefTag.REF_TYPE_IMMEDIATE: return true;
		case RefTag.REF_TYPE_INDIRECT:
			return archive.storage.exists(Page.pathForTag(refTag.getHash()));
		case RefTag.REF_TYPE_2INDIRECT:
			return hasTreeContentsLocally();
		default:
			return false;
		}
	}
	
	public void assertExists() throws IOException {
		if(!exists()) {
			throw new ENOENTException("PageTree for " + Util.bytesToHex(refTag.getBytes()));
		}
	}
	
	public RefTag getRefTag() {
		return refTag;
	}
	
	public void setPageTag(int pageNum, byte[] pageTag) throws IOException {
		chunkForPageNum(pageNum).setTag(pageNum % tagsPerChunk(), pageTag);
	}
	
	public byte[] getPageTag(int pageNum) throws IOException {
		return chunkForPageNum(pageNum).getTag(pageNum % tagsPerChunk());
	}
	
	public long numChunks() {
		return numChunks;
	}
	
	public RefTag commit() throws IOException {
		while(dirtyChunks.peek() != null) {
			dirtyChunks.poll().write();
		}

		return getRefTag();
	}
	
	public void resize(long newMinPages) throws IOException {
		long currentLevel = levelOfChunkId(chunkIndexForPageNum(0));
		long newLevel = (int) Math.floor(Math.log(newMinPages)/Math.log(tagsPerChunk()));
		long diff = newLevel - currentLevel;
		
		if(diff == 0) {
			resizeToSameLevel(newMinPages);
		} else if(diff < 0) {
			resizeToSmallerLevel(newMinPages, diff);
		} else if(diff > 0) {
			resizeToLargerLevel(newMinPages, diff);
		}
	}
	
	protected void resizeToSameLevel(long newMinPages) throws IOException {
		if(numPages > newMinPages) {
			for(long i = newMinPages; i < numPages; i++) {
				byte[] blank = new byte[archive.crypto.hashLength()];
				chunkForPageNum(i).setTag(i % tagsPerChunk(), blank);
			}
		}
		
		numPages = newMinPages;
	}
	
	protected void resizeToSmallerLevel(long newMinPages, long diff) throws IOException {
		long newRootId = chunkIdAtPosition(-diff, 0);
		setTag(chunkAtIndex(newRootId).chunkTag);
		
		/* TODO: renumber cached chunks in memory (requires a chance to HashCache) */
		chunkCache.removeAll();
	}
	
	protected void resizeToLargerLevel(long newMinPages, long diff) throws IOException {
		PageTreeChunk root = chunkAtIndex(0);
		for(int i = 0; i < diff; i++) {
			PageTreeChunk newRoot = new PageTreeChunk(this, new byte[archive.crypto.hashLength()], 0);
			newRoot.setTag(0, root.chunkTag);
			root = newRoot;
		}

		/* TODO: renumber cached chunks in memory (requires a chance to HashCache) */
		chunkCache.removeAll();
	}
	
	public boolean hasTag(long pageNum) throws IOException {
		return chunkForPageNum(pageNum).hasTag(pageNum % tagsPerChunk());
	}
	
	public byte[] tagForChunk(long index) throws IOException {
		return chunkCache.get(index).chunkTag;
	}
	
	public ZKArchive getArchive() {
		return archive;
	}
	
	protected PageTreeChunk chunkForPageNum(long pageNum) throws IOException {
		return chunkCache.get(chunkIndexForPageNum(pageNum));
	}
	
	protected long chunkIndexForPageNum(long pageNum) {
		long level = levelOfChunkId(numChunks-1);
		long base = (int) ((1 - Math.pow(tagsPerChunk(), level))/(1 - tagsPerChunk()));
		return base + pageNum;
	}
	
	protected PageTreeChunk chunkAtIndex(long index) throws IOException {
		if(index == 0) {
			return new PageTreeChunk(this, refTag.getHash(), 0);
		}
		
		if(index >= numChunks) {
			resize(numChunks);
		}
		
		long parentIndex = indexForParent(index);
		long offsetInParent = (index - 1) % tagsPerChunk();
		
		byte[] chunkTag = chunkCache.get(parentIndex).getTag(offsetInParent);
		return new PageTreeChunk(this, chunkTag, index);
	}
	
	protected long indexForParent(long index) {
		long level = levelOfChunkId(index);
		long offset = index == 0 ? 0 : (index - 1) % tagsPerChunk();
		
		long parentLevel = level - 1;
		long parentOffset = offset/tagsPerChunk();
		long parentIndex = chunkIdAtPosition(parentLevel, parentOffset);
		
		return parentIndex;
	}
	
	protected void setTag(byte[] tag) {
		int refType;
		if(maxNumPages == 1) {
			if(tag.length < archive.crypto.hashLength()) {
				refType = RefTag.REF_TYPE_IMMEDIATE;
			} else {
				refType = RefTag.REF_TYPE_INDIRECT;
			}
		} else {
			refType = RefTag.REF_TYPE_2INDIRECT;
		}
		
		refTag = new RefTag(archive, tag, refType, numPages);
	}
	
	protected boolean hasTreeContentsLocally() throws IOException {
		for(int i = 0; i < numChunks; i++) {
			if(!hasChunkLocally(i)) return false;
		}
		
		return true;
	}
	
	protected boolean hasChunkLocally(int index) throws IOException {
		if(!archive.config.getCacheStorage().exists(Page.pathForTag(tagForChunk(index)))) return false;
		PageTreeChunk chunk = chunkAtIndex(index);
		for(int i = 0; i < tagsPerChunk(); i++) {
			byte[] tag = chunk.getTag(i);
			if(!archive.config.getCacheStorage().exists(Page.pathForTag(tag))) return false;
		}
		
		return true;
	}
	
	protected void markDirty(PageTreeChunk chunk) {
		dirtyChunks.add(chunk);
	}
	
	protected long levelOfChunkId(long chunkId) {
	    if(chunkId == 0) return 0;
	    long offset = (chunkId - 1) % tagsPerChunk();
	    return (long) Math.floor(Math.log(1-(chunkId-offset)*(1-tagsPerChunk()))/Math.log(tagsPerChunk()));
	}
	
	protected long offsetOfChunkId(long chunkId) {
		return chunkId == 0 ? 0 : (chunkId - 1) % tagsPerChunk();
	}
	
	protected int tagsPerChunk() {
		return archive.config.getPageSize()/archive.getCrypto().hashLength();
	}
	
	protected long chunkIdAtPosition(long level, long offset) {
		return (long) (1-Math.pow(tagsPerChunk(), level))/(1-tagsPerChunk()) + offset;
	}
}