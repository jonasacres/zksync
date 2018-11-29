package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;
import com.acrescrypto.zksync.utility.HashCache;
import com.acrescrypto.zksync.utility.Util;

public class PageTree {
	protected ZKArchive archive;
	protected RefTag refTag;
	protected HashCache<Long,PageTreeChunk> chunkCache;
	protected Queue<PageTreeChunk> dirtyChunks;
	protected long inodeId, inodeIdentity;
	protected long numChunks, maxNumPages, numPages;
	protected boolean trusted; // if true, do not validate public key signature on each page chunk
	
	public class PageTreeStats {
		public long numCachedPages, numCachedChunks, totalPages, totalChunks;
	}
	
	/* Open a revtag, which is a reftag to an inode table */ 
	public PageTree(RefTag revTag) throws IOException {
		this.archive = revTag.getArchive();
		this.refTag = revTag;
		this.inodeId = InodeTable.INODE_ID_INODE_TABLE;
		this.inodeIdentity = 0;
		initWithSize(refTag.getNumPages());
	}
	
	public PageTree(Inode inode) {
		this.archive = inode.fs.archive;
		assert(0 < tagsPerChunk() && tagsPerChunk() <= Integer.MAX_VALUE);
		this.refTag = inode.refTag;
		this.inodeId = inode.stat.getInodeId();
		this.inodeIdentity = inode.identity;
		this.trusted = true; // if we validated the inode table, we know the page chunks are legit too
		initWithSize(refTag.getNumPages());
	}
	
	/** Clone another tree, hijacking its chunkCache in the process. This is used for resizing.
	 * It is expected that the original tree will be wiped with initWithSize immediately afterwards before calling
	 * any further operations.
	 * */
	protected PageTree(PageTree original) throws IOException {
		this.trusted = original.trusted;
		this.archive = original.archive;
		this.refTag = original.refTag;
		this.inodeId = original.inodeId;
		this.inodeIdentity = original.inodeIdentity;
		
		this.numChunks = original.numChunks;
		this.numPages = original.numPages;
		this.maxNumPages = original.maxNumPages;
		
		this.dirtyChunks = original.dirtyChunks;
		
		for(long chunkId : original.chunkCache.cachedKeys()) {
			original.chunkCache.get(chunkId).tree = this;
		}
		
		this.chunkCache = new HashCache<>(original.chunkCache,
				(index)->loadChunkAtIndex(index),
				(index, chunk)->{
					if(chunk.dirty) chunk.write(); }
				);
	}
	
	protected PageTree() {} // used for testing
	
	protected void initWithSize(long size) {
		int numLeafChunks = (int) Math.ceil((double) size / tagsPerChunk());
		int level = (int) Math.ceil(Math.log(numLeafChunks)/Math.log(tagsPerChunk()));
		int supportNodes = (int) (1-Math.pow(tagsPerChunk(), level))/(1-tagsPerChunk());
		
		numChunks = supportNodes + numLeafChunks;
		this.numPages = size;
		
		if(numPages > 1 || refTag.getRefType() == RefTag.REF_TYPE_2INDIRECT) {
			this.maxNumPages = numLeafChunks * tagsPerChunk();
		} else if(refTag.getRefType() == RefTag.REF_TYPE_INDIRECT) {
			this.maxNumPages = tagsPerChunk();
		} else {
			this.maxNumPages = 1;
		}
		
		this.dirtyChunks = new LinkedList<>();
		this.chunkCache = new HashCache<>(16,
				(index)->loadChunkAtIndex(index),
				(index, chunk)->{
					if(chunk.dirty) chunk.write();
				}
				);
	}
	
	public boolean exists() throws IOException {
		if(numPages < 0) return false;
		
		switch(refTag.getRefType()) {
		case RefTag.REF_TYPE_IMMEDIATE: return numPages >= 0 && numPages <= 1;
		case RefTag.REF_TYPE_INDIRECT:
			return archive.config.getCacheStorage().exists(Page.pathForTag(refTag.getHash()));
		case RefTag.REF_TYPE_2INDIRECT:
			return hasTreeContentsLocally();
		default:
			return false;
		}
	}
	
	public boolean pageExists(long pageNum) throws IOException {
		if(numPages < 0) return false;
		if(pageNum < 0) return false;
		
		switch(refTag.getRefType()) {
		case RefTag.REF_TYPE_IMMEDIATE: return pageNum == 0;
		case RefTag.REF_TYPE_INDIRECT:
			if(pageNum != 0) return false;
			return archive.config.getCacheStorage().exists(Page.pathForTag(refTag.getHash()));
		case RefTag.REF_TYPE_2INDIRECT:
			if(numChunks <= 0 || numPages <= pageNum) return false;
			if(!archive.config.getCacheStorage().exists(Page.pathForTag(tagForChunk(chunkIndexForPageNum(pageNum))))) return false;
			byte[] tag = getPageTag(pageNum);
			return archive.config.getCacheStorage().exists(Page.pathForTag(tag));
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
	
	public void setPageTag(long pageNum, byte[] pageTag) throws IOException {
		if(pageNum >= maxNumPages) {
			resize(1+pageNum);
		}
		
		chunkForPageNum(pageNum).setTag(pageNum % tagsPerChunk(), pageTag);
		numPages = Math.max(numPages, 1+pageNum);
	}
	
	public byte[] getPageTag(long pageNum) throws IOException {
		return chunkForPageNum(pageNum).getTag(pageNum % tagsPerChunk());
	}
	
	public long numPages() {
		return numPages;
	}
	
	protected long numPieces() {
		return numPages() + numChunks();
	}
	
	public long numChunks() {
		return numChunks;
	}
	
	public PageTreeStats getStats() throws IOException {
		PageTreeStats stats = new PageTreeStats();
		if(refTag.refType == RefTag.REF_TYPE_IMMEDIATE) {
			stats.totalPages = stats.numCachedPages = 1; // make life easy for percentage-calculations (no division by zero)
			stats.totalChunks = stats.numCachedChunks = 1;
			return stats;
		}
		
		stats.totalChunks = numChunks;
		stats.totalPages = numPages;
		HashSet<Long> foundChunks = new HashSet<>();
		
		for(long i = 0; i < numChunks; i++) {
			if(i != 0 && !foundChunks.contains(indexForParent(i))) continue;
			try {
				tagForChunk(i);
				foundChunks.add(i);
			} catch(InaccessibleStorageException exc) {}
		}
		
		stats.numCachedChunks = foundChunks.size();
		
		for(long i = 0; i < numPages; i++) {
			if(!foundChunks.contains(chunkIndexForPageNum(i))) continue;
			String path = Page.pathForTag(getPageTag(i));
			if(archive.getConfig().getCacheStorage().exists(path)) {
				stats.numCachedPages++;
			}
		}
		
		return stats;
	}
	
	public long numDataPages() {
		// TODO API: (deprecate) replace with numPieces
		return numPages + (refTag.getRefType() == RefTag.REF_TYPE_2INDIRECT ? numChunks : 0);
	}
	
	public RefTag commit() throws IOException {
		if(numPages > 1) {
			while(dirtyChunks.peek() != null) {
				dirtyChunks.poll().write();
			}
		} else if(numPages <= 1) {
			setTag(chunkAtIndex(0).getTag(0));
		}

		return getRefTag();
	}
	
	public void resize(long newMinPages) throws IOException {
		if(newMinPages == 0) {
			this.refTag = RefTag.blank(archive);
			initWithSize(0);
			return;
		}
		
		long currentLevel = levelOfChunkId(chunkIndexForPageNum(0));
		long newLevel = (int) Math.max(0, Math.floor(Math.log(newMinPages-1)/Math.log(tagsPerChunk())));
		
		if(newLevel == currentLevel) {
			resizeToSameLevel(newMinPages);
		} else {
			resizeToDifferentLevel(newMinPages);
		}
	}
	
	protected void resizeToSameLevel(long newMinPages) throws IOException {
		if(newMinPages <= 1 && maxNumPages == 1) return;
		maxNumPages = (long) (Math.pow(tagsPerChunk(), levelOfChunkId(chunkIndexForPageNum(0))) * tagsPerChunk());
		numChunks = 1 + chunkIndexForPageNum(newMinPages-1);

		if(numPages > newMinPages) {
			byte[] blank = new byte[archive.crypto.hashLength()];
			for(long i = newMinPages; i < numPages; i++) {
				chunkForPageNum(i).setTag(i % tagsPerChunk(), blank);
			}
		}
	}
	
	protected void resizeToDifferentLevel(long newMinPages) throws IOException {
		PageTree existing = new PageTree(this);
		initWithSize(newMinPages);
		this.refTag = RefTag.blank(archive);
		
		maxNumPages = (long) (Math.pow(tagsPerChunk(), levelOfChunkId(chunkIndexForPageNum(0))) * tagsPerChunk());
		for(int i = 0; i < Math.min(existing.numPages, newMinPages); i++) {
			setPageTag(i, existing.getPageTag(i));
		}
	}

	public boolean hasTag(long pageNum) throws IOException {
		if(pageNum < 0 || pageNum >= numPages) return false;
		return chunkForPageNum(pageNum).hasTag(pageNum % tagsPerChunk());
	}
	
	public byte[] tagForChunk(long index) throws IOException {
		if(index == 0) {
			if(refTag.isBlank() || refTag.refType != RefTag.REF_TYPE_2INDIRECT) {
				return new byte[archive.getCrypto().hashLength()];
			}
			
			return refTag.getHash();
		}
		
		long parentIndex = indexForParent(index);
		long offsetInParent = (index - 1) % tagsPerChunk();

		return chunkAtIndex(parentIndex).getTag(offsetInParent);
	}
	
	public ZKArchive getArchive() {
		return archive;
	}
	
	protected PageTreeChunk chunkForPageNum(long pageNum) throws IOException {
		return chunkAtIndex(chunkIndexForPageNum(pageNum));
	}
	
	protected long chunkIndexForPageNum(long pageNum) {
		long level = levelOfChunkId(numChunks-1);
		long base = (int) ((1 - Math.pow(tagsPerChunk(), level))/(1 - tagsPerChunk()));
		return base + pageNum/tagsPerChunk();
	}
	
	protected PageTreeChunk chunkAtIndex(long index) throws IOException {
		return chunkCache.get(index);
	}
	
	protected PageTreeChunk loadChunkAtIndex(long index) throws IOException {
		if(index == 0) {
			PageTreeChunk chunk = new PageTreeChunk(this, tagForChunk(0), 0, !trusted);
			
			if(refTag.getRefType() != RefTag.REF_TYPE_2INDIRECT && !refTag.isBlank()) {
				chunk.loadTag(0, refTag.getLiteral());
			}
			
			return chunk;
		}
		
		if(index >= numChunks) {
			long level = levelOfChunkId(index);
			long supportedByPreviousLevel = (int) (tagsPerChunk() * Math.pow(tagsPerChunk(), level-1));
			long supportedByChunkOffset = tagsPerChunk() * (1 + offsetOfChunkId(index));
			long newMinPages = Math.max(supportedByChunkOffset, supportedByPreviousLevel);
			resize(newMinPages);
		}
		
		long parentIndex = indexForParent(index);
		long offsetInParent = (index - 1) % tagsPerChunk();
		
		byte[] chunkTag = chunkAtIndex(parentIndex).getTag(offsetInParent);
		if(chunkCache.hasCached(index)) {
			// it's possible that finding the parent triggered a separate search operation for this index, due to evictions
			return chunkCache.get(index);
		}
		
		return new PageTreeChunk(this, chunkTag, index, !trusted);
	}
	
	protected long indexForParent(long index) {
		long level = levelOfChunkId(index);
		long baseIndex = (1 - (int) Math.pow(tagsPerChunk(), level))/(1-tagsPerChunk());
		
		long parentOffset = index == 0 ? 0 : (index - baseIndex) / tagsPerChunk();
		long parentLevel = level - 1;
		long parentIndex = chunkIdAtPosition(parentLevel, parentOffset);
		
		return parentIndex;
	}
	
	protected void setTag(byte[] tag) {
		int refType;
		if(numPages <= 1) {
			if(tag.length < archive.crypto.hashLength()) {
				refType = RefTag.REF_TYPE_IMMEDIATE;
			} else {
				refType = RefTag.REF_TYPE_INDIRECT;
			}
		} else {
			refType = RefTag.REF_TYPE_2INDIRECT;
			archive.addPageTag(tag);
		}
		
		refTag = new RefTag(archive, tag, refType, numPages);
	}
	
	protected boolean hasTreeContentsLocally() throws IOException {
		if(numChunks <= 0 || numPages <= 0) return false;
		for(int i = 0; i < numChunks; i++) {
			if(!hasChunkContentsLocally(i)) return false;
		}
		
		return true;
	}
	
	protected boolean hasChunkContentsLocally(long index) throws IOException {
		if(!archive.config.getCacheStorage().exists(Page.pathForTag(tagForChunk(index)))) return false;
		PageTreeChunk chunk = loadChunkAtIndex(index);
		for(int i = 0; i < tagsPerChunk(); i++) {
			byte[] tag = chunk.getTag(i);
			if(chunk.isZero(tag)) continue;
			if(!archive.config.getCacheStorage().exists(Page.pathForTag(tag))) return false;
		}
		
		return true;
	}
	
	protected void markDirty(PageTreeChunk chunk) {
		dirtyChunks.add(chunk);
	}
	
	protected void markClean(PageTreeChunk chunk) {
		dirtyChunks.remove(chunk);
	}
	
	protected long levelOfChunkId(long chunkId) {
	    if(chunkId <= 0) return 0;
	    long offset = (chunkId - 1) % tagsPerChunk();
	    return (long) Math.floor(Math.log(1-(chunkId-offset)*(1-tagsPerChunk()))/Math.log(tagsPerChunk()));
	}
	
	protected long offsetOfChunkId(long chunkId) {
		long level = levelOfChunkId(chunkId);
		long base = (long) (1-Math.pow(tagsPerChunk(), level))/(1-tagsPerChunk());
		return chunkId - base;
	}
	
	protected int tagsPerChunk() {
		/* Much of this class is written to avoid the assumption of int-sized values, but some
		 * stuff like ArrayList wants ints. So we're actually limited to int-sizes stuff anyway, which is
		 * probably fine for now.
		 * */
		return archive.config.getTagsPerChunk();
	}
	
	protected long chunkIdAtPosition(long level, long offset) {
		return (long) (1-Math.pow(tagsPerChunk(), level))/(1-tagsPerChunk()) + offset;
	}
	
	protected boolean hasTagLocally(byte[] tag) {
		String path = Page.pathForTag(tag);
		return archive.getConfig().getCacheStorage().exists(path);
	}

	public long getInodeId() {
		return inodeId;
	}
	
	public void setTrusted(boolean trusted) {
		this.trusted = trusted;
	}
	
	public boolean getTrusted() {
		return trusted;
	}
	
	public void dump() throws IOException {
		System.out.println(this + ": inodeId=" + inodeId + ", inodeIdentity=" + inodeIdentity + ", refTag=" + Util.bytesToHex(refTag.getBytes()));
		System.out.println("numChunks=" + numChunks + ", maxNumPages=" + maxNumPages + ", numPages=" + numPages);
		for(int i = 0; i < numChunks; i++) {
			chunkAtIndex(i).dump();
		}
	}
	
	public void dumpCache() throws IOException {
		for(long chunkId : chunkCache.cachedKeys()) {
			chunkCache.get(chunkId).dump();
		}
	}
}
