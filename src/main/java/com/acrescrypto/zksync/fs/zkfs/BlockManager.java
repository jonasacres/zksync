package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.LinkedList;

public class BlockManager {
	protected LinkedList<Block> pendingBlocks;
	protected int maxOpenBlocks;
	protected ZKArchive archive;
	
	public BlockManager(ZKArchive archive, int maxOpenBlocks) {
		this.archive = archive;
		this.maxOpenBlocks = maxOpenBlocks;
		pendingBlocks = new LinkedList<>();
	}
	
	public Block addData(long identity, long pageNum, byte type, byte[] contents, int offset, int length) throws IOException {
		boolean canStoreAsImmediate = pageNum == 0 && length < archive.getCrypto().hashLength();
		boolean requiresWholePage = length >= archive.getConfig().getPageSize();
		
		if(canStoreAsImmediate || requiresWholePage) {
			return addDataSingle(identity, pageNum, type, contents, offset, length);
		}

		return addDataMultitenanted(identity, pageNum, type, contents, offset, length);
	}

	protected Block addDataSingle(long identity, long pageNum, byte type, byte[] contents, int offset, int length) throws IOException {
		Block block = new Block(archive);
		block.addData(identity, pageNum, type, contents, offset, length);
		block.write();
		return block;
	}
	
	protected synchronized Block addDataMultitenanted(long identity, long pageNum, byte type, byte[] contents, int offset, int length) throws IOException {
		Block block = blockForData(identity, pageNum, type, length);
		for(Block existing : pendingBlocks) {
			existing.removeData(identity, pageNum, type);
		}
		
		block.addData(identity, pageNum, type, contents, offset, length);
		return block;
	}
	
	public synchronized void writeAll() throws IOException {
		for(Block block : pendingBlocks) {
			if(block.isWritable()) {
				block.write();
			}
		}
		
		pendingBlocks.clear();
	}
	
	protected Block blockForData(long identity, long pageNum, byte type, int length) throws IOException {
		Block existing = pendingBlockToFit(identity, pageNum, type, length);
		if(existing != null) {
			return existing;
		}
		
		Block newBlock = new Block(archive);
		pendingBlocks.add(newBlock);
		enforceOpenBlockLimit();
		return newBlock;
	}
	
	protected Block pendingBlockToFit(long identity, long pageNum, byte type, int length) {
		Block bestFit = null;
		pruneClosedBlocks();
		for(Block block : pendingBlocks) {
			if(block.hasData(identity, pageNum, type)) {
				block.removeData(identity, pageNum, type);
			}
			
			if(!block.canFitData(length)) {
				continue;
			}
			
			if(bestFit == null || bestFit.remainingCapacity > block.remainingCapacity) {
				bestFit = block;
			}
		}
		
		return bestFit;
	}
	
	protected void pruneClosedBlocks() {
		pendingBlocks.removeIf((block)->!block.isWritable);
	}
	
	protected void enforceOpenBlockLimit() throws IOException {
		assert(maxOpenBlocks >= 0);
		
		while(pendingBlocks.size() > maxOpenBlocks) {
			Block fullestBlock = null;
			for(Block block : pendingBlocks) {
				if(fullestBlock == null || block.remainingCapacity < fullestBlock.remainingCapacity) {
					fullestBlock = block;
				}
			}
			
			fullestBlock.write();
			pendingBlocks.remove(fullestBlock);
		}
	}
	
	public int getMaxOpenBlocks() {
		return maxOpenBlocks;
	}
	
	public synchronized void setMaxOpenBlocks(int maxOpenBlocks) throws IOException {
		this.maxOpenBlocks = maxOpenBlocks;
		enforceOpenBlockLimit();
	}
}
