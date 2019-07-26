package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.LinkedList;

public class BlockManager {
	protected LinkedList<Block> pendingBlocks;
	protected int maxOpenBlocks;
	protected ZKFS fs;
	
	public BlockManager(int maxOpenBlocks) {
		pendingBlocks = new LinkedList<>();
	}
	
	public Block addData(long identity, long pageNum, byte type, byte[] contents, int offset, int length) throws IOException {
		if(length == fs.getArchive().getConfig().getPageSize()) {
			return addDataSingle(identity, pageNum, type, contents, offset, length);
		}

		return addDataMultitenanted(identity, pageNum, type, contents, offset, length);
	}

	protected Block addDataSingle(long identity, long pageNum, byte type, byte[] contents, int offset, int length) throws IOException {
		Block block = new Block(fs);
		block.addData(identity, pageNum, type, contents, offset, length);
		block.write();
		return block;
	}
	
	protected synchronized Block addDataMultitenanted(long identity, long pageNum, byte type, byte[] contents, int offset, int length) throws IOException {
		Block block = blockForData(length);
		for(Block existing : pendingBlocks) {
			existing.removeData(identity, pageNum, type);
		}
		
		block.addData(identity, pageNum, type, contents, offset, length);
		return block;
	}
	
	public synchronized void writeAll() throws IOException {
		for(Block block : pendingBlocks) {
			block.write();
		}
		
		pendingBlocks.clear();
	}
	
	protected Block blockForData(int length) throws IOException {
		Block existing = pendingBlockToFit(length);
		if(existing != null) {
			return existing;
		}
		
		Block newBlock = new Block(fs);
		pendingBlocks.add(newBlock);
		enforceOpenBlockLimit();
		return newBlock;
	}
	
	protected Block pendingBlockToFit(int length) {
		Block bestFit = null;
		for(Block block : pendingBlocks) {
			if(!block.canFitData(length)) {
				continue;
			}
			
			if(bestFit == null || bestFit.remainingCapacity > block.remainingCapacity) {
				bestFit = block;
			}
		}
		
		return bestFit;
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
