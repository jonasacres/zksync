package com.acrescrypto.zksync.fs;

import java.io.IOException;

public class ChunkableFileHandle {
	protected File file;
	protected int chunkSize, expectedSize, numChunks, chunksObtained;
	protected String path, tempPath;
	protected byte[] chunkMask;
	
	public ChunkableFileHandle(FS fs, String path, int expectedSize, int chunkSize) throws IOException {
		this.path = path;
		this.expectedSize = expectedSize;
		this.chunkSize = chunkSize;
		this.numChunks = (int) Math.ceil(((double) expectedSize)/chunkSize);
		this.chunkMask = new byte[(int) Math.ceil(numChunks/8.0)];
		this.tempPath = path + ".partial";
		
		file = fs.open(path, File.O_CREAT|File.O_RDWR);
		file.truncate(expectedSize);
		scanChunkMask(fs);
	}
	
	public synchronized void writeChunk(int chunkIdx, byte[] data) throws IOException {
		assert(chunkIdx > 0 && chunkIdx < numChunks);
		assert(data.length == chunkSize || chunkIdx == numChunks-1);
		if(hasChunk(chunkIdx)) return;
		file.seek(chunkIdx*chunkSize, File.SEEK_SET);
		file.write(data);
		markChunk(chunkIdx);
	}
	
	public boolean hasChunk(int chunkIdx) {
		return (chunkMask[chunkIdx/8] & (1 << (chunkIdx % 8))) != 0;
	}
	
	public boolean isFinished() {
		return chunksObtained == numChunks;
	}
	
	protected synchronized void scanChunkMask(FS fs) throws IOException {
		if(!fs.exists(tempPath)) return;
		int chunkIdx = -1;
		
		file.rewind();
		while(file.hasData()) {
			chunkIdx++;
			byte[] chunk = file.read(chunkSize);
			boolean filled = false;
			for(byte b : chunk) {
				if(b != 0x00) {
					filled = true;
					break;
				}
			}
			
			if(filled) {
				markChunk(chunkIdx);
			}
		}
		
		file.close();
	}
	
	protected void markChunk(int chunkIdx) {
		chunksObtained++;
		chunkMask[chunkIdx/8] |= (1 << (chunkIdx % 8));
	}
}
