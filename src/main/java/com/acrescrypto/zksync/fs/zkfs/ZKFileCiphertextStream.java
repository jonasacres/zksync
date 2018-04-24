package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.io.InputStream;

import com.acrescrypto.zksync.fs.File;

public class ZKFileCiphertextStream extends InputStream {
	RefTag tag;
	PageMerkle merkle;
	File current;
	int nextFileNum;
	byte[] buf = new byte[1];
	
	public ZKFileCiphertextStream(RefTag tag) throws IOException {
		this.tag = tag;
		this.merkle = new PageMerkle(tag);
	}
	
	@Override
	public int available() throws IOException {
		return current.available();
	}
	
	@Override
	public void close() throws IOException {
		current.close();
	}
	
	@Override
	public boolean markSupported() {
		return false;
	}
	
	@Override
	public int read() throws IOException {
		if(current == null) return -1;
		
		if(!current.hasData()) {
			if(!loadNextFile()) return -1;
		}
		
		current.read(buf, 0, 1);
		return buf[0];
	}
	
	@Override
	public int read(byte[] buf, int off, int len) throws IOException {
		int numBytesRead = 0;
		if(off < 0 || len > buf.length-off) throw new IndexOutOfBoundsException();
		while(numBytesRead < len) {
			if((current == null || !current.hasData()) && !loadNextFile()) break;
			numBytesRead += current.read(buf, off+numBytesRead, len-numBytesRead);
		}
		
		return numBytesRead == 0 ? -1 : numBytesRead;
	}
	
	// TODO: consider a faster skip(), but right now I don't think we ever call it
	
	public long numFiles() {
		return tag.numPages + merkle.numChunks();
	}
	
	protected boolean loadNextFile() throws IOException {
		if(current != null) current.close();

		if(nextFileNum < numFiles()) {
			nextFileNum++;
			if(nextFileNum <= numFiles()) {
				current = tag.getArchive().getStorage().open(currentFilePath(), File.O_RDONLY);
				return true;
			}
		}
		
		current = null;
		return false;
	}
	
	protected String currentFilePath() throws IOException {
		if(nextFileNum-1 <= merkle.numChunks()) return PageMerkle.pathForChunk(merkle.tag, nextFileNum);
		return Page.pathForTag(tag.archive, merkle.getPageTag(nextFileNum-merkle.numChunks()));
	}
}
