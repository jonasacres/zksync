package com.acrescrypto.zksyncweb.data;

import java.io.IOException;

import com.acrescrypto.zksync.fs.zkfs.RevisionInfo;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;

public class XRevisionInfo {
	private byte[] revTag;
	private Long generation;
	private byte[][] parents;
	
	public XRevisionInfo(RevisionTag tag) throws IOException {
		RevisionInfo info = tag.getInfo();
		this.setRevTag(tag.getBytes());
		this.setGeneration(info.getGeneration());
		this.setParents(new byte[info.getParents().size()][]);
		
		for(int i = 0; i < getParents().length; i++) {
			this.getParents()[i] = info.getParents().get(i).getBytes();
		}
	}

	public XRevisionInfo() {
	}

	public Long getGeneration() {
		return generation;
	}

	public void setGeneration(Long generation) {
		this.generation = generation;
	}

	public byte[] getRevTag() {
		return revTag;
	}

	public void setRevTag(byte[] revTag) {
		this.revTag = revTag;
	}

	public byte[][] getParents() {
		return parents;
	}

	public void setParents(byte[][] parents) {
		this.parents = parents;
	}
}
