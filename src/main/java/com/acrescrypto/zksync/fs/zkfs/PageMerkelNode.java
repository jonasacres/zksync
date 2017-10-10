package com.acrescrypto.zksync.fs.zkfs;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.CryptoSupport;

public class PageMerkelNode {
	byte[] tag;
	boolean dirty;
	PageMerkelNode parent, left, right;
	CryptoSupport crypto;
	
	PageMerkelNode(CryptoSupport crypto) {
		this.crypto = crypto;
		this.tag = new byte[crypto.hashLength()];
	}
	
	void setTag(byte[] pageTag) {
		if(Arrays.equals(pageTag, this.tag)) return;
		this.tag = pageTag.clone();
		dirty = false;
		if(parent != null) parent.markDirty();
	}
	
	void recalculate() {
		if(!dirty) return;
		PageMerkelNode[] nodes = { left, right }; 
		
		ByteBuffer merged = ByteBuffer.allocate(2*crypto.hashLength());
		for(PageMerkelNode child : nodes) {
			if(child == null) continue;
			if(child.dirty) child.recalculate();
			merged.put(child.tag);
		}
		
		this.tag = crypto.hash(merged.array());
		dirty = false;
	}
	
	void markDirty() {
		dirty = true;
		if(parent != null) parent.markDirty();
	}
}
