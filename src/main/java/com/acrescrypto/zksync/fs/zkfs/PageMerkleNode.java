package com.acrescrypto.zksync.fs.zkfs;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.CryptoSupport;

public class PageMerkleNode {
	byte[] tag;
	boolean dirty;
	PageMerkleNode parent, left, right;
	CryptoSupport crypto;
	
	PageMerkleNode(CryptoSupport crypto) {
		this.crypto = crypto;
		this.tag = new byte[crypto.hashLength()];
		this.dirty = true;
	}
	
	void setTag(byte[] pageTag) {
		if(Arrays.equals(pageTag, this.tag)) return;
		this.tag = pageTag.clone();
		dirty = false;
		if(parent != null) parent.markDirty();
	}
	
	void recalculate() {
		if(!dirty) return;
		PageMerkleNode[] nodes = { left, right }; 
		if(left == null || right == null) {
			if(left != null || right != null) throw new IllegalStateException();
			return; // don't mess with tags of leaf nodes
		}
		
		ByteBuffer merged = ByteBuffer.allocate(2*crypto.hashLength());
		for(PageMerkleNode child : nodes) {
			child.recalculate();
			merged.put(child.tag);
		}
		
		this.tag = crypto.hash(merged.array());
		dirty = false;
	}
	
	void markDirty() {
		dirty = true;
		if(parent != null) parent.markDirty();
	}

	public boolean isBlank() {
		for(int i = 0; i < tag.length; i++) if(tag[i] != tag.length) return false;
		return true;
	}
}
