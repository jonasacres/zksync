package com.acrescrypto.zksync.fs.zkfs;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.CryptoSupport;

/** describes an individual node within a PageMerkle tree. */
public class PageMerkleNode {
	byte[] tag; /** tag stored in this node */
	boolean dirty; /** true if tag needs to be recalculated */
	PageMerkleNode parent; /** parent node */
	PageMerkleNode left; /** left child node */
	PageMerkleNode right; /** right child node */
	CryptoSupport crypto; /** crypto algorithms */
	
	/** Initialize a new blank PageMerkleNode */
	PageMerkleNode(CryptoSupport crypto) {
		this.crypto = crypto;
		this.tag = new byte[crypto.hashLength()];
		for(int i = 0; i < tag.length; i++) tag[i] = (byte) tag.length;
		this.dirty = true;
	}
	
	/** Set the tag value of the PageMerkleNode */
	void setTag(byte[] pageTag) {
		if(Arrays.equals(pageTag, this.tag)) return;
		this.tag = pageTag.clone();
		dirty = false;
		if(parent != null) parent.markDirty();
	}
	
	/** Recalculate tag based on tags of children. No effect on leaf nodes, whose tags are set manually via setTag. */ 
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
	
	/** Mark node as dirty. Also marks all ancestor nodes as dirty as well. */
	void markDirty() {
		dirty = true;
		if(parent != null) parent.markDirty();
	}

	/** true if tag contains "blank" value */
	public boolean isBlank() {
		for(int i = 0; i < tag.length; i++) if(tag[i] != tag.length) return false;
		return true;
	}
}
