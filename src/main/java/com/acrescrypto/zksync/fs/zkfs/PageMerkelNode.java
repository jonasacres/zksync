package com.acrescrypto.zksync.fs.zkfs;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Ciphersuite;

public class PageMerkelNode {
	byte[] tag;
	boolean dirty;
	PageMerkelNode parent, left, right;
	Ciphersuite ciphersuite;
	
	PageMerkelNode(Ciphersuite suite) {
		this.ciphersuite = suite;
		this.tag = new byte[suite.hashLength()];
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
		
		ByteBuffer merged = ByteBuffer.allocate(2*ciphersuite.hashLength());
		for(PageMerkelNode child : nodes) {
			if(child == null) continue;
			if(child.dirty) child.recalculate();
			merged.put(child.tag);
		}
		
		this.tag = ciphersuite.hash(merged.array());
		dirty = false;
	}
	
	void markDirty() {
		dirty = true;
		if(parent != null) parent.markDirty();
	}
}
