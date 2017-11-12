package com.acrescrypto.zksync.fs.zkfs;

import java.nio.ByteBuffer;

public class RefTag {
	protected ZKFS fs;
	protected byte[] tag, hash;
	protected int refType;
	protected long numPages; // TODO: really think this through. 64 bits is safe, but big. 32-bit would be 2^32 pages, or 2^48 bytes by default.
	
	public static int REFTAG_EXTRA_DATA_SIZE = 16;
	// reserve 16 bytes because you know we'll use it, and it probably won't be enough
	// 1 byte encoding type
	// 8 bytes num chunks
	
	public static RefTag blank(ZKFS fs) {
		return new RefTag(fs, new byte[0], Inode.REF_TYPE_IMMEDIATE, 0);
	}
	
	public RefTag(ZKFS fs, byte[] tag) {
		if(tag.length != fs.crypto.hashLength() + REFTAG_EXTRA_DATA_SIZE) throw new RuntimeException("received invalid reftag");
		ByteBuffer buf = ByteBuffer.wrap(tag);
		
		// TODO: I'm not sure refType is even needed anymore, if numChunks is always known.
		// 0 => immediate, 1 => indirect, 2 => 2indirect
		
		this.fs = fs;
		this.tag = tag;
		this.hash = new byte[fs.crypto.hashLength()];
		buf.get(this.hash);
		this.refType = buf.get() & 0x03; // only use low-order two bits for field; others are reserved
		this.numPages = buf.getLong();
	}
	
	public RefTag(ZKFS fs, byte[] hash, int refType, long numPages) {
		this.fs = fs;
		this.hash = hash; // TODO: BEWARE! Immediates must be padded.
		this.refType = refType;
		this.numPages = numPages;
		this.tag = serialize();
	}
	
	public byte[] padHash(byte[] hash) {
		// this breaks if the hash length is > 255 bytes, but honestly, who needs a hash that long?
		byte needed = (byte) (hash.length - fs.crypto.hashLength());
		if(needed == 0) return hash;
		ByteBuffer buf = ByteBuffer.allocate(fs.crypto.hashLength());
		buf.put(hash);
		while(buf.hasRemaining()) buf.put(needed);
		return buf.array();
	}
	
	public byte[] unpadHash(byte[] hash) {
		if(refType != Inode.REF_TYPE_IMMEDIATE) return hash; // TODO: consider numPages == 1
		int len = fs.crypto.hashLength() - hash[hash.length-1];
		ByteBuffer buf = ByteBuffer.allocate(len);
		buf.put(hash, 0, len);
		return buf.array();
	}
	
	public byte[] getBytes() {
		return tag;
	}
	
	public byte[] getHash() {
		return hash;
	}
	
	public int getRefType() {
		return refType;
	}
	
	public long getNumChunks() {
		return numPages;
	}
	
	public boolean isBlank() {
		if(numPages != 0) return false;
		if(refType != Inode.REF_TYPE_IMMEDIATE) return false;
		for(int i = 0; i < hash.length; i++) if(hash[i] != fs.crypto.hashLength()) return false;
		return true;
	}
	
	public void setLiteral(byte[] literal, int offset, int length) {
		ByteBuffer buf = ByteBuffer.allocate(length);
		buf.put(literal, offset, length);
		this.hash = padHash(buf.array());
	}
	
	public RevisionInfo getInfo() {
		return null; // TODO: this is gonna hurt...
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(fs.crypto.hashLength() + REFTAG_EXTRA_DATA_SIZE);
		buf.put(hash);
		buf.put((byte) (this.refType & 0x03));
		buf.putLong(numPages);
		return buf.array();
	}
}
