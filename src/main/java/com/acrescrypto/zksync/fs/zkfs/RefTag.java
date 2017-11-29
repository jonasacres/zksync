package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.Util;

public class RefTag implements Comparable<RefTag> {
	protected ZKArchive archive;
	protected ZKFS readOnlyFs;
	protected RevisionInfo info;
	protected byte[] tag, hash;
	protected int refType;	
	protected long numPages;
	public static final byte REF_TYPE_2INDIRECT = 2;
	public static final byte REF_TYPE_INDIRECT = 1;
	public static final byte REF_TYPE_IMMEDIATE = 0;
	
	public static int REFTAG_EXTRA_DATA_SIZE = 16;
	// reserve 16 bytes because you know we'll use it, and it probably won't be enough
	// 1 byte encoding type
	// 8 bytes num chunks
	
	public static RefTag blank(ZKArchive archive) {
		return new RefTag(archive, new byte[0], RefTag.REF_TYPE_IMMEDIATE, 0);
	}
	
	public RefTag(ZKArchive archive, byte[] tag) {
		if(tag == null) tag = blank(archive).getBytes();
		
		this.archive = archive;
		deserialize(tag);
	}
	
	public RefTag(ZKArchive archive, byte[] hash, int refType, long numPages) {
		this.archive = archive;
		this.hash = padHash(hash);
		this.refType = refType;
		this.numPages = numPages;
		this.tag = serialize();
	}
	
	public byte[] padHash(byte[] hash) {
		// this breaks if the hash length is > 255 bytes, but honestly, who needs a hash that long?
		byte needed = (byte) (archive.crypto.hashLength() - hash.length);
		if(needed == 0) return hash;
		ByteBuffer buf = ByteBuffer.allocate(archive.crypto.hashLength());
		buf.put(hash);
		while(buf.hasRemaining()) buf.put(needed);
		return buf.array();
	}
	
	public byte[] unpadHash(byte[] hash) {
		if(refType != RefTag.REF_TYPE_IMMEDIATE) return hash;
		int len = archive.crypto.hashLength() - hash[hash.length-1];
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
	
	public long getNumPages() {
		return numPages;
	}
	
	public boolean isBlank() {
		if(numPages != 0) return false;
		if(refType != RefTag.REF_TYPE_IMMEDIATE) return false;
		for(int i = 0; i < hash.length; i++) if(hash[i] != archive.crypto.hashLength()) return false;
		return true;
	}
	
	public byte[] getLiteral() {
		return unpadHash(hash);
	}
	
	public RevisionInfo getInfo() throws IOException {
		if(info == null) info = readOnlyFS().getRevisionInfo();
		return info;
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(archive.crypto.hashLength() + REFTAG_EXTRA_DATA_SIZE);
		buf.put(hash);
		buf.put((byte) (this.refType & 0x03));
		buf.putLong(numPages);
		buf.put(new byte[REFTAG_EXTRA_DATA_SIZE-8-1]);
		
		assert(!buf.hasRemaining());
		return buf.array();
	}
	
	protected void deserialize(byte[] serialized) {
		int expectedLen = archive.crypto.hashLength() + REFTAG_EXTRA_DATA_SIZE;
		assert(serialized.length == expectedLen);
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		this.hash = new byte[archive.crypto.hashLength()];
		buf.get(hash);
		this.refType = buf.get() & 0x03;
		this.numPages = buf.getLong();
		this.tag = serialized.clone();
	}
	
	public ZKFS readOnlyFS() throws IOException {
		/* TODO: don't like that these accumulate in memory... want some sort of eviction queue across all RefTags.
		 * Maybe one we can reuse for the directory cache in ZKFS?
		 */
		if(readOnlyFs == null) readOnlyFs = getFS();
		return readOnlyFs;
	}
	
	public ZKFS getFS() throws IOException {
		return new ZKFS(this);
	}
	
	public int hashCode() {
		return ByteBuffer.wrap(tag).getInt();
	}
	
	public boolean equals(Object other) {
		if(other == null || !other.getClass().equals(this.getClass())) return false;
		return Arrays.equals(((RefTag) other).tag, this.tag);
	}
	
	public int compareTo(RefTag other) {
		try {
			if(archive.getRevisionTree().ancestorsOf(this).contains(other)) return 1;
			if(archive.getRevisionTree().ancestorsOf(other).contains(this)) return -1;
		} catch(Exception exc) {
			throw new RuntimeException("Caught exception comparing revisions");
		}
		
		int r = (new Long(info.inode.modifiedTime)).compareTo(other.info.inode.modifiedTime);
		if(r != 0) return r;
		
		for(int i = 0; i < tag.length; i++) if(tag[i] != other.tag[i]) return Byte.valueOf(tag[i]).compareTo(other.tag[i]);
		return 0;
	}
	
	public ZKArchive getArchive() {
		return archive;
	}
	
	public String toString() {
		return "RefTag " + Util.bytesToHex(tag);
	}
}
