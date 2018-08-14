package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.utility.Util;

// TODO: (refactor) Confused terminology... "tag" can refer either to the bare hash, or this more fluent "RefTag".
/** Encodes a reference to file data. These are stored inside inodes to allow retrieval of file contents. The RefTag
 * for the inode table itself identifies a revision in the archive. RefTags contain certain metadata to indicate
 * how the content is stored. */
public class RefTag implements Comparable<RefTag> {
	public final static byte FLAG_NO_NEW_CONTENT = 1 << 0;
	
	protected ZKArchiveConfig config;
	protected RevisionInfo info;
	protected byte[] tag, hash;
	protected byte archiveType, versionMajor, versionMinor, flags;
	protected int refType;	
	protected long numPages;
	protected boolean cacheOnly;
	
	/** doubly-indirect storage; tag points to a tree containing tags for each page of data. */
	public static final byte REF_TYPE_2INDIRECT = 2;
	
	/** indirect storage; tag points to a single page, containing the data. */
	public static final byte REF_TYPE_INDIRECT = 1;
	
	/** immediate storage; tag contains the literal data of the file. */
	public static final byte REF_TYPE_IMMEDIATE = 0;
	
	public static int REFTAG_EXTRA_DATA_SIZE = 16;
	// reserve 16 bytes because you know we'll use it, and it probably won't be enough
	// 1 byte archive type
	// 2 bytes version
	// 1 byte ref type
	// 8 bytes num chunks
	// 1 byte flags
	
	public static int REFTAG_SHORT_SIZE = 8;
	
	public static RefTag blank(ZKArchive archive) {
		return new RefTag(archive, new byte[0], RefTag.REF_TYPE_IMMEDIATE, 0);
	}
	
	public static RefTag blank(ZKArchiveConfig config) {
		return new RefTag(config, new byte[0], RefTag.REF_TYPE_IMMEDIATE, 0);
	}
	
	public RefTag(ZKArchive archive, byte[] tag) {
		this(archive.config, tag);
		cacheOnly = archive.isCacheOnly();
	}
	
	public RefTag(ZKArchive archive, byte[] hash, int refType, long numPages) {
		this(archive.config, hash, refType, numPages);
		cacheOnly = archive.isCacheOnly(); 
	}
	
	public RefTag(ZKArchiveConfig config, byte[] hash, int refType, long numPages) {
		this.config = config;
		this.hash = padHash(hash);
		this.refType = refType;
		this.numPages = numPages;
		this.tag = serialize();
	}
	
	public RefTag(ZKArchiveConfig config, byte[] tag) {
		if(tag == null) tag = blank(config).getBytes();
		
		this.config = config;
		deserialize(tag);
	}
	
	public boolean hasFlag(byte flag) {
		return (flags & flag) != 0;
	}
	
	public void setFlag(byte flag) {
		flags |= flag;
		this.tag = serialize();
	}
	
	public byte getFlags() {
		return flags;
	}
	
	public byte[] padHash(byte[] hash) {
		// this breaks if the hash length is > 255 bytes, but honestly, who needs a hash that long?
		byte needed = (byte) (config.accessor.master.crypto.hashLength() - hash.length);
		if(needed == 0) return hash;
		ByteBuffer buf = ByteBuffer.allocate(config.accessor.master.crypto.hashLength());
		buf.put(hash);
		while(buf.hasRemaining()) buf.put(needed);
		return buf.array();
	}
	
	public byte[] unpadHash(byte[] hash) {
		if(refType != RefTag.REF_TYPE_IMMEDIATE) return hash;
		int len = config.accessor.master.crypto.hashLength() - hash[hash.length-1];
		if(len <= 0) return new byte[0];
		ByteBuffer buf = ByteBuffer.allocate(len);
		buf.put(hash, 0, Math.min(hash.length, len));
		return buf.array();
	}
	
	public byte[] getBytes() {
		return tag;
	}
	
	public byte[] getHash() {
		return hash;
	}
	
	public byte[] getShortHashBytes() {
		byte[] shortHash = new byte[8];
		for(int i = 0; i < shortHash.length; i++) shortHash[i] = hash[i];
		return shortHash;
	}
	
	public long getShortHash() {
		return ByteBuffer.wrap(hash).getLong();
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
		for(int i = 0; i < hash.length; i++) if(hash[i] != config.getCrypto().hashLength()) return false;
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
		ByteBuffer buf = ByteBuffer.allocate(config.refTagSize());
		buf.put(hash);
		buf.put(archiveType);
		buf.put(versionMajor);
		buf.put(versionMinor);
		buf.put((byte) (this.refType & 0x03));
		buf.putLong(numPages);
		buf.put(flags);
		buf.put(new byte[REFTAG_EXTRA_DATA_SIZE-2-8-1-1-1]);
		
		assert(!buf.hasRemaining());
		return buf.array();
	}
	
	protected void deserialize(byte[] serialized) {
		int expectedLen = config.accessor.master.crypto.hashLength() + REFTAG_EXTRA_DATA_SIZE;
		assert(serialized.length == expectedLen);
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		this.hash = new byte[config.getCrypto().hashLength()];
		buf.get(hash);
		this.archiveType = buf.get();
		this.versionMajor = buf.get();
		this.versionMinor = buf.get();
		this.refType = buf.get() & 0x03;
		this.numPages = buf.getLong();
		this.flags = buf.get();
		this.tag = serialized.clone();
	}
	
	public ZKFS readOnlyFS() throws IOException {
		if(cacheOnly) {
			return config.archive.cacheOnlyArchive().readOnlyFilesystems.get(this);
		} else {
			return config.archive.readOnlyFilesystems.get(this);
		}
	}
	
	public RefTag makeCacheOnly() {
		RefTag tag = new RefTag(config, serialize());
		tag.cacheOnly = true;
		return tag;
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
		if(this.equals(other)) return 0;
		
		try {
			if(config.getRevisionTree().tagHasAncestor(this, other)) return 1;
			if(config.getRevisionTree().tagHasAncestor(other, this)) return -1;
		} catch(Exception exc) {
			// can't load at least one of the two revisions; proceed to other comparisons
		}
		
		try {
			if(getInfo() != null && other.getInfo() != null) {
				int r = (new Long(info.inode.modifiedTime)).compareTo(other.info.inode.modifiedTime);
				if(r != 0) return r;
			}
		} catch (IOException e) {
		}
		
		for(int i = 0; i < tag.length; i++) {
			int v = Util.unsignByte(tag[i]) - Util.unsignByte(other.tag[i]);
			if(v != 0) return v;
		}
		
		return 0; // shouldn't be possible since we checked equality at start
	}
	
	public ZKArchiveConfig getConfig() {
		return config;
	}
	
	public ZKArchive getArchive() throws IOException {
		return cacheOnly ? config.archive.cacheOnlyArchive() : config.archive;
	}
	
	public String toString() {
		return "RefTag " + Util.bytesToHex(tag);
	}

	public ObfuscatedRefTag obfuscate() throws IOException {
		return new ObfuscatedRefTag(this);
	}
}
