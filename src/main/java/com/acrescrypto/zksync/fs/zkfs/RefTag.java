package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.utility.Util;

// TODO Someday: (refactor) Confused terminology... "tag" can refer either to the bare hash, or this more fluent "RefTag".
/** Encodes a reference to file data. These are stored inside inodes to allow retrieval of file contents. The RefTag
 * for the inode table itself identifies a revision in the archive. RefTags contain certain metadata to indicate
 * how the content is stored. */
public class RefTag implements Comparable<RefTag> {
	protected ZKArchiveConfig config;
	protected RevisionInfo info;
	protected byte[] refTagBytes;
	protected StorageTag storageTag;
	protected byte archiveType, versionMajor, versionMinor;
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
	// rest is zero
	
	public static int REFTAG_SHORT_SIZE = 8;
	
	public static RefTag blank(ZKArchive archive) {
		return new RefTag(archive,
				new StorageTag(archive.getCrypto(), new byte[0]),
				RefTag.REF_TYPE_IMMEDIATE,
				0);
	}
	
	public RefTag(ZKArchive archive, byte[] refTagBytes) {
		this(archive.config, refTagBytes);
		cacheOnly = archive.isCacheOnly();
	}
	
	public RefTag(ZKArchive archive, StorageTag storageTag, int refType, long numPages) {
		this(archive.config, storageTag, refType, numPages);
		cacheOnly = archive.isCacheOnly(); 
	}
	
	public RefTag(ZKArchiveConfig config, StorageTag storageTag, int refType, long numPages) {
		this.config = config;
		this.storageTag = storageTag;
		this.refType = refType;
		this.numPages = numPages;
		this.refTagBytes = serialize();
	}
	
	public RefTag(ZKArchiveConfig config, byte[] tag) {
		if(tag == null) tag = blank(config.getArchive()).getBytes();
		
		this.config = config;
		deserialize(tag);
	}

	public byte[] getBytes() {
		return refTagBytes;
	}
	
	public StorageTag getStorageTag() {
		return storageTag;
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
		if(storageTag.getTagBytes().length > 0) return false;
		return true;
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(config.refTagSize());
		buf.put(storageTag.getPaddedTagBytes());
		buf.put(archiveType);
		buf.put(versionMajor);
		buf.put(versionMinor);
		buf.put((byte) (this.refType & 0x03));
		buf.putLong(numPages);
		buf.put(new byte[REFTAG_EXTRA_DATA_SIZE-2-8-1-1]);
		
		assert(!buf.hasRemaining());
		return buf.array();
	}
	
	protected void deserialize(byte[] serialized) {
		int expectedLen = config.accessor.master.crypto.hashLength() + REFTAG_EXTRA_DATA_SIZE;
		assert(serialized.length == expectedLen);
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		byte[] storageTagBytes = new byte[config.getCrypto().hashLength()];
		buf.get(storageTagBytes);
		this.archiveType = buf.get();
		this.versionMajor = buf.get();
		this.versionMinor = buf.get();
		this.refType = buf.get() & 0x03;
		this.numPages = buf.getLong();
		this.refTagBytes = serialized.clone();
		
		if(refType == REF_TYPE_IMMEDIATE) {
			this.storageTag = StorageTag.fromPaddedBytes(config.getCrypto(), storageTagBytes);
		} else {
			this.storageTag = new StorageTag(config.getCrypto(), storageTagBytes);
		}
	}
	
	public int hashCode() {
		return ByteBuffer.wrap(refTagBytes).getInt();
	}
	
	public boolean equals(Object other) {
		if(other == null || !other.getClass().equals(this.getClass())) return false;
		return Arrays.equals(((RefTag) other).refTagBytes, this.refTagBytes);
	}
	
	public int compareTo(RefTag other) {
		return Util.compareArrays(refTagBytes, other.refTagBytes);
	}
	
	public ZKArchiveConfig getConfig() {
		return config;
	}
	
	public ZKArchive getArchive() throws IOException {
		return cacheOnly ? config.archive.cacheOnlyArchive() : config.archive;
	}
	
	public String toString() {
		return Util.formatRefTag(this);
	}
}
