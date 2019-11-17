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
	private byte[] refTagBytes;
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
		return blank(archive.config);
	}
	
	public static RefTag blank(ZKArchiveConfig config) {
		return new RefTag(config,
				new StorageTag(config.getCrypto(), new byte[0]),
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
	}
	
	public RefTag(ZKArchiveConfig config, byte[] tag) {
		if(tag == null) {
			try {
				tag = blank(config.getArchive()).getBytes();
			} catch(IOException exc) {
				// shouldn't be possible since we don't have to write a block
				exc.printStackTrace();
				throw new RuntimeException(exc);
			}
		}
		
		this.config = config;
		deserialize(tag);
	}

	public byte[] getBytes() throws IOException {
		if(refTagBytes == null) {
			refTagBytes = serialize(); 
		}
		
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
		if(!storageTag.isFinalized()) return false;
		if(storageTag.getTagBytesPreserialized().length > 0) return false;
		return true;
	}
	
	protected byte[] serialize() throws IOException {
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
	
	public boolean equals(Object other) {
		if(other == null || !other.getClass().equals(this.getClass())) return false;
		RefTag o = (RefTag) other;
		
		if(refType != o.refType) return false;
		if(numPages != o.numPages) return false;
		if(versionMajor != o.versionMajor) return false;
		if(versionMinor != o.versionMinor) return false;
		if(archiveType != o.archiveType) return false;
		
		if(storageTag.isFinalized() != o.storageTag.isFinalized()) return false;
		if(!storageTag.isFinalized()) {
			return storageTag.equals(o.storageTag);
		}

		try {
			return Arrays.equals(this.getBytes(), o.getBytes());
		} catch(IOException exc) {
			// shouldn't be possible since we checked that we finalized first
			exc.printStackTrace();
			throw new RuntimeException(exc);
		}
	}
	
	public int compareTo(RefTag other) {
		if(storageTag.isFinalized() != other.storageTag.isFinalized()) {
			if(storageTag.isFinalized()) {
				// those with finalized tag bytes sort before those with pending tags
				return -1;
			}
			
			return 1;
		}
		
		if(storageTag.isFinalized()) {
			try {
				return Util.compareArrays(getBytes(), other.getBytes());
			} catch (IOException exc) {
				// we checked finalized so this shouldn't be possible
				exc.printStackTrace();
				throw new RuntimeException(exc);
			}
		}
		
		return storageTag.compareTo(other.storageTag);
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
	
	@Override
	public int hashCode() {
		if(refTagBytes == null) {
			if(storageTag.isFinalized()) {
				try {
					getBytes(); // force serialization
				} catch(IOException exc) {
					// since storageTag has bytes, this should not be a possibility
					exc.printStackTrace();
					throw new RuntimeException(exc);
				}
			} else {
				return storageTag.hashCode();
			}
		}
		
		return ByteBuffer.wrap(refTagBytes).getInt();
	}
}
