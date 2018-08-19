package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.utility.Util;

public class RevisionTag {
	RefTag refTag;
	int numParents;
	long height;
	byte[] serialized;
	int hashCode;
	RevisionInfo info;
	boolean cacheOnly;

	public static int sizeForConfig(ZKArchiveConfig config) {
		return config.refTagSize() + 8 + 4 + config.getCrypto().asymSignatureSize();
	}
	
	public static RevisionTag blank(ZKArchiveConfig config) {
		return new RevisionTag(RefTag.blank(config), 0, 0);
	}
	
	public RevisionTag(RefTag refTag, int numParents, long height) {
		this.refTag = refTag;
		this.height = height;
		this.numParents = numParents;
		serialize();
	}
	
	public RevisionTag(ZKArchiveConfig config, byte[] serialization) {
		deserialize(config, serialization);
	}
	
	public ZKArchive getArchive() {
		return refTag.config.getArchive();
	}
	
	public ZKArchiveConfig getConfig() {
		return refTag.config;
	}
	
	public RefTag getRefTag() {
		return refTag;
	}
	
	public byte[] getBytes() {
		return serialized;
	}
	
	public RevisionInfo getInfo() throws IOException {
		if(info == null) info = readOnlyFS().getRevisionInfo();
		return info;
	}
	
	public ZKFS readOnlyFS() throws IOException {
		if(cacheOnly) {
			return refTag.config.archive.cacheOnlyArchive().readOnlyFilesystems.get(this);
		} else {
			return refTag.config.archive.readOnlyFilesystems.get(this);
		}
	}
	
	public RevisionTag makeCacheOnly() {
		RevisionTag tag = new RevisionTag(refTag, numParents, height);
		tag.cacheOnly = true;
		return tag;
	}
	
	public ZKFS getFS() throws IOException {
		return new ZKFS(this);
	}
	
	public byte[] serialize() {
		Key key = refTag.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_REFTAG);
		byte[] ciphertext = key.encryptCBC(new byte[key.getCrypto().symBlockSize()], refTag.serialize());
		
		ByteBuffer buffer = ByteBuffer.allocate(sizeForConfig(refTag.getConfig()));
		buffer.put(ciphertext);
		buffer.putLong(height);
		buffer.putInt(numParents);
		buffer.put(refTag.config.privKey.sign(buffer.array(), 0, buffer.position()));
		
		serialized = buffer.array();
		hashCode = ByteBuffer.wrap(serialized).getInt();
		return serialized;
	}
	
	public void deserialize(ZKArchiveConfig config, byte[] serialized) {
		assert(serialized.length == sizeForConfig(config));
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		int signedLen = serialized.length - config.getCrypto().asymSignatureSize();
		config.pubKey.assertValid(serialized, 0, signedLen,
				serialized, signedLen, config.getCrypto().asymSignatureSize());
		
		byte[] encryptedRefTag = new byte[config.refTagSize()];		
		buf.get(encryptedRefTag);
		height = buf.getLong();
		numParents = buf.getInt();
	
		assert(height > 0);
		assert(numParents > 0);
		
		if(!config.accessor.isSeedOnly()) {
			Key key = config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_REFTAG);
			byte[] rawRefTag = key.decryptCBC(new byte[key.getCrypto().symBlockSize()], encryptedRefTag);
			refTag = new RefTag(config, rawRefTag);
		}
	}
	
	public int compareTo(RevisionTag other) {
		if(this.height != other.height) return Long.compare(this.height, other.height);
		if(this.numParents != other.numParents) return Integer.compare(this.numParents, other.numParents);
		return refTag.compareTo(other.refTag);
	}
	
	public boolean equals(Object other) {
		if(!(other instanceof RevisionTag)) return false;
		return compareTo((RevisionTag) other) == 0;
	}
	
	public String toString() {
		return Util.bytesToHex(serialized, 4);
	}
	
	public int hashCode() {
		return hashCode;
	}

	public long getShortHash() {
		return Util.shortTag(serialized);
	}
}
