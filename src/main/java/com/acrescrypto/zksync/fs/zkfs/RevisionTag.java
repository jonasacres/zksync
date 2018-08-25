package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.utility.Util;

public class RevisionTag implements Comparable<RevisionTag> {
	RefTag refTag;
	long height;
	long parentHash;
	byte[] serialized;
	int hashCode;
	RevisionInfo info;
	boolean cacheOnly;

	public static int sizeForConfig(ZKArchiveConfig config) {
		return config.refTagSize() + 8 + 8 + config.getCrypto().asymSignatureSize();
	}
	
	public static RevisionTag blank(ZKArchiveConfig config) {
		return new RevisionTag(RefTag.blank(config), 0, 0);
	}
	
	public RevisionTag(RefTag refTag, long parentHash, long height) {
		this.refTag = refTag;
		this.parentHash = parentHash;
		this.height = height;
		serialize();
	}
	
	public RevisionTag(ZKArchiveConfig config, byte[] serialization) {
		deserialize(config, serialization);
	}
	
	public ZKArchive getArchive() throws IOException {
		ZKArchive archive = refTag.config.getArchive();
		if(cacheOnly && !archive.isCacheOnly()) {
			archive = archive.cacheOnlyArchive();
		}
		return archive;
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
	
	public long getHeight() {
		return height;
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
		RevisionTag tag = new RevisionTag(refTag, parentHash, height);
		tag.cacheOnly = true;
		return tag;
	}
	
	public ZKFS getFS() throws IOException {
		return new ZKFS(this);
	}
	
	public byte[] serialize() {
		/* This is intended to be an obfuscation, and not true encryption. The use of CBC mode with a fixed IV
		 * creates the possibility of revealing shared prefixes between hashes. However, since we have
		 * 128-bit blocks, this will happen at an average rate of 1 in 2**64 revtags.
		 * 
		 * We do not want randomized IVs, because we want each serialization to be identical so peers know when
		 * they're looking at the same revtag.
		 * 
		 * At this time, there do not seem to be obvious serious consequences to an adversary learning one or more
		 * revtags. But since we want our adversaries in the dark as much as possible, we obfuscate.
		 */
		ByteBuffer plaintext = ByteBuffer.allocate(sizeForConfig(refTag.config) - refTag.config.getCrypto().asymSignatureSize());
		plaintext.put(refTag.serialize());
		plaintext.putLong(parentHash);
		plaintext.putLong(height);

		Key key = refTag.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_REFTAG);
		byte[] ciphertext = key.encryptCBC(new byte[key.getCrypto().symBlockSize()], plaintext.array());
		
		ByteBuffer signedCiphertext = ByteBuffer.allocate(sizeForConfig(refTag.getConfig()));
		signedCiphertext.put(ciphertext);
		signedCiphertext.put(refTag.config.privKey.sign(signedCiphertext.array(), 0, signedCiphertext.position()));
		
		serialized = signedCiphertext.array();
		hashCode = ByteBuffer.wrap(serialized).getInt();
		return serialized;
	}
	
	public void deserialize(ZKArchiveConfig config, byte[] serialized) {
		assert(serialized.length == sizeForConfig(config));
		if(Arrays.equals(new byte[serialized.length], serialized)) {
			this.refTag = RefTag.blank(config);
			this.height = 0;
			this.parentHash = 0;
			return;
		}
		
		int signedLen = serialized.length - config.getCrypto().asymSignatureSize();
		config.pubKey.assertValid(serialized, 0, signedLen,
				serialized, signedLen, config.getCrypto().asymSignatureSize());
		
		hashCode = ByteBuffer.wrap(serialized).getInt();
		this.serialized = serialized.clone();
	
		assert(height >= 0);
		
		if(!config.accessor.isSeedOnly()) {
			Key key = config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_REFTAG);
			byte[] rawBytes = key.decryptCBC(new byte[key.getCrypto().symBlockSize()],
					serialized, 0, serialized.length - config.getCrypto().asymSignatureSize());
			ByteBuffer raw = ByteBuffer.wrap(rawBytes);
			byte[] rawRefTag = new byte[config.refTagSize()];
			raw.get(rawRefTag);
			parentHash = raw.getLong();
			height = raw.getLong();
			refTag = new RefTag(config, rawRefTag);
		}
	}
	
	public int compareTo(RevisionTag other) {
		if(this.height != other.height) return Long.compare(this.height, other.height);
		return Util.compareArrays(serialized, other.serialized);
	}
	
	public boolean equals(Object other) {
		if(!(other instanceof RevisionTag)) return false;
		return compareTo((RevisionTag) other) == 0;
	}
	
	public String toString() {
		return Util.bytesToHex(serialized, 4) + " height=" + height + " parentHash=" + String.format("%016x", parentHash);
	}
	
	public int hashCode() {
		return hashCode;
	}

	public long getShortHash() {
		return Util.shortTag(serialized);
	}
}
