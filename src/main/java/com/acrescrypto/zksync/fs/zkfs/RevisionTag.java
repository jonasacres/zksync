package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.InvalidRevisionTagException;
import com.acrescrypto.zksync.utility.HashCache;
import com.acrescrypto.zksync.utility.Util;

public class RevisionTag implements Comparable<RevisionTag> {
	protected static HashCache<RevisionTag,Boolean> verificationCache =
			new HashCache<>(128,
					(tag)->tag.isValidUncached(),
					(tag, isValid)->{});
	
	private RefTag refTag;
	private long height = -1;
	private long parentHash = -1;
	byte[] serialized;
	int hashCode;
	RevisionInfo info;
	boolean cacheOnly;
	ZKArchiveConfig config;

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
		this.config = refTag.config;
		serialize();
	}
	
	public RevisionTag(ZKArchiveConfig config, byte[] serialization, boolean verifySignature) {
		this(config, ByteBuffer.wrap(serialization), verifySignature);
	}
	
	public RevisionTag(ZKArchiveConfig config, ByteBuffer buf, boolean verifySignature) {
		this.config = config;
		deserialize(buf, verifySignature);
	}

	public ZKArchive getArchive() throws IOException {
		ZKArchive archive = getRefTag().config.getArchive();
		if(cacheOnly && !archive.isCacheOnly()) {
			archive = archive.cacheOnlyArchive();
		}
		return archive;
	}
	
	public ZKArchiveConfig getConfig() {
		return config;
	}
	
	public boolean isUnpacked() {
		return refTag != null;
	}
	
	public RefTag getRefTag() {
		unpack();
		return refTag;
	}
	
	public byte[] getBytes() {
		return serialized;
	}
	
	public long getHeight() {
		unpack();
		return height;
	}
	
	public long getParentHash() {
		unpack();
		return parentHash;
	}
	
	public RevisionInfo getInfo() throws IOException {
		try(ZKFS fs = readOnlyFS()) {
			if(info == null) info = fs.getRevisionInfo();
			return info;
		}
	}
	
	public ZKFS readOnlyFS() throws IOException {
		if(cacheOnly) {
			return refTag.config.archive.cacheOnlyArchive().openRevisionReadOnly(this);
		} else {
			return refTag.config.archive.openRevisionReadOnly(this);
		}
	}
	
	public RevisionTag makeCacheOnly() throws IOException {
		if(cacheOnly) return this;
		RefTag coRefTag = new RefTag(refTag.getArchive().cacheOnlyArchive(), refTag.getBytes());
		RevisionTag tag = new RevisionTag(coRefTag, parentHash, height);
		tag.cacheOnly = true;
		return tag;
	}
	
	public ZKFS getFS() throws IOException {
		return new ZKFS(this);
	}
	
	public byte[] serialize() {
		if(refTag == null) {
			// TODO API: (coverage) branch
			// no plaintext, so we must have read this as a seed-only peer; serialization will be set
			return serialized;
		}
		
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

		Key key = refTag.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, "easysafe-reftag-key");
		byte[] ciphertext = key.encryptUnauthenticated(new byte[key.getCrypto().symIvLength()], plaintext.array());
		
		ByteBuffer signedCiphertext = ByteBuffer.allocate(sizeForConfig(refTag.getConfig()));
		signedCiphertext.put(ciphertext);
		signedCiphertext.put(refTag.config.privKey.sign(signedCiphertext.array(), 0, signedCiphertext.position()));
		
		serialized = signedCiphertext.array();
		hashCode = ByteBuffer.wrap(serialized).getInt();
		return serialized;
	}
	
	public void deserialize(ByteBuffer buf, boolean verifySignature) {
		byte[] serialized = new byte[sizeForConfig(config)];
		if(buf.remaining() < sizeForConfig(config)) {
			throw new InvalidRevisionTagException(Util.bytesToHex(buf.array()));
		}
		buf.get(serialized);
		
		if(Arrays.equals(new byte[serialized.length], serialized)) {
			this.refTag = RefTag.blank(config);
			this.height = 0;
			this.parentHash = 0;
			return;
		}
		
		hashCode = ByteBuffer.wrap(serialized).getInt();
		this.serialized = serialized;
		
		// we can move unpack after verification, but then we don't have height info available in debug logs
		unpack();

		if(verifySignature) {
			assertValid();
		}
	}
	
	/** Returns true if we've got all the pages for the inode table and directories cached
	 * locally. */
	public boolean hasStructureLocally() throws IOException {
		PageTree tree = new PageTree(getRefTag());
		if(!tree.exists()) return false;
		
		try(ZKFS fs = readOnlyFS()) {
			for(Inode inode : fs.getInodeTable().values()) {
				if(inode.isDeleted()) continue;
				if(!inode.getStat().isDirectory()) continue;
				
				PageTree dirTree = new PageTree(inode);
				if(!dirTree.exists()) return false;
			}
		}
		
		return true;
	}
	
	public boolean waitForStructure(long timeoutMs) throws IOException {
		long deadline;
		if(timeoutMs < 0) {
			deadline = Long.MAX_VALUE;
		} else {
			deadline = System.currentTimeMillis() + timeoutMs;
		}
		
		while(!hasStructureLocally()) {
			if(System.currentTimeMillis() >= deadline) return false;
			long waitIntervalMs = timeoutMs < 0 ? timeoutMs : deadline - System.currentTimeMillis();
			config.getSwarm().waitForPage(waitIntervalMs);
		}
		
		return true;
	}
	
	public boolean isValid() {
		try {
			return verificationCache.get(this);
		} catch (IOException exc) {
			// this shouldn't actually be possible, but HashCache contract requires IOException handling
			throw new RuntimeException("Caught IOException in validating revision tag " + Util.formatRevisionTag(this), exc);
		}
	}
	
	public boolean isValidUncached() {
		int signedLen = serialized.length - config.getCrypto().asymSignatureSize();
		return config.pubKey.verify(serialized, 0, signedLen,
				serialized, signedLen, config.getCrypto().asymSignatureSize());
	}
	
	public void assertValid() {
		if(isValid()) return;
		throw new SecurityException("Error authenticating revision tag: " + Util.formatRevisionTag(this));
	}
	
	protected void unpack() {
		if(refTag != null) return;
		if(config.accessor.isSeedOnly()) return;
		if(config.archiveRoot == null) return;
		
		Key key = config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, "easysafe-reftag-key");
		byte[] rawBytes = key.decryptUnauthenticated(new byte[key.getCrypto().symIvLength()],
				serialized, 0, serialized.length - config.getCrypto().asymSignatureSize());
		ByteBuffer raw = ByteBuffer.wrap(rawBytes);
		byte[] rawRefTag = new byte[config.refTagSize()];
		raw.get(rawRefTag);
		parentHash = raw.getLong();
		height = raw.getLong();
		refTag = new RefTag(config, rawRefTag);
		if(height < 0) throw new SecurityException();
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
		return Util.formatRevisionTag(this) + " parentHash=" + String.format("%016x", parentHash);
	}
	
	public int hashCode() {
		return hashCode;
	}

	public long getShortHash() {
		return Util.shortTag(serialized);
	}
}
