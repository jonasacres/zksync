package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.CantUnpackRevisionTagException;
import com.acrescrypto.zksync.exceptions.InvalidRevisionTagException;
import com.acrescrypto.zksync.utility.HashCache;
import com.acrescrypto.zksync.utility.Util;

public class RevisionTag implements Comparable<RevisionTag> {
	protected static HashCache<RevisionTag,Boolean> verificationCache =
			new HashCache<>(128,
					(tag)->tag.isValidUncached(),
					(tag, isValid)->{});
	
	private RefTag  refTag;
	private boolean isMerge;
	private long    height                 = -1;
	private long    parentHash             = -1;
	private long    hasStructureCheckTime;
	
	private byte[]          serialized;
	private int             hashCode;
	private RevisionInfo    info;
	private boolean         cacheOnly;
	private ZKArchiveConfig config;

	public static int sizeForConfig(ZKArchiveConfig config) {
		return config.refTagSize()
			 + 8
			 + 8
			 + config.getCrypto().asymSignatureSize();
	}
	
	public static RevisionTag blank(ZKArchiveConfig config) {
		return new RevisionTag(RefTag.blank(config), 0, 0, false);
	}
	
	public RevisionTag(RefTag refTag, long parentHash, long height, boolean isMerge) {
		// the reftag needs to already have a final storage tag for us to make a revtag
		assert(refTag.getStorageTag().isFinalized());
		
		this.refTag     = refTag;
		this.parentHash = parentHash;
		this.height     = height;
		this.config     = refTag.config;
		this.isMerge    = isMerge;
		serialize();
	}
	
	public RevisionTag(ZKArchiveConfig config, byte[] serialization, boolean verifySignature) {
		this(config, ByteBuffer.wrap(serialization), verifySignature);
	}
	
	public RevisionTag(ZKArchiveConfig config, ByteBuffer buf, boolean verifySignature) {
		this.config = config;
		deserialize(buf, verifySignature);
	}

	public RevisionTag(RevisionTag existing, ZKArchive archive) throws IOException {
		this.refTag     = new RefTag(
				existing.refTag.getArchive().cacheOnlyArchive(),
				existing.refTag.getBytes());
		this.serialized = existing.serialized;
		this.height     = existing.height;
		this.parentHash = existing.parentHash;
		this.isMerge    = existing.isMerge;
		this.config     = archive.getConfig();
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
		ensureUnpacked();
		return refTag;
	}
	
	public byte[] getBytes() {
		return serialized;
	}
	
	public long getHeight() {
		try {
			ensureUnpacked();
		} catch(CantUnpackRevisionTagException exc) {
			return -1;
		}
		
		return height;
	}
	
	public long getParentHash() {
		try {
			ensureUnpacked();
		} catch(CantUnpackRevisionTagException exc) {
			return -1;
		}
		
		return parentHash;
	}
	
	public boolean hasInfoFetched() {
		return info != null;
	}
	
	public RevisionInfo getInfo() throws IOException {
		if(info != null) return info;
		
		try(ZKFS fs = readOnlyFS()) {
			if(info == null) info = fs.getRevisionInfo();
			return info;
		}
	}
	
	public ZKFS readOnlyFS() throws IOException {
		ensureUnpacked();
		if(cacheOnly) {
			return refTag.config.archive.cacheOnlyArchive().openRevisionReadOnly(this);
		} else {
			return refTag.config.archive.openRevisionReadOnly(this);
		}
	}
	
	public RevisionTag makeCacheOnlyCopy() throws IOException {
		if(cacheOnly) return this;
		
		RevisionTag tag = new RevisionTag(this, refTag.getArchive().cacheOnlyArchive());
		tag.cacheOnly = true;
		
		return tag;
	}
	
	public boolean isCacheOnly() {
		return cacheOnly;
	}
	
	public boolean isMerge() {
		return isMerge;
	}
	
	public ZKFS getFS() throws IOException {
		return new ZKFS(this);
	}
	
	public boolean matchesPrefix(String base64Prefix) {
		String unsafedPrefix = Util.fromWebSafeBase64(base64Prefix);
		String myBase64 = Util.encode64(this.getBytes());
		return myBase64.startsWith(unsafedPrefix);
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
		ByteBuffer plaintext = ByteBuffer.allocate(
				sizeForConfig(refTag.config)
			  - refTag.config.getCrypto().asymSignatureSize());
		try {
			plaintext.put(refTag.serialize());
		} catch(IOException exc) {
			// should not be possible since we must have refTag.storageTag.hasBytes()
			exc.printStackTrace();
			throw new RuntimeException(exc);
		}
		
		long heightWithMerge = (height & Long.MAX_VALUE)
				             | (isMerge
				                ? (1L << 63)
				                : 0);
		plaintext.putLong(parentHash);
		plaintext.putLong(heightWithMerge);

		Key key = refTag.config.deriveKey(
				ArchiveAccessor.KEY_ROOT_ARCHIVE,
				"easysafe-reftag-key");
		byte[] ciphertext = key.encryptUnauthenticated(
				new byte[key.getCrypto().symIvLength()],
				plaintext.array());
		
		ByteBuffer signedCiphertext = ByteBuffer.allocate(sizeForConfig(refTag.getConfig()));
		signedCiphertext.put(ciphertext);
		signedCiphertext.put(refTag.config.privKey.sign(signedCiphertext.array(),
				0,
				signedCiphertext.position()));
		
		serialized = signedCiphertext.array();
		hashCode   = ByteBuffer.wrap(serialized).getInt();
		return serialized;
	}
	
	public void deserialize(ByteBuffer buf, boolean verifySignature) {
		byte[] serialized = new byte[sizeForConfig(config)];
		
		if(buf.remaining() < sizeForConfig(config)) {
			throw new InvalidRevisionTagException(Util.bytesToHex(buf.array()));
		}
		
		buf.get(serialized);
		
		this.serialized = serialized;
		this.hashCode   = ByteBuffer.wrap(serialized).getInt();
		
		if(Arrays.equals(new byte[serialized.length], serialized)) {
			this.refTag     = RefTag.blank(config.getArchive());
			this.height     = 0;
			this.parentHash = 0;
			this.isMerge    = false;
			return;
		}
		
		// we can move unpack after verification, but then we don't have height info available in debug logs
		unpack();

		if(verifySignature) {
			assertValid();
		}
	}
	
	/** Returns true if we've got all the pages for the inode table and directories cached
	 * locally. */
	public boolean hasStructureLocally() throws IOException {
		/* Caching this could be dangerous if someone deletes pages underneath us. */
		
		long localCacheRecheckTime = config.getMaster().getGlobalConfig().getLong("fs.settings.revtagHasLocalCacheTimeout");
		if(Util.currentTimeMillis() - hasStructureCheckTime <= localCacheRecheckTime) {
			return true;
		}
		
		PageTree tree = new PageTree(getRefTag());
		if(!tree.exists()) {
			return false;
		}
		
		try(ZKFS fs = config.getArchive().openRevisionReadOnlyOpportunistic(this)) {
			for(Inode inode : fs.getInodeTable().values()) {
				if(inode.isDeleted()) continue;
				if(!inode.getStat().isDirectory()) continue;
				
				PageTree dirTree = new PageTree(inode);
				if(!dirTree.exists()) {
					return false;
				}
			}
		}
		
		hasStructureCheckTime = Util.currentTimeMillis();
		return true;
	}
	
	public boolean waitForStructure(long timeoutMs) throws IOException {
		long deadline;
		if(timeoutMs < 0) {
			deadline = Long.MAX_VALUE;
		} else {
			deadline = System.currentTimeMillis() + timeoutMs;
			if(deadline < 0) deadline = Long.MAX_VALUE;
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
		if(refTag != null)               return;
		if(config.accessor.isSeedOnly()) return;
		if(config.archiveRoot == null)   return;
		
		Key key = config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, "easysafe-reftag-key");
		byte[] rawBytes = key.decryptUnauthenticated(
				new byte[key.getCrypto().symIvLength()],
				serialized,
				0,
				serialized.length - config.getCrypto().asymSignatureSize());
		ByteBuffer raw   = ByteBuffer.wrap(rawBytes);
		byte[] rawRefTag = new byte[config.refTagSize()];
		raw.get(rawRefTag);
		
		parentHash           = raw.getLong();
		long heightWithMerge = raw.getLong();
		refTag               = new RefTag(config, rawRefTag);
		height               = heightWithMerge & Long.MAX_VALUE;
		isMerge              = heightWithMerge < 0;
	}
	
	protected void ensureUnpacked() {
		if(refTag != null) return;
		unpack();
		if(refTag == null) {
			throw new CantUnpackRevisionTagException(this);
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
		return Util.formatRevisionTag(this)
			 + " parentHash="
			 + String.format("%016x", parentHash);
	}
	
	public int hashCode() {
		return hashCode;
	}

	public long getShortHash() {
		return Util.shortTag(serialized);
	}
}
