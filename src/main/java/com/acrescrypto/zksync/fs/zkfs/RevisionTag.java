package com.acrescrypto.zksync.fs.zkfs;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.EINVALException;

/* tag structure:
 *  encrypted (XORed with refKey().authenticate(keySalt)
 *    generation (8 bytes)       => monotonic generation number
 *    parentShortTag (8 bytes)   => first 8 bytes of parent hash
 *    authorHash (8 bytes)       => first 8 bytes of refKey().authenticate(authorName)
 *    timestamp (8 bytes)        => revision creation timestamp in epoch ms
 *    flags (2 bytes)            => bitfield; see REV_FLAG_*
 *    TOTAL ENCRYPTED SECTION: 34 bytes
 *  plaintext
 *    keySalt (16 bytes)         => first 16 bytes of rev.authKey().authenticate(ciphertext)
 *    TOTAL PLAINTEXT SECTION: 16 bytes
 *  
 *  TOTAL TAG LENGTH: 50 bytes
 *  
 */

public class RevisionTag {
	protected ZKFS fs;
	protected byte[] tag, keySalt;
	
	protected long generation, parentShortTag, authorHash, timestamp;
	protected short flags;
	
	public final static int GENERATION_SIZE = 8;
	public final static int SHORT_TAG_SIZE = 8;
	public final static int TIMESTAMP_SIZE = 8;
	public final static int AUTHOR_HASH_SIZE = 8;
	public final static int FLAG_SIZE = 2;
	public final static int KEY_SALT_SIZE = 16;
	public final static int REV_TAG_SIZE = GENERATION_SIZE + SHORT_TAG_SIZE + TIMESTAMP_SIZE + AUTHOR_HASH_SIZE + FLAG_SIZE + KEY_SALT_SIZE;
	
	public final static int REV_FLAG_MULTIPLE_PARENTS = 0x01;   // has >1 parent 
	
	public static RevisionTag nullRevision(ZKFS fs) {
		return new RevisionTag(fs);
	}
	
	public RevisionTag(ZKFS fs) {
		this.fs = fs;
		tag = new byte[REV_TAG_SIZE];
		keySalt = new byte[KEY_SALT_SIZE];
	}
	
	public RevisionTag(Revision rev, byte[] ciphertext) {
		this.fs = rev.fs;
		tag = makeTag(rev, ciphertext);
	}
	
	public RevisionTag(ZKFS fs, String path) throws EINVALException {
		this.fs = fs;
		decode(fs.hashFromPath(path));
	}
	
	public RevisionTag(ZKFS fs, byte[] tag) {
		this.fs = fs;
		decode(tag);
	}
	
	public boolean isRoot() {
		return parentShortTag == 0;
	}
	
	public boolean hasMultipleParents() {
		return (flags & RevisionTag.REV_FLAG_MULTIPLE_PARENTS) != 0;
	}
	
	public long getTimestamp() {
		return timestamp;
	}
	
	public long getShortTag() {
		return ByteBuffer.wrap(tag).getLong();
	}
	
	public long getParentShortTag() {
		return parentShortTag;
	}
	
	public byte[] getTag() {
		return tag;
	}
	
	public String getPath() {
		return Paths.get(ZKFS.REVISION_DIR, fs.pathForHash(tag)).toString();
	}
	
	public boolean equals(Object other) {
		return Arrays.equals(tag, ((RevisionTag) other).tag);
	}
	
	public int hashCode() {
		return ByteBuffer.wrap(tag, tag.length-4, 4).getInt();
	}
	
	protected byte[] makeTag(Revision rev, byte[] ciphertext) {
		RevisionTag firstParent = rev.getNumParents() > 0 ? rev.getParentTag(0) : RevisionTag.nullRevision(fs);
		parentShortTag = ByteBuffer.wrap(firstParent.tag).getLong();
		generation = rev.getGeneration();
		authorHash = ByteBuffer.wrap(refKey().authenticate(rev.getSupernode().getStat().getUser().getBytes())).getLong();
		keySalt = rev.getRevKeySalt(ciphertext);
		timestamp = rev.getSupernode().getStat().getMtime()/1000l;
		flags = 0;
		
		if(rev.getNumParents() > 1) {
			flags |= RevisionTag.REV_FLAG_MULTIPLE_PARENTS;
		}
		
		ByteBuffer plaintextBuf = ByteBuffer.allocate(REV_TAG_SIZE-KEY_SALT_SIZE);
		plaintextBuf.putLong(generation);
		plaintextBuf.putLong(parentShortTag);
		plaintextBuf.putLong(authorHash);
		plaintextBuf.putLong(timestamp);
		plaintextBuf.putShort(flags);
		
		ByteBuffer tagBuf = ByteBuffer.allocate(REV_TAG_SIZE);
		byte[] key = refKey().authenticate(keySalt);
		
		tagBuf.put(fs.crypto.xor(plaintextBuf.array(), key));
		tagBuf.put(keySalt);
		
		return tagBuf.array();
	}
	
	protected Key refKey() {
		return fs.deriveKey(ZKFS.KEY_TYPE_AUTH, ZKFS.KEY_INDEX_REVISION_TREE);
	}
	
	protected void decode(byte[] tag) {
		this.tag = tag;
		byte[] ciphertext = new byte[REV_TAG_SIZE-KEY_SALT_SIZE];
		this.keySalt = new byte[KEY_SALT_SIZE];
		
		ByteBuffer buf = ByteBuffer.wrap(tag);
		buf.get(ciphertext, 0, REV_TAG_SIZE-KEY_SALT_SIZE);
		buf.get(keySalt, 0, KEY_SALT_SIZE);
		
		byte[] key = refKey().authenticate(keySalt);
		
		ByteBuffer plaintextBuf = ByteBuffer.wrap(fs.crypto.xor(ciphertext, key));
		this.generation = plaintextBuf.getLong();
		this.parentShortTag = plaintextBuf.getLong();
		this.authorHash = plaintextBuf.getLong();
		this.timestamp = plaintextBuf.getLong();
		this.flags = plaintextBuf.getShort();
	}
}
