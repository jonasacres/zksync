package com.acrescrypto.zksync.fs.zkfs;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.InvalidSignatureException;

/** Store a reftag in a means that is extremely difficult to understand without knowledge of the archive root key.
 * The security guarantees for this secrecy are weaker than those for actual archive contents. Hence, it is described
 * as "obfuscated" rather than "encrypted" or "secure."
 * 
 * This methodology will reveal reftags in 2^64 hashes, due to the fact that it uses CBC-mode encryption with a fixed IV.
 * Therefore, reftags with a matched prefix of one or more 128-bit blocks will produce obfuscated reftags with the same
 * number of matched blocks. It will take an average of 2^64 obfuscated reftags for a one-block collision to take place,
 * which is sufficient to reveal the reftag.
 * 
 * Revealing the reftag does not immediately compromise the security of the archive. Secrecy of the reftag is not
 * necessary for secrecy of any part of the archive. However, since concealing as much information as possible is a
 * design goal of this project, some effort is made here to protect the reftags.
 * 
 * Further security could be obtained using a cipher whose block size matches the hash length; e.g. Threefish. This
 * was deemed to add unacceptable complexity to the format, since it would mean requiring support for a second block
 * cipher. Similarly, the reftags could be protected with random IVs; this would require more store space and
 * bandwidth, and also seriously complicate the process of allowing blind peers to de-duplicate reftags.
 */

public class ObfuscatedRefTag implements Comparable<ObfuscatedRefTag> {
	protected ZKArchive archive;
	protected byte[] ciphertext;
	protected byte[] signature;
	
	public static int sizeForArchive(ZKArchive archive) {
		return archive.refTagSize() + archive.crypto.asymSignatureSize();
	}
	
	public ObfuscatedRefTag(ZKArchive archive, byte[] serialized) {
		this.archive = archive;
		deserialize(serialized);
	}
	
	public ObfuscatedRefTag(RefTag refTag) {
		this.archive = refTag.archive;
		Key key = archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_REFTAG);
		this.ciphertext = archive.crypto.encryptCBC(key.getRaw(), new byte[archive.crypto.symBlockSize()], transform(refTag.getBytes()));
		this.signature = archive.config.privKey.sign(ciphertext);
	}
	
	public int ciphertextLength() {
		return (int) Math.ceil((double) archive.refTagSize() / archive.crypto.symKeyLength()) * archive.crypto.symKeyLength();
	}
	
	public int totalLength() {
		return ciphertextLength() + signature.length;
	}
	
	public boolean verify() {
		return archive.config.pubKey.verify(ciphertext, signature);
	}
	
	public void assertValid() throws InvalidSignatureException {
		if(!verify()) throw new InvalidSignatureException();
	}
	
	public RefTag reveal() throws InvalidSignatureException {
		assertValid();
		Key key = archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_REFTAG);
		byte[] mangled = archive.crypto.decryptCBC(key.getRaw(), new byte[archive.crypto.symBlockSize()], this.ciphertext);
		byte[] plaintext = transform(mangled);
		return new RefTag(archive, plaintext);
	}
	
	protected byte[] transform(byte[] tag) {
		byte[] hmac = makeHash(tag);
		ByteBuffer buf = ByteBuffer.allocate(tag.length);
		buf.put(tag, 0, tag.length - RefTag.REFTAG_EXTRA_DATA_SIZE);
		for(int i = 0; i < RefTag.REFTAG_EXTRA_DATA_SIZE; i++) {
			byte b = (byte) (hmac[i] ^ tag[tag.length - RefTag.REFTAG_EXTRA_DATA_SIZE + i]);
			buf.put(b);
		}
		return buf.array();
	}
	
	protected byte[] makeHash(byte[] tag) {
		Key key = archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_REFTAG);
		ByteBuffer buf = ByteBuffer.allocate(archive.refTagSize() - RefTag.REFTAG_EXTRA_DATA_SIZE);
		buf.put(tag, 0, buf.remaining());
		return key.authenticate(buf.array());
	}
	
	protected void deserialize(byte[] serialized) {
		assert(serialized.length == sizeForArchive(archive));
		this.ciphertext = new byte[archive.refTagSize()];
		this.signature = new byte[serialized.length - ciphertext.length];
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		buf.get(ciphertext);
		buf.get(signature);
	}
	
	public byte[] serialize() {
		ByteBuffer buffer = ByteBuffer.allocate(ciphertext.length + signature.length);
		buffer.put(ciphertext);
		buffer.put(signature);
		return buffer.array();
	}
	
	public boolean equals(Object obj) {
		if(!ObfuscatedRefTag.class.isInstance(obj)) return false;
		return Arrays.equals(serialize(), ((ObfuscatedRefTag) obj).serialize());
	}
	
	public int compareTo(ObfuscatedRefTag other) {
		byte[] me = serialize(), them = other.serialize();
		for(int i = 0; i < Math.min(me.length, them.length); i++) {
			if(me[i] == them[i]) continue;
			if(me[i] < them[i]) return -1;
			return 1;
		}
		
		if(me.length < them.length) return -1;
		else if(me.length > them.length) return 1;
		return 0;
	}
}