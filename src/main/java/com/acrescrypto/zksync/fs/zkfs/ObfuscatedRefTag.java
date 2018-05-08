package com.acrescrypto.zksync.fs.zkfs;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PublicKey;
import com.acrescrypto.zksync.exceptions.InvalidSignatureException;

/** Store a reftag in a means that is extremely difficult to understand without knowledge of the archive root key.
 * The security guarantees for this secrecy are weaker than those for actual archive contents. This is because
 * this level of privacy can be obtained without adding to the storage size of these records.
 * 
 * The reftag is first "mangled" by computing the HMAC of its hash bytes using an archive root-derivative key.
 * The mangled reftag is produced by taking the original hash bytes, and concatenating them with the XOR of the
 * first bytes of the HMAC with the metadata bytes. Thus, the metadata bytes (which are very low-entropy) are XORed
 * with a high-entropy source known only to parties who possess the archive root key.
 * 
 * The resulting mangled hash is encrypted in CBC mode with a fixed IV of null bytes. This will reveal reftags sharing
 * a common prefix of one or more blocks; however, since the prefix is a hash, this is extremely unlikely.
 * 
 * The purpose here is not to make it impossible for an adversary to learn reftags. That's likely to be too impractical,
 * since there are other means by which reftags might be discovered. However, those means are significantly harder than
 * just reading the revision file. The purpose of this technique is to add cost to the adversary.
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
		// TODO P2P: (review) It's been suggested to use a 512-bit block cipher here...
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
	
	public RefTag decrypt() throws InvalidSignatureException {
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
