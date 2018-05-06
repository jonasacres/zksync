package com.acrescrypto.zksync.fs.zkfs;

import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.InvalidSignatureException;

/** Store a reftag in a means that is extremely difficult to understand without knowledge of the archive root key.
 * The security guarantees for this secrecy are weaker than those for actual archive contents. This is because
 * this level of privacy can be obtained without adding to the storage size of these records, while strengthening it to
 * the degree used elsewhere would require additional storage and introduce new security considerations, complicating the
 * logic. 
 * 
 * To be clear, because I know reviewers will balk: the reftag is encrypted in ECB mode. This is because having a
 * deterministic output makes it possible for seed peers to de-duplicate reftags without understanding what they are.
 * Of course, this means matched blocks can be correlated. This is extremely unlikely to happen in the earlier blocks,
 * which are hash output; it's reasonably likely to happen in the final block which is metadata. At the time of this
 * writing, that metadata could be used by a peer to see when the archive inode table grows enough to need a new page.
 * 
 * So we XOR it with the HMAC of the first block of the hash, signed with the archive key, prior to encryption.
 * 
 * The upshot of this that we have a safer way to store revision tags. I would not call it wholly secure, but it is
 * impossible to guarantee the security of these. Seed peers will almost certainly be able to figure out which pages
 * belong to inode tables, because those will be the first pages everyone asks for.
 * 
 * I explained this idea to a few people, and the reactions were negative. "If the data's important, secure it with an
 * IV! But if it's not, why encrypt it at all?" Well, I don't know what value the attackers get from having it, and I'm
 * not sure I can actually keep them from getting it. But while it's hard to get, there's nothing forcing me to disclose
 * the reftags to make zksync work, and I'm in no way benefited from non-keyholders knowing it.
 */

public class ObfuscatedRefTag {
	protected ZKArchive archive;
	protected byte[] ciphertext;
	protected byte[] signature;
	
	public static int sizeForArchive(ZKArchive archive) {
		return archive.refTagSize() + archive.crypto.signatureSize();
	}
	
	public ObfuscatedRefTag(ZKArchive archive, byte[] serialized) {
		this.archive = archive;
		deserialize(serialized);
	}
	
	@SuppressWarnings("deprecation")
	public ObfuscatedRefTag(RefTag refTag) {
		this.archive = refTag.archive;
		Key key = archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_REFTAG);
		this.ciphertext = archive.crypto.encryptUnsafeDangerousWarningECBMode(key.getRaw(), refTag.getBytes());
		this.signature = archive.crypto.sign(archive.config.privKey.getBytes(), ciphertext);
	}
	
	public int ciphertextLength() {
		return (int) Math.ceil((double) archive.refTagSize() / archive.crypto.symKeyLength()) * archive.crypto.symKeyLength();
	}
	
	public int totalLength() {
		return ciphertextLength() + signature.length;
	}
	
	public boolean verify() {
		return archive.crypto.verify(archive.config.pubKey.getBytes(), this.ciphertext, this.signature);
	}
	
	public void assertValid() throws InvalidSignatureException {
		if(!verify()) throw new InvalidSignatureException();
	}
	
	@SuppressWarnings("deprecation")
	public RefTag decrypt() throws InvalidSignatureException {
		Key key = archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_REFTAG);
		byte[] mangled = archive.crypto.decryptUnsafeDangerousWarningECBMode(key.getRaw(), this.ciphertext);
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
}
