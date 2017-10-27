package com.acrescrypto.zksync.crypto;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.Blake2bDigest;

import com.acrescrypto.zksync.fs.zkfs.config.PubConfig;

import de.mkammerer.argon2.Argon2Factory;
import de.mkammerer.argon2.jna.Argon2Library;
import de.mkammerer.argon2.jna.Size_t;
import de.mkammerer.argon2.jna.Uint32_t;

public class CryptoSupport {
	private SecureRandom secureRandom = new SecureRandom();
	private PubConfig pubConfig;
	
	public CryptoSupport(PubConfig pubConfig) {
		this.pubConfig = pubConfig;
	}
	
	public byte[] deriveKeyFromPassword(byte[] passphrase, byte[] salt) {
        final Uint32_t iterations = new Uint32_t(pubConfig.getArgon2TimeCost());
        final Uint32_t memory = new Uint32_t(pubConfig.getArgon2MemoryCost());
        final Uint32_t parallelism = new Uint32_t(pubConfig.getArgon2Parallelism());
        final Uint32_t hashLen = new Uint32_t(symKeyLength());

        int len = Argon2Library.INSTANCE.argon2_encodedlen(iterations, memory, parallelism,
                new Uint32_t(salt.length), hashLen, Argon2Factory.Argon2Types.ARGON2i.ordinal).intValue();
        byte[] encoded = new byte[len];
        
        int result = Argon2Library.INSTANCE.argon2i_hash_encoded(
                iterations, memory, parallelism, passphrase, new Size_t(passphrase.length),
                salt, new Size_t(salt.length), new Size_t(hashLen.intValue()), encoded, new Size_t(encoded.length)
        );

        if (result != Argon2Library.ARGON2_OK) {
            String errMsg = Argon2Library.INSTANCE.argon2_error_message(result);
            throw new IllegalStateException(String.format("%s (%d)", errMsg, result));
        }
        
        String[] comps = (new String(encoded)).split("\\$");
        String base64 = comps[5];
        if(base64.charAt(base64.length()-1) == 0x00) base64 = base64.substring(0, base64.length()-1);
        byte[] key = {};
		key = Base64.getDecoder().decode(base64);
        
        return key;
	}
	
	public HashContext startHash() {
		return new HashContext();
	}
	
	public byte[] hash(byte[] data) {
		return startHash().update(data).finish();
	}
	
	public byte[] authenticate(byte[] key, byte[] data) {
		// Key must be no greater than 64 bytes, per requirements of blake2
		Digest digest = new Blake2bDigest(key, hashLength(), null, null);
		byte[] result = new byte[hashLength()];
		digest.update(data, 0, data.length);
		digest.doFinal(result, 0);
		return result;
	}
	
	public byte[] expand(byte[] ikm, int length, byte[] salt, byte[] info) {
		/* 
		 * This is HKDF as described in RFC 5869, with the critical difference that we use BLAKE2's built-in support
		 * for keyed hashes in place of HMAC. Because that feature of BLAKE2 requires keys to be no greater than 64
		 * bytes, we first compute the BLAKE2b hash of the supplied salt to get a key that is guaranteed to be
		 * 64 bytes long.
		 * 
		 * The decision not to use HKDF was based purely on implementation concerns. I was unable to find test
		 * vectors for HKDF based on BLAKE2b, and I was having difficulty replicating results in separate
		 * implementations.
		 *  
		 */
		ByteBuffer output = ByteBuffer.allocate(length);
		byte[] resizedSalt = hash(salt); // critical HKDF difference #1 (hashes salt before use)
		byte[] prk = authenticate(resizedSalt, ikm);
		
		int n = (int) Math.ceil(((double) length)/prk.length);
		byte[] tp = {};
		for(int i = 1; i <= n; i++) {
			ByteBuffer concatted = ByteBuffer.allocate(tp.length + info.length + 1);
			concatted.put(tp);
			concatted.put(info);
			concatted.put((byte) i);
			tp = authenticate(prk, concatted.array()); // critical HKDF difference #2 (uses keyed blake2 instead of hmac)
			output.put(tp, 0, Math.min(tp.length, output.remaining()));
		}
		
		return output.array();
	}
	
	/* Encrypt a message.
	 * @param key Message key to be used. Should be of length symKeyLength().
	 * @param iv Message initialization vector/nonce.
	 * @param plaintext Plaintext to be encrypted
	 * @param associatedData Optional associated data to include in message tag.
	 * @param padSize Fixed-length to pad plaintext to prior to encryption. Set 0 for no padding, or -1 to directly encrypt without adding length field.
	 */
	public byte[] encrypt(byte[] key, byte[] iv, byte[] plaintext, byte[] associatedData, int padSize)
	{
		try {
			Cipher cipher = Cipher.getInstance("AES/OCB/NoPadding", "BC");
			cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES/OCB/NoPadding") , new IvParameterSpec(iv));
			if(associatedData != null) cipher.updateAAD(associatedData, 0, associatedData.length);
			byte[] paddedPlaintext = padToSize(plaintext, padSize),
				   ciphertextWithExcess = new byte[paddedPlaintext.length+32];
			int length = cipher.doFinal(paddedPlaintext, 0, paddedPlaintext.length, ciphertextWithExcess);
			
			ByteBuffer ciphertext = ByteBuffer.allocate(length);
			ciphertext.put(ciphertextWithExcess, 0, length);
			return ciphertext.array();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
			return null; // unreachable, but it makes the compiler happy
		}
	}
	
	public byte[] decrypt(byte[] key, byte[] iv, byte[] ciphertext, byte[] associatedData, boolean padded)
	{
		try {
			Cipher cipher = Cipher.getInstance("AES/OCB/NoPadding", "BC");
			cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES/OCB/NoPadding") , new IvParameterSpec(iv));
			if(associatedData != null) cipher.updateAAD(associatedData);
			byte[] paddedPlaintext = new byte[ciphertext.length];
			int actualLength = cipher.doFinal(ciphertext, 0, ciphertext.length, paddedPlaintext);
			
			if(padded) return unpad(paddedPlaintext);
			ByteBuffer buf = ByteBuffer.allocate(actualLength);
			buf.put(paddedPlaintext, 0, actualLength);
			return buf.array();
		} catch(javax.crypto.AEADBadTagException e) {
			throw new SecurityException();
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
			return null; // unreachable, but it makes the compiler happy
		}
	}
	
	public byte[] xor(byte[] a, byte[] b) {
		int len = Math.min(a.length, b.length);
		byte[] r = new byte[len];
		
		for(int i = 0; i < len; i++) r[i] = (byte) (a[i] ^ b[i]);
		return r;
	}
	
	private byte[] padToSize(byte[] raw, int padSize) {
		if(padSize < 0) return raw.clone();
		if(padSize == 0) padSize = raw.length + 4;
		if(raw.length > padSize) {
			throw new IllegalArgumentException("attempted to pad data beyond maximum size");
		}
		
		ByteBuffer padded = ByteBuffer.allocate(padSize+4);
		padded.putInt(raw.length);
		padded.put(raw);
		while(padded.position() < padSize) padded.put((byte) 0);
		return padded.array();
	}
	
	private byte[] unpad(byte[] raw) {
		ByteBuffer buf = ByteBuffer.wrap(raw);
		int length = buf.getInt();
		if(length > buf.remaining()) throw new SecurityException("invalid ciphertext");
		
		byte[] unpadded = new byte[length];
		buf.get(unpadded, 0, length);
		return unpadded;
	}
	
	public byte[] rng(int numBytes) {
		
		byte[] buf = new byte[numBytes];
		secureRandom.nextBytes(buf);
		return buf;
	}
	
	public int symKeyLength() {
		return 256/8; // 256-bit symmetric keys 
	}
	
	public int symIvLength() {
		return 64/8; // 64-bit IV 
	}
	
	public int hashLength() {
		return 512/8; // 512-bit hashes
	}
}
