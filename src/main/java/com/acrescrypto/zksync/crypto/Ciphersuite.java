package com.acrescrypto.zksync.crypto;

import java.nio.ByteBuffer;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.bouncycastle.crypto.generators.HKDFBytesGenerator;
import org.bouncycastle.crypto.params.HKDFParameters;

import de.mkammerer.argon2.Argon2Factory;
import de.mkammerer.argon2.jna.Argon2Library;
import de.mkammerer.argon2.jna.Size_t;
import de.mkammerer.argon2.jna.Uint32_t;

public class Ciphersuite {
	private SecureRandom secureRandom = new SecureRandom();
	
	// TODO: configure these from pubconfig
	private int argon2_tcost = 1 << 10;
	private int argon2_mcost = 16;
	private int argon2_parallelism = 2;
	
	public Ciphersuite() {
	}
	
	public byte[] deriveKeyFromPassword(char[] passphrase, byte[] salt) {
        final Uint32_t iterations = new Uint32_t(argon2_tcost);
        final Uint32_t memory = new Uint32_t(argon2_mcost);
        final Uint32_t parallelism = new Uint32_t(argon2_parallelism);
        final Uint32_t hashLen = new Uint32_t(symKeyLength());

        int len = Argon2Library.INSTANCE.argon2_encodedlen(iterations, memory, parallelism,
                new Uint32_t(salt.length), hashLen, Argon2Factory.Argon2Types.ARGON2i.ordinal).intValue();
        byte[] encoded = new byte[len];
        
        int result = Argon2Library.INSTANCE.argon2i_hash_encoded(
                iterations, memory, parallelism, passphrase.toString().getBytes(), new Size_t(passphrase.length),
                salt, new Size_t(salt.length), new Size_t(hashLen.intValue()), encoded, new Size_t(encoded.length)
        );

        if (result != Argon2Library.ARGON2_OK) {
            String errMsg = Argon2Library.INSTANCE.argon2_error_message(result);
            throw new IllegalStateException(String.format("%s (%d)", errMsg, result));
        }
        
        ByteBuffer buf = ByteBuffer.allocate(symKeyLength());
        buf.put(encoded, encoded.length-symKeyLength(), symKeyLength());
        return buf.array();
	}
	
	public HashContext startHash() {
		return new HashContext();
	}
	
	public byte[] hash(byte[] data) {
		return startHash().update(data).finish();
	}
	
	public byte[] authenticate(byte[] key, byte[] data) {
		Digest digest = new Blake2bDigest(key, hashLength(), null, null);
		byte[] result = new byte[hashLength()];
		digest.update(data, 0, data.length);
		digest.doFinal(result, 0);
		return result;
	}
	
	public byte[] hkdf(byte[] ikm, int length, byte[] salt, byte[] info) {
		byte[] output = new byte[length];
		HKDFBytesGenerator hkdf = new HKDFBytesGenerator(new Blake2bDigest());
		hkdf.init(new HKDFParameters(ikm, salt, info));
		hkdf.generateBytes(output, 0, length);
		return output;
	}
	
	public byte[] encrypt(byte[] key, byte[] iv, byte[] plaintext, int padSize)
	{
		try {
			Cipher cipher = Cipher.getInstance("AES/OCB/NoPadding", "BC");
			cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES/OCB/NoPadding") , new IvParameterSpec(iv));
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
	
	public byte[] decrypt(byte[] key, byte[] iv, byte[] ciphertext)
	{
		try {
			Cipher cipher = Cipher.getInstance("AES/OCB/NoPadding", "BC");
			cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES/OCB/NoPadding") , new IvParameterSpec(iv));
			byte[] paddedPlaintext = new byte[ciphertext.length];
			cipher.doFinal(ciphertext, 0, ciphertext.length, paddedPlaintext);
			byte[] plaintext = unpad(paddedPlaintext);
			return plaintext;
		} catch(javax.crypto.AEADBadTagException e) {
			throw new SecurityException();
		} catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
			return null; // unreachable, but it makes the compiler happy
		}
	}
	
	private byte[] padToSize(byte[] raw, int padSize) {
		if(padSize == 0) padSize = raw.length + 4;
		if(raw.length + 4 > padSize) throw new IllegalArgumentException("attempted to pad data beyond maximum size");
		ByteBuffer padded = ByteBuffer.allocate(padSize);
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
