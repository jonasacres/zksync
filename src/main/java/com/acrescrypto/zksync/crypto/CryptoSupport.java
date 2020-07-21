package com.acrescrypto.zksync.crypto;

import java.nio.ByteBuffer;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.ChaCha20ParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;


import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.utility.Util;

import org.slf4j.Logger;

import de.mkammerer.argon2.jna.Argon2Library;
import de.mkammerer.argon2.jna.JnaUint32;
import de.mkammerer.argon2.jna.Size_t;

public class CryptoSupport {
	private PRNG defaultPrng;
	private Logger logger = LoggerFactory.getLogger(CryptoSupport.class);

	// TODO Load Test: (WARNING) cheapArgon2 set true for load test!!
	public static boolean cheapArgon2 = true; // set to true for tests, false otherwise

	public final static int ARGON2_TIME_COST = 40; // iterations
	public final static int ARGON2_MEMORY_COST = 1048576;  // KiB
	public final static int ARGON2_PARALLELISM = 16; // threads

	public final static byte[] PASSPHRASE_SALT_READ = "easysafe-argon2-salt-read".getBytes();
	public final static byte[] PASSPHRASE_SALT_WRITE = "easysafe-argon2-salt-write".getBytes();
	public final static byte[] PASSPHRASE_SALT_LOCAL = "easysafe-argon2-salt-local".getBytes();
	
	private int maxSimultaneousArgon2;
	protected int activeSimultaneousArgon2;

	public static CryptoSupport defaultCrypto() {
		return new CryptoSupport();
	}

	private CryptoSupport() {
		defaultPrng = new PRNG();
	}
	
	public byte[] deriveKeyFromPassphrase(byte[] passphrase, byte[] salt) {
		synchronized(this) {
			while(getMaxSimultaneousArgon2() > 0 && activeSimultaneousArgon2 >= getMaxSimultaneousArgon2()) {
				try {
					logger.info("Waiting to derive argon2 key; {} active of max {} simultaneous",
							activeSimultaneousArgon2,
							getMaxSimultaneousArgon2());
					this.wait();
				} catch (InterruptedException e) {}
			}
			
			activeSimultaneousArgon2++;
		}
		
		byte[] key = deriveKeyFromPassphraseNonblocking(passphrase, salt);
		
		synchronized(this) {
			activeSimultaneousArgon2--;
			this.notify();
		}
		
		return key;
	}

	public byte[] deriveKeyFromPassphraseNonblocking(byte[] passphrase, byte[] salt) {
		JnaUint32 iterations = new JnaUint32(cheapArgon2 ? 1 : ARGON2_TIME_COST);
		JnaUint32 memory = new JnaUint32(cheapArgon2 ? 128 : ARGON2_MEMORY_COST);
		JnaUint32 parallelism = new JnaUint32(cheapArgon2 ? 1 : ARGON2_PARALLELISM);
		JnaUint32 hashLen = new JnaUint32(symKeyLength());

		byte[] key = new byte[symKeyLength()];

		logger.info("Starting argon2: {} iterations, {} memory, {} parallelism, {} hashlen",
				iterations,
				memory,
				parallelism,
				hashLen);
		long startTs = System.currentTimeMillis();
		int result = Argon2Library.INSTANCE.argon2i_hash_raw(
				iterations,
				memory, 
				parallelism,
				passphrase,
				new Size_t(passphrase.length),
				salt,
				new Size_t(salt.length),
				key,
				new Size_t(hashLen.intValue())
				);
		
		logger.info("argon2 calculation complete; elasped time = {}ms, result = {} (want 0)",
				System.currentTimeMillis() - startTs,
				result);

		if(result != Argon2Library.ARGON2_OK) {
			String errMsg = Argon2Library.INSTANCE.argon2_error_message(result);
			logger.error("Argon2 failed: {}", errMsg);
			throw new IllegalStateException(String.format("%s (%d)", errMsg, result));
		}

		return key;
	}

	public HashContext startHash() {
		return new HashContext();
	}

	public byte[] hash(byte[] data) {
		return startHash().update(data).finish();
	}

	public byte[] hash(byte[] data, int offset, int length) {
		return startHash().update(data, offset, length).finish();
	}

	public byte[] authenticate(byte[] key, byte[] data) {
		return authenticate(key, data, 0, data.length);
	}

	public byte[] authenticate(byte[] key, byte[] data, int offset, int length) {
		// HMAC, per RFC 2104
		/* We're using HMAC since some standards (e.g. noise) explicitly call for the use of
		 * HMAC in place of an algorithm-specific keyed hash, as supported in BLAKE2b. */
		int blockSize = HashContext.BLOCK_SIZE;

		byte[] ipad = new byte[blockSize], opad = new byte[blockSize];
		for(int i = 0; i < blockSize; i++) {
			ipad[i] = 0x36;
			opad[i] = 0x5c;
		}

		if(key.length > blockSize) {
			key = hash(key);
		}

		if(key.length < blockSize) {
			byte[] newKey = new byte[blockSize];
			System.arraycopy(key, 0, newKey, 0, key.length);
			key = newKey;
		}

		HashContext inner = startHash();
		inner.update(xor(key, ipad));
		inner.update(data);

		HashContext outer = startHash();
		outer.update(xor(key, opad));
		outer.update(inner.finish());
		return outer.finish();
	}
	
	public byte[] expandAndDestroy(byte[] ikm, int length, byte[] salt, byte[] info) {
		byte[] retval = expand(ikm, length, salt, info);
		Util.zero(ikm);
		
		return retval;
	}

	public byte[] expand(byte[] ikm, int length, byte[] salt, byte[] info) {
		// HKDF, per RFC 5869
		byte[] prk = authenticate(salt, ikm);
		ByteBuffer output = ByteBuffer.allocate(length);

		int n = (int) Math.ceil((double) length/hashLength());
		byte[] tp = {};
		for(int i = 1; i <= n; i++) {
			ByteBuffer concatted = ByteBuffer.allocate(tp.length + info.length + 1);
			concatted.put(tp);
			concatted.put(info);
			concatted.put((byte) i);
			tp = authenticate(prk, concatted.array());
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
			if(plaintext == null) plaintext = new byte[0];
			return processAEADCipher(true,
					8*symTagLength(),
					key,
					iv,
					padToSize(plaintext, 0, plaintext.length, padSize),
					associatedData);
		} catch (Exception exc) {
			logger.error("Encountered exception encrypting data", exc);
			throw new RuntimeException(exc);
		}
	}

	public byte[] encrypt(byte[] key, byte[] iv, byte[] plaintext, int offset, int length, byte[] associatedData, int adOffset, int adLen, int padSize)
	{
		try {
			return processAEADCipher(true,
					8*symTagLength(),
					key,
					iv,
					padToSize(plaintext, offset, length, padSize),
					associatedData);
		} catch (Exception exc) {
			logger.error("Encountered exception encrypting data", exc);
			throw new RuntimeException(exc);
		}
	}

	public byte[] decrypt(byte[] key, byte[] iv, byte[] ciphertext, byte[] associatedData, boolean padded)
	{
		return decrypt(key,
				iv,
				ciphertext,
				0,
				ciphertext.length,
				associatedData,
				0,
				associatedData == null ? 0 : associatedData.length,
						padded);
	}

	public byte[] decrypt(byte[] key, byte[] iv, byte[] ciphertext, int offset, int length, byte[] associatedData, int adOffset, int adLen, boolean padded) {
		byte[] paddedPlaintext = processAEADCipher(false,
				128,
				key,
				iv,
				ciphertext,
				offset,
				length,
				associatedData,
				adOffset,
				adLen);
		if(padded) return unpad(paddedPlaintext);
		return paddedPlaintext;
	}

	public byte[] encryptCBC(byte[] key, byte[] iv, byte[] plaintext) {
		return encryptUnauthenticated(key, iv, plaintext, 0, plaintext.length);
	}

	public byte[] encryptUnauthenticated(byte[] key, byte[] iv, byte[] plaintext, int offset, int length) {
		return processOrdinaryCipher(true, key, iv, plaintext, offset, length);
	}

	public byte[] decryptCBC(byte[] key, byte[] iv, byte[] ciphertext) {
		return decryptCBC(key, iv, ciphertext, 0, ciphertext.length);
	}

	public byte[] decryptCBC(byte[] key, byte[] iv, byte[] ciphertext, int offset, int length) {
		byte[] paddedPlaintext = processOrdinaryCipher(false,
				key,
				iv,
				ciphertext,
				offset,
				length);
		return paddedPlaintext;
	}

	public static byte[] xor(byte[] a, byte[] b) {
		int len = Math.min(a.length, b.length);
		byte[] r = new byte[len];

		for(int i = 0; i < len; i++) r[i] = (byte) (a[i] ^ b[i]);
		return r;
	}

	protected static byte[] processAEADCipher(boolean encrypt, int tagLen, byte[] keyBytes, byte[] iv, byte[] in, byte[] ad) throws IllegalStateException {
		return processAEADCipher(encrypt,
				tagLen,
				keyBytes,
				iv,
				in,
				0,
				in == null ? 0 : in.length,
						ad,
						0,
						ad == null ? 0 : ad.length);
	}

	protected static byte[] processAEADCipher(boolean encrypt, int tagLen, byte[] keyBytes, byte[] nonce, byte[] in, int inOffset, int inLen, byte[] ad, int adOffset, int adLen) throws IllegalStateException {
		try {
			if(nonce.length < 12) {
				byte[] newNonce = new byte[12];
				System.arraycopy(nonce, 0, newNonce, 0, nonce.length);
				nonce = newNonce;
			}

			Cipher cipher = Cipher.getInstance("ChaCha20-Poly1305");
			IvParameterSpec ivSpec = new IvParameterSpec(nonce);
			SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "ChaCha20");
			cipher.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, keySpec, ivSpec);
			byte[] out = new byte[cipher.getOutputSize(inLen)];
			int offset = 0;
			if(ad != null) cipher.updateAAD(ad, adOffset, adLen);
			if(in != null) offset += cipher.update(in, inOffset, inLen, out, 0);
			cipher.doFinal(out, offset);
			return out;
		} catch(BadPaddingException exc) {
			throw new SecurityException(exc);
		} catch (ShortBufferException|InvalidKeyException|NoSuchAlgorithmException|NoSuchPaddingException|IllegalBlockSizeException|InvalidAlgorithmParameterException exc) {
			exc.printStackTrace();
			throw new RuntimeException(exc);
		}
	}

	protected static byte[] processOrdinaryCipher(boolean encrypt, byte[] keyBytes, byte[] nonce, byte[] in, int offset, int length) throws IllegalStateException {
		try {
			if(nonce.length < 12) {
				byte[] newNonce = new byte[12];
				System.arraycopy(nonce, 0, newNonce, 0, nonce.length);
				nonce = newNonce;
			}

			Cipher cipher = Cipher.getInstance("ChaCha20");
			ChaCha20ParameterSpec ivSpec = new ChaCha20ParameterSpec(nonce, 1);
			SecretKeySpec keySpec = new SecretKeySpec(keyBytes, "ChaCha20");
			cipher.init(encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, keySpec, ivSpec);
			byte[] out = new byte[cipher.getOutputSize(length)];
			cipher.update(in, offset, length, out, 0);
			cipher.doFinal(out, offset);
			return out;
		} catch(BadPaddingException exc) {
			throw new SecurityException(exc);
		} catch (ShortBufferException|InvalidKeyException|NoSuchAlgorithmException|NoSuchPaddingException|IllegalBlockSizeException|InvalidAlgorithmParameterException exc) {
			exc.printStackTrace();
			throw new RuntimeException(exc);
		}
	}

	public byte[] padToSize(byte[] raw, int offset, int length, int padSize) {
		if(raw == null) return null;
		if(padSize < 0) {
			if(offset == 0 && length == raw.length) return raw;
			return ByteBuffer.allocate(length).put(raw, offset, length).array();
		}
		if(padSize == 0) padSize = length + 4;
		if(length > padSize) {
			throw new IllegalArgumentException("attempted to pad data beyond maximum size (" + raw.length + " > " + padSize + ")");
		}

		ByteBuffer padded = ByteBuffer.allocate(padSize+4);
		padded.putInt(length);
		padded.put(raw, offset, length);
		return padded.array();
	}

	public byte[] unpad(byte[] raw) {
		ByteBuffer buf = ByteBuffer.wrap(raw);
		int length = buf.getInt();
		if(length > buf.remaining() || length < 0) {
			throw new SecurityException("invalid ciphertext");
		}

		byte[] unpadded = new byte[length];
		buf.get(unpadded, 0, length);
		return unpadded;
	}

	public PRNG prng(byte[] seed) {
		return new PRNG(seed);
	}

	public PRNG defaultPrng() {
		return defaultPrng;
	}

	public byte[] rng(int numBytes) {
		return defaultPrng.getBytes(numBytes);
	}

	public byte[] makeSymmetricKey() {
		return rng(symKeyLength());
	}

	public byte[] makeSymmetricKey(byte[] material) {
		return expand(material, symKeyLength(), new byte[0], new byte[0]);
	}

	public byte[] makeSymmetricIv() {
		return rng(symIvLength());
	}

	public PrivateSigningKey makePrivateSigningKey() {
		return new PrivateSigningKey(this, rng(asymPrivateSigningKeySize()));
	}

	public PrivateSigningKey makePrivateSigningKey(byte[] privateKeyMaterial) {
		if(privateKeyMaterial.length != asymPrivateSigningKeySize()) {
			privateKeyMaterial = this.expand(privateKeyMaterial, asymPrivateSigningKeySize(), "zksync-salt".getBytes(), new byte[0]);
		}

		return new PrivateSigningKey(this, privateKeyMaterial);
	}

	public PublicSigningKey makePublicSigningKey(byte[] publicKeyMaterial) {
		return new PublicSigningKey(this, publicKeyMaterial);
	}

	public PrivateDHKey makePrivateDHKey() {
		return new PrivateDHKey(this);
	}

	public PrivateDHKey makePrivateDHKeyPair(byte[] privKey, byte[] pubKey) {
		return new PrivateDHKey(this, privKey, pubKey);
	}

	public PublicDHKey makePublicDHKey(byte[] publicKeyMaterial) {
		return new PublicDHKey(this, publicKeyMaterial);
	}

	public int symKeyLength() {
		return 256/8; // 256-bit symmetric keys 
	}

	public int symIvLength() {
		return 96/8;
	}

	public int symTagLength() {
		return 128/8;
	}

	public int symBlockSize() {
		return 128/8; // AES has 128-bit blocks
	}

	public int hashLength() {
		return HashContext.HASH_SIZE;
	}

	public int asymPrivateSigningKeySize() {
		return 256/8;
	}

	public int asymPublicSigningKeySize() {
		return 256/8;
	}

	public int asymSignatureSize() {
		return 512/8;
	}

	public int asymPrivateDHKeySize() {
		return PublicDHKey.KEY_SIZE;
	}

	public int asymPublicDHKeySize() {
		return PublicDHKey.KEY_SIZE;
	}

	public int asymDHSecretSize() {
		return PrivateDHKey.RAW_SECRET_SIZE;
	}

	public int symPadToReachSize(int paddedLen) {
		return paddedLen - 4 - symTagLength();
	}

	public int symPaddedCiphertextSize(int textLen) {
		return 4 + textLen + symTagLength();
	}

	public byte[] symNonce(long i) {
		return ByteBuffer.allocate(symIvLength()).putLong(i).array();
	}

	public int getMaxSimultaneousArgon2() {
		return maxSimultaneousArgon2;
	}

	public void setMaxSimultaneousArgon2(int maxSimultaneousArgon2) {
		this.maxSimultaneousArgon2 = maxSimultaneousArgon2;
	}
}
