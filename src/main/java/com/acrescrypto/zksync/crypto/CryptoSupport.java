package com.acrescrypto.zksync.crypto;

import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Base64;

import org.bouncycastle.crypto.CipherParameters;
import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.InvalidCipherTextException;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.modes.AEADBlockCipher;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.modes.OCBBlockCipher;
import org.bouncycastle.crypto.params.AEADParameters;
import org.bouncycastle.crypto.params.KeyParameter;
import org.bouncycastle.crypto.params.ParametersWithIV;

import com.acrescrypto.zksync.utility.Logger;

import de.mkammerer.argon2.Argon2Factory;
import de.mkammerer.argon2.jna.Argon2Library;
import de.mkammerer.argon2.jna.Size_t;
import de.mkammerer.argon2.jna.Uint32_t;
import net.i2p.crypto.eddsa.EdDSAEngine;
import net.i2p.crypto.eddsa.EdDSAPrivateKey;
import net.i2p.crypto.eddsa.EdDSAPublicKey;
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveTable;
import net.i2p.crypto.eddsa.spec.EdDSAParameterSpec;
import net.i2p.crypto.eddsa.spec.EdDSAPrivateKeySpec;
import net.i2p.crypto.eddsa.spec.EdDSAPublicKeySpec;

public class CryptoSupport {
	private PRNG defaultPrng;
	public static boolean cheapArgon2; // set to true for tests, false otherwise
	
	public final static int ARGON2_TIME_COST = 4;
	public final static int ARGON2_MEMORY_COST = 65536;
	public final static int ARGON2_PARALLELISM = 1;
	
	public final static byte[] PASSPHRASE_SALT = "zksync-salt".getBytes();
	
	public CryptoSupport() {
		defaultPrng = new PRNG();
	}
	
	public byte[] deriveKeyFromPassphrase(byte[] passphrase, byte[] salt) {
        final Uint32_t iterations = new Uint32_t(cheapArgon2 ? 1 : ARGON2_TIME_COST);
        final Uint32_t memory = new Uint32_t(cheapArgon2 ? 8 : ARGON2_MEMORY_COST);
        final Uint32_t parallelism = new Uint32_t(ARGON2_PARALLELISM);
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
	
	public byte[] deriveKeyFromPassphrase(byte[] passphrase) {
		return deriveKeyFromPassphrase(passphrase, PASSPHRASE_SALT);
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
			return processAEADCipher(true, 128, key, iv, padToSize(plaintext, padSize), associatedData);
		} catch (Exception exc) {
			Logger.exception(Logger.LOG_FATAL, exc);
			System.exit(1);
			return null; // unreachable, but it makes the compiler happy
		}
	}
	
	public byte[] decrypt(byte[] key, byte[] iv, byte[] ciphertext, byte[] associatedData, boolean padded)
	{
		try {
			byte[] paddedPlaintext = processAEADCipher(false, 128, key, iv, ciphertext, associatedData);
			if(padded) return unpad(paddedPlaintext);
			return paddedPlaintext;
		} catch(InvalidCipherTextException exc) {
			Logger.exception(Logger.LOG_SECURITY, exc);
			throw new SecurityException();
		} catch(Exception exc) {
			Logger.exception(Logger.LOG_FATAL, exc);
			System.exit(1);
			return null; // unreachable, but it makes the compiler happy
		}
	}
	
	public byte[] encryptCBC(byte[] key, byte[] iv, byte[] plaintext, int padSize) {
		try {
			return processOrdinaryCipher(true, 128, key, iv, padToSize(plaintext, padSize));
		} catch (Exception exc) {
			Logger.exception(Logger.LOG_FATAL, exc);
			System.exit(1);
			return null; // unreachable, but it makes the compiler happy
		}
	}
	
	public byte[] decryptCBC(byte[] key, byte[] iv, byte[] ciphertext, boolean padded) {
		try {
			byte[] paddedPlaintext = processOrdinaryCipher(false, 128, key, iv, ciphertext);
			if(padded) return unpad(paddedPlaintext);
			return paddedPlaintext;
		} catch(Exception exc) {
			Logger.exception(Logger.LOG_FATAL, exc);
			System.exit(1);
			return null; // unreachable, but it makes the compiler happy
		}
	}
	
	public byte[] sign(byte[] privKey, byte[] text) {
		try {
	        EdDSAParameterSpec spec = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519);
	        Signature sgr = new EdDSAEngine(MessageDigest.getInstance(spec.getHashAlgorithm()));
	        EdDSAPrivateKeySpec key = new EdDSAPrivateKeySpec(privKey, spec);
	        EdDSAPrivateKey sKey = new EdDSAPrivateKey(key);
	        sgr.initSign(sKey);
	        sgr.update(text);
			return sgr.sign();
		} catch(NoSuchAlgorithmException | InvalidKeyException | SignatureException exc) {
			Logger.exception(Logger.LOG_FATAL, exc);
			System.exit(1);
			return null;
		}
	}
	
	public boolean verify(byte[] pubKeyBytes, byte[] text, byte[] signature) {
		try {
			EdDSAParameterSpec spec = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519);
	        Signature sgr = new EdDSAEngine(MessageDigest.getInstance(spec.getHashAlgorithm()));
	        EdDSAPublicKeySpec pubKey = new EdDSAPublicKeySpec(pubKeyBytes, spec);
            EdDSAPublicKey vKey = new EdDSAPublicKey(pubKey);
            sgr.initVerify(vKey);
            sgr.update(text);
            return sgr.verify(signature);
		} catch(NoSuchAlgorithmException | SignatureException | InvalidKeyException exc) {
			Logger.exception(Logger.LOG_FATAL, exc);
			System.exit(1);
			return false;
		}
	}
	
	public static byte[] xor(byte[] a, byte[] b) {
		int len = Math.min(a.length, b.length);
		byte[] r = new byte[len];
		
		for(int i = 0; i < len; i++) r[i] = (byte) (a[i] ^ b[i]);
		return r;
	}
	
	protected static byte[] processAEADCipher(boolean encrypt, int tagLen, byte[] keyBytes, byte[] iv, byte[] in, byte[] ad) throws IllegalStateException, InvalidCipherTextException {
        KeyParameter key = new KeyParameter(keyBytes);
        AEADParameters params = new AEADParameters(key, tagLen, iv);
        AEADBlockCipher cipher = new OCBBlockCipher(new AESEngine(), new AESEngine());
        cipher.init(encrypt, params);

		int offset = 0;
		byte[] out = new byte[cipher.getOutputSize(in != null ? in.length : 0)];
		if(ad != null) cipher.processAADBytes(ad, 0, ad.length);
        if(in != null) offset = cipher.processBytes(in, 0, in.length, out, 0);
        offset += cipher.doFinal(out, offset);
        assert(offset == out.length);
        
        return out;
	}
	
	protected static byte[] processOrdinaryCipher(boolean encrypt, int tagLen, byte[] keyBytes, byte[] iv, byte[] in) throws IllegalStateException {
        KeyParameter key = new KeyParameter(keyBytes);
        CipherParameters params = new ParametersWithIV(key, iv);
        CBCBlockCipher cipher = new CBCBlockCipher(new AESEngine());
        cipher.init(encrypt, params);

		int offset = 0;
		byte[] out = new byte[in.length];
        if(in != null) offset = cipher.processBlock(in, 0, out, 0);
        assert(offset == out.length);
        
        return out;
	}
	
	private byte[] padToSize(byte[] raw, int padSize) {
		if(raw == null) return null;
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
	
	public PRNG prng(byte[] seed) {
		return new PRNG(seed);
	}
	
	public PRNG defaultPrng() {
		return defaultPrng;
	}
	
	public byte[] rng(int numBytes) {
		return defaultPrng.getBytes(numBytes);
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

	public int symBlockSize() {
		return symIvLength();
	}

	public int signatureSize() {
		// TODO P2P: (implement) fixed signature size
		return 0;
	}
}
