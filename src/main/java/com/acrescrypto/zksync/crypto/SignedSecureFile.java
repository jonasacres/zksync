package com.acrescrypto.zksync.crypto;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.utility.Util;

public class SignedSecureFile {
	public final static int SALT_LEN = 16;
	protected FS fs;
	protected PrivateSigningKey privKey;
	protected PublicSigningKey pubKey;
	protected Key textKey, authKey;
	protected byte[] tag;
	protected final Logger logger = LoggerFactory.getLogger(SignedSecureFile.class);
	
	public static SignedSecureFile withParams(FS storage, Key textKey, Key authKey, PrivateSigningKey privKey) {
		return new SignedSecureFile(storage, textKey, authKey, privKey);
	}

	public static SignedSecureFile withTag(byte[] pageTag, FS storage, Key textKey, Key authKey, PublicSigningKey pubKey) {
		return new SignedSecureFile(pageTag, storage, textKey, authKey, pubKey);
	}
	
	public static int fileSize(CryptoSupport crypto, int padSize) {
		return SALT_LEN + crypto.asymSignatureSize() + crypto.symPaddedCiphertextSize(padSize);
	}
	
	public SignedSecureFile(FS fs, Key textKey, Key authKey, PrivateSigningKey privKey) {
		this.fs = fs;
		this.textKey = textKey;
		this.authKey = authKey;
		this.privKey = privKey;
		this.pubKey = privKey.publicKey();
	}
	
	public SignedSecureFile(byte[] tag, FS fs, Key textKey, Key authKey, PublicSigningKey pubKey) {
		this.tag = tag;
		this.fs = fs;
		this.textKey = textKey;
		this.authKey = authKey;
		this.pubKey = pubKey;
	}
	
	public byte[] read(boolean verify) throws IOException {
		try {
			CryptoSupport crypto = textKey.getCrypto();

			byte[] contents = fs.read(path());
			if(!Arrays.equals(authKey.authenticate(contents), tag)) {
				throw new SecurityException();
			}
			
			byte[] salt = new byte[SALT_LEN];
			System.arraycopy(contents, 0, salt, 0, SALT_LEN);
			
			byte[] derivedKeyRaw = crypto.expand(salt, crypto.symKeyLength(), textKey.getRaw(), "easysafe-tagged-file".getBytes());
			Key derivedKey = new Key(crypto, derivedKeyRaw);
			
			int sigOffset = contents.length - pubKey.crypto.asymSignatureSize();
			if(sigOffset < 0) {
				throw new SecurityException();
			}
			
			if(verify) {
				if(!pubKey.verify(contents, 0, sigOffset, contents, sigOffset, pubKey.crypto.asymSignatureSize())) {
					throw new SecurityException();
				}
			}
			
			return derivedKey.decrypt(fixedIV(), contents, SALT_LEN, sigOffset - SALT_LEN);
		} catch (IOException exc) {
			throw new InaccessibleStorageException();
		}
	}
	
	public byte[] read() throws IOException {
		return read(true);
	}
	
	public byte[] write(byte[] plaintext, int padSize) throws IOException {
		assert(privKey != null);
		try {
			// TODO: (refactor) the multiple Util.concat calls here are pretty wasteful...
			CryptoSupport crypto = textKey.getCrypto();
			byte[] salt = crypto.expand(plaintext, SALT_LEN, "".getBytes(), "".getBytes());
			byte[] derivedKeyRaw = crypto.expand(salt, crypto.symKeyLength(), textKey.getRaw(), "easysafe-tagged-file".getBytes());
			Key derivedKey = new Key(crypto, derivedKeyRaw);
			
			byte[] ciphertext = derivedKey.encrypt(fixedIV(), plaintext, padSize);
			byte[] signature = privKey.sign(Util.concat(salt, ciphertext));
			
			byte[] output = Util.concat(salt, ciphertext, signature);
			tag = authKey.authenticate(output);
			
			fs.write(path(), output);
			fs.squash(path());
			return tag;
		} catch(IOException exc) {
			logger.error("Caught exception writing SignedSecureFile to {}", path(), exc);
			throw new InaccessibleStorageException();
		}
	}
	
	protected String path() {
		return Page.pathForTag(tag);
	}
	
	protected byte[] fixedIV() {
		return new byte[textKey.crypto.symIvLength()];
	}
}
