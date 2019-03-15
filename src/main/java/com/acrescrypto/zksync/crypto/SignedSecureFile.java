package com.acrescrypto.zksync.crypto;

import java.io.IOException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.utility.Util;

public class SignedSecureFile {
	protected FS fs;
	protected PrivateSigningKey privKey;
	protected PublicSigningKey pubKey;
	protected Key textKey, authKey, saltKey;
	protected byte[] tag;
	protected final Logger logger = LoggerFactory.getLogger(SignedSecureFile.class);
	
	public static SignedSecureFile withParams(FS storage, Key textKey, Key saltKey, Key authKey, PrivateSigningKey privKey) {
		return new SignedSecureFile(storage, textKey, saltKey, authKey, privKey);
	}

	public static SignedSecureFile withTag(byte[] pageTag, FS storage, Key textKey, Key saltKey, Key authKey, PublicSigningKey pubKey) {
		return new SignedSecureFile(pageTag, storage, textKey, saltKey, authKey, pubKey);
	}
	
	public static int fileSize(CryptoSupport crypto, int padSize) {
		return crypto.hashLength() + crypto.asymSignatureSize() + 4 + padSize;
	}
	
	public SignedSecureFile(FS fs, Key textKey, Key saltKey, Key authKey, PrivateSigningKey privKey) {
		this.fs = fs;
		this.textKey = textKey;
		this.saltKey = saltKey;
		this.authKey = authKey;
		this.privKey = privKey;
		this.pubKey = privKey.publicKey();
	}
	
	public SignedSecureFile(byte[] tag, FS fs, Key textKey, Key saltKey, Key authKey, PublicSigningKey pubKey) {
		this.tag = tag;
		this.fs = fs;
		this.textKey = textKey;
		this.saltKey = saltKey;
		this.authKey = authKey;
		this.pubKey = pubKey;
	}
	
	public byte[] read(boolean verify) throws IOException {
		try {
			CryptoSupport crypto = textKey.getCrypto();

			byte[] contents = fs.read(path());
			if(!Arrays.equals(authKey.authenticate(contents), tag)) {
				logger.error("SignedSecureFile {}: Unable to authenticate, |contents|={}, H(contents)={}, H(expected)={}",
						path(),
						contents.length,
						Util.formatLongId(contents),
						Util.formatLongId(tag));
				throw new SecurityException();
			}
			
			int sigOffset = contents.length - pubKey.crypto.asymSignatureSize();
			if(sigOffset < 0) {
				throw new SecurityException();
			}
			
			if(verify) {
				if(!pubKey.verify(contents, 0, sigOffset, contents, sigOffset, pubKey.crypto.asymSignatureSize())) {
					throw new SecurityException();
				}
			}
			
			byte[] salt = new byte[crypto.hashLength()];
			System.arraycopy(contents, 0, salt, 0, salt.length);
			
			Key derivedKey = textKey.derive("easysafe-file-encryption", salt);
			byte[] paddedPlaintext = derivedKey.decryptUnauthenticated(fixedIV(),
					contents,
					salt.length,
					sigOffset - salt.length);
			return crypto.unpad(paddedPlaintext);
		} catch (Exception exc) {
			long size = -1;
			try {
				size = fs.stat(path()).getSize();
			} catch(Exception exc2) {
				logger.warn("Encountered exception statting file {}",
						path(),
						exc2);
			}
			logger.warn("Encountered exception loading file {}, size {}, fs type {}",
					path(),
					size,
					fs.getClass().getSimpleName(),
					exc);
			if(exc instanceof IOException) {
				throw new InaccessibleStorageException();
			} else {
				throw exc;
			}
		}
	}
	
	public byte[] read() throws IOException {
		return read(true);
	}
	
	public byte[] write(byte[] plaintext, int padSize) throws IOException {
		assert(privKey != null);
		try {
			// TODO Someday: (refactor) there are a lot of unnecessary buffer copies here...
			
			byte[] paddedPlaintext = textKey.crypto.padToSize(plaintext, 0, plaintext.length, padSize);
			byte[] salt = saltKey.authenticate(plaintext);
			Key encKey = textKey.derive("easysafe-file-encryption", salt);
			byte[] ciphertext = encKey.encryptUnauthenticated(fixedIV(), paddedPlaintext);
			byte[] result = new byte[salt.length + ciphertext.length + privKey.crypto.asymSignatureSize()];
			
			System.arraycopy(salt, 0, result, 0, salt.length);
			System.arraycopy(ciphertext, 0, result, salt.length, ciphertext.length);
			byte[] signature = privKey.sign(result, 0, salt.length + ciphertext.length);
			System.arraycopy(signature, 0, result, salt.length + ciphertext.length, signature.length);
			
			tag = authKey.authenticate(result);
			try {
				Stat stat = fs.stat(path());
				// SwarmFS will always say the file exists at the appropriate size, but it always shows mtime 0, and inode id equal to the short tag
				// If we have the SwarmFS answer, we don't 'really' have this file and we have to write. Else, no need to write the same data twice.
				if(stat.getMtime() != 0 || stat.getInodeId() != Util.shortTag(tag)) {
					return tag;
				}
			} catch(ENOENTException exc) {}
			
			fs.write(path(), result);
			fs.squash(path());
			return tag;
		} catch(IOException exc) {
			logger.error("FS -: Caught exception writing SignedSecureFile to {}", path(), exc);
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
