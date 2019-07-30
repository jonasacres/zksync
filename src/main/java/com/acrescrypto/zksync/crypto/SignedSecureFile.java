package com.acrescrypto.zksync.crypto;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.StorageTag;

public class SignedSecureFile {
	protected FS fs;
	protected PrivateSigningKey privKey;
	protected PublicSigningKey pubKey;
	protected Key textKey, authKey, saltKey;
	protected StorageTag tag;
	protected final Logger logger = LoggerFactory.getLogger(SignedSecureFile.class);
	
	public static SignedSecureFile withParams(FS storage, Key textKey, Key saltKey, Key authKey, PrivateSigningKey privKey) {
		return new SignedSecureFile(storage, textKey, saltKey, authKey, privKey);
	}

	public static SignedSecureFile withTag(StorageTag pageTag, FS storage, Key textKey, Key saltKey, Key authKey, PublicSigningKey pubKey) {
		return new SignedSecureFile(pageTag, storage, textKey, saltKey, authKey, pubKey);
	}
	
	public static int fileSize(CryptoSupport crypto, int padSize) {
		return crypto.hashLength() + crypto.symPaddedCiphertextSize(padSize) + crypto.asymSignatureSize();
	}
	
	public SignedSecureFile(FS fs, Key textKey, Key saltKey, Key authKey, PrivateSigningKey privKey) {
		this.fs = fs;
		this.textKey = textKey;
		this.saltKey = saltKey;
		this.authKey = authKey;
		this.privKey = privKey;
		this.pubKey = privKey.publicKey();
	}
	
	public SignedSecureFile(StorageTag tag, FS fs, Key textKey, Key saltKey, Key authKey, PublicSigningKey pubKey) {
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
			StorageTag actualTag = new StorageTag(authKey, contents);
			if(!tag.equals(actualTag)) {
				logger.warn("SignedSecureFile {}: Unable to authenticate, |contents|={}, tag(contents)={}, tag(expected)={}",
						path(),
						contents.length,
						actualTag,
						tag);
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
			byte[] plaintext = derivedKey.decrypt(fixedIV(),
					contents,
					salt.length,
					sigOffset - salt.length);
			return plaintext;
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
				throw new InaccessibleStorageException(exc);
			} else {
				throw exc;
			}
		}
	}
	
	public byte[] read() throws IOException {
		return read(true);
	}
	
	public StorageTag write(byte[] plaintext, int padSize) throws IOException {
		assert(privKey != null);
		try {
			// TODO Someday: (refactor) there are a lot of unnecessary buffer copies here...
			
			byte[] salt = saltKey.authenticate(plaintext);
			Key encKey = textKey.derive("easysafe-file-encryption", salt);
			byte[] ciphertext = encKey.encrypt(fixedIV(), plaintext, padSize);
			byte[] result = new byte[salt.length + ciphertext.length + privKey.crypto.asymSignatureSize()];
			
			System.arraycopy(salt, 0, result, 0, salt.length);
			System.arraycopy(ciphertext, 0, result, salt.length, ciphertext.length);
			byte[] signature = privKey.sign(result, 0, salt.length + ciphertext.length);
			System.arraycopy(signature, 0, result, salt.length + ciphertext.length, signature.length);
			
			tag = new StorageTag(authKey.getCrypto(), authKey.authenticate(result));
			try {
				Stat stat = fs.stat(path());
				// SwarmFS will always say the file exists at the appropriate size, but it always shows mtime 0, and inode id equal to the short tag
				// If we have the SwarmFS answer, we don't 'really' have this file and we have to write. Else, no need to write the same data twice.
				if(stat.getMtime() != 0 || stat.getInodeId() != tag.shortTag()) {
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
		return tag.path();
	}
	
	protected byte[] fixedIV() {
		return new byte[textKey.crypto.symIvLength()];
	}
}
