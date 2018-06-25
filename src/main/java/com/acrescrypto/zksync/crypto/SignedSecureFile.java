package com.acrescrypto.zksync.crypto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.Page;

public class SignedSecureFile {
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
		return crypto.asymSignatureSize() + crypto.symPaddedCiphertextSize(padSize);
	}
	
	public static boolean verifySignature(byte[] contents, byte[] associatedData, PublicSigningKey pubKey) {
		int sigOffset = contents.length-pubKey.crypto.asymSignatureSize();
		int signableLen = sigOffset;
		byte[] signable = contents;
		if(associatedData != null) {
			ByteBuffer buf = ByteBuffer.allocate(associatedData.length + sigOffset);
			buf.put(associatedData);
			buf.put(contents, 0, sigOffset);
			signableLen += associatedData.length;
			signable = buf.array();
		}
		return pubKey.verify(signable, 0, signableLen, contents, sigOffset, pubKey.crypto.asymSignatureSize());
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
	
	public byte[] read() throws IOException {
		try {
			byte[] contents = fs.read(path());
			if(!Arrays.equals(authKey.authenticate(contents), tag)) {
				throw new SecurityException();
			}
			
			int sigOffset = contents.length - pubKey.crypto.asymSignatureSize();
			if(sigOffset < 0) {
				throw new SecurityException();
			}
			
			
			if(!pubKey.verify(contents, 0, sigOffset, contents, sigOffset, pubKey.crypto.asymSignatureSize())) {
				throw new SecurityException();
			}
			
			return textKey.decrypt(fixedIV(), contents, 0, sigOffset);
		} catch (IOException exc) {
			throw new InaccessibleStorageException();
		}
	}
	
	public byte[] write(byte[] plaintext, int padSize) throws IOException {
		assert(privKey != null);
		try {
			byte[] ciphertext = textKey.encrypt(fixedIV(), plaintext, padSize);
			byte[] signature = privKey.sign(ciphertext);
			
			ByteBuffer buf = ByteBuffer.allocate(ciphertext.length + signature.length);
			buf.put(ciphertext);
			buf.put(signature);
			tag = authKey.authenticate(buf.array());
			
			fs.write(path(), buf.array());
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
