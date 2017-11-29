package com.acrescrypto.zksync.crypto;

import java.io.IOException;

import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;
import com.acrescrypto.zksync.fs.FS;

public class SecureFile {
	Key key;
	byte[] iv;
	FS fs;
	String path;
	
	public static SecureFile atPath(FS fs, String path, Key key, byte[] ivMaterial, byte[] salt) {
		return new SecureFile(fs, path, key, ivMaterial, salt);
	}
	
	protected SecureFile() {}
	
	public SecureFile(FS fs, String path, Key key, byte[] ivMaterial, byte[] salt) {
		this.fs = fs;
		this.path = path;
		this.key = key;
		this.iv = deriveIv(ivMaterial, salt);
	}
	
	protected byte[] deriveIv(byte[] ivMaterial, byte[] salt) {
		if(salt == null) salt = new byte[0];
		return key.crypto.expand(ivMaterial, key.crypto.symIvLength(), salt, "zksync".getBytes());
	}
	
	public byte[] read() throws IOException {
		try {
			byte[] ciphertext = fs.read(path);
			return key.decrypt(iv, ciphertext);
		} catch (IOException exc) {
			throw new InaccessibleStorageException();
		}
	}
	
	public void write(byte[] plaintext, int padSize) throws IOException {
		try {
			byte[] ciphertext = key.encrypt(iv, plaintext, padSize);
			fs.write(path, ciphertext);
			fs.squash(path);
		} catch(IOException exc) {
			throw new InaccessibleStorageException();
		}
	}
}
