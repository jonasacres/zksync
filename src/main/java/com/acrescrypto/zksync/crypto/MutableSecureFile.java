package com.acrescrypto.zksync.crypto;

import java.io.IOException;

import com.acrescrypto.zksync.fs.FS;

public class MutableSecureFile extends SecureFile {
	public static MutableSecureFile atPath(FS fs, String path, Key key) {
		return new MutableSecureFile(fs, path, key);
	}

	public MutableSecureFile(FS fs, String path, Key key) {
		this.fs = fs;
		this.path = path;
		this.key = key;
	}
	
	public byte[] read() throws IOException {
		/* TODO: IMPORTANT! Encrypt MutableSecureFiles!
		 */
		return fs.read(path);
	}
	
	public void write(byte[] plaintext, int padSize) throws IOException {
		fs.write(path, plaintext);
	}
}
