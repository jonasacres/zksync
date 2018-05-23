package com.acrescrypto.zksync.crypto;

import java.io.IOException;
import java.nio.ByteBuffer;

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
		return readContents(fs.read(path));
	}
	
	public void write(byte[] plaintext, int padSize) throws IOException {
		fs.write(path, makeContents(plaintext, padSize));
	}
	
	protected byte[] readContents(byte[] contents) {
		ByteBuffer buf = ByteBuffer.wrap(contents);
		byte[] randIv = new byte[key.crypto.symIvLength()];
		byte[] randKeyCiphertextRaw = new byte[encryptedKeyLength()];
		byte[] ciphertext = new byte[contents.length - headerLength()];
		
		buf.get(randIv);
		buf.get(randKeyCiphertextRaw);
		buf.get(ciphertext);
		
		byte[] randKeyRaw = key.decrypt(randIv, randKeyCiphertextRaw);
		Key randKey = new Key(key.crypto, randKeyRaw);
		byte[] plaintext = randKey.decrypt(new byte[key.crypto.symIvLength()], ciphertext);
		
		return plaintext;
	}
	
	protected byte[] makeContents(byte[] plaintext, int padSize) {
		Key randKey = new Key(key.crypto, key.crypto.rng(key.crypto.symKeyLength()));
		byte[] randIv = key.crypto.rng(key.crypto.symIvLength());
		byte[] ciphertext = randKey.encrypt(new byte[key.crypto.symIvLength()], plaintext, padSize == 0 ? 0 : padSize-headerLength());
		ByteBuffer output = ByteBuffer.allocate(headerLength() + ciphertext.length);
		output.put(randIv);
		output.put(key.encrypt(randIv, randKey.getRaw(), 0));
		output.put(ciphertext);
		return output.array();
	}
	
	protected int encryptedKeyLength() {
		Key dummyKey = new Key(key.crypto, new byte[key.crypto.symKeyLength()]);
		return dummyKey.encrypt(new byte[key.crypto.symIvLength()], new byte[key.crypto.symKeyLength()], 0).length;
	}
	
	protected int headerLength() {
		return key.crypto.symIvLength() + encryptedKeyLength();
	}
}
