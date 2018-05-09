package com.acrescrypto.zksync.crypto;

import java.nio.ByteBuffer;

public class Key {
	private byte[] raw;
	protected CryptoSupport crypto;
	
	public Key(CryptoSupport crypto, byte[] raw) {
		this.crypto = crypto;
		this.raw = raw;
	}
	
	public Key derive(int index, byte[] data) {
		ByteBuffer saltBuf = ByteBuffer.allocate(data.length+1);
		saltBuf.put(data);
		saltBuf.put((byte) index);
		return new Key(crypto, crypto.expand(raw, crypto.symKeyLength(), saltBuf.array(), "zksync".getBytes()));
	}
	
	public byte[] encrypt(byte[] iv, byte[] plaintext, int padSize) {
		return encrypt(iv, plaintext, 0, plaintext.length, padSize);
	}
	
	public byte[] encrypt(byte[] iv, byte[] plaintext, int offset, int length, int padSize) {
		return crypto.encrypt(raw, iv, plaintext, offset, length, null, 0, 0, padSize);
	}
	
	public byte[] decrypt(byte[] iv, byte[] ciphertext) {
		return decrypt(iv, ciphertext, 0, ciphertext.length);
	}
	
	public byte[] decrypt(byte[] iv, byte[] ciphertext, int offset, int length) {
		return crypto.decrypt(raw, iv, ciphertext, offset, length, null, 0, 0, true);
	}

	public byte[] authenticate(byte[] data) {
		return crypto.authenticate(raw, data);
	}
	
	public byte[] getRaw() {
		return raw;
	}
}
