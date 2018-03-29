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
		return crypto.encrypt(raw, iv, plaintext, null, padSize);
	}
	
	public byte[] decrypt(byte[] iv, byte[] ciphertext) {
		return crypto.decrypt(raw, iv, ciphertext, null, true);
	}
	
	public byte[] authenticate(byte[] data) {
		return crypto.authenticate(raw, data);
	}
	
	public byte[] getRaw() {
		return raw;
	}
}
