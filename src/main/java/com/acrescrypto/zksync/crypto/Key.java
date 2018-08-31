package com.acrescrypto.zksync.crypto;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.utility.Util;

public class Key {
	private byte[] raw;
	protected CryptoSupport crypto;
	
	public Key(CryptoSupport crypto) {
		this.crypto = crypto;
		this.raw = crypto.makeSymmetricKey();
	}
	
	public Key(CryptoSupport crypto, byte[] raw) {
		this.crypto = crypto;
		this.raw = raw;
	}
	
	public Key derive(int index, byte[] data) {
		ByteBuffer saltBuf = ByteBuffer.allocate(data.length+4);
		saltBuf.put(data);
		saltBuf.putInt(index);
		return new Key(crypto, crypto.expand(raw, crypto.symKeyLength(), saltBuf.array(), "zksync".getBytes()));
	}
	
	public byte[] encryptCBC(byte[] iv, byte[] plaintext) {
		return crypto.encryptCBC(raw, iv, plaintext, 0, plaintext.length);
	}
	
	public byte[] encryptCBC(byte[] iv, byte[] plaintext, int offset, int length) {
		return crypto.encryptCBC(raw, iv, plaintext, offset, length);
	}
	
	public byte[] decryptCBC(byte[] iv, byte[] ciphertext) {
		return crypto.decryptCBC(raw, iv, ciphertext, 0, ciphertext.length);
	}
	
	public byte[] decryptCBC(byte[] iv, byte[] ciphertext, int offset, int length) {
		return crypto.decryptCBC(raw, iv, ciphertext, offset, length);
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
	
	public byte[] decryptUnpadded(byte[] iv, byte[] ciphertext) {
		return decryptUnpadded(iv, ciphertext, 0, ciphertext.length);
	}
	
	public byte[] decrypt(byte[] iv, byte[] ciphertext, int offset, int length) {
		return crypto.decrypt(raw, iv, ciphertext, offset, length, null, 0, 0, true);
	}
	
	public byte[] decryptUnpadded(byte[] iv, byte[] ciphertext, int offset, int length) {
		return crypto.decrypt(raw, iv, ciphertext, offset, length, null, 0, 0, false);
	}

	public byte[] authenticate(byte[] data) {
		return crypto.authenticate(raw, data);
	}
	
	public byte[] getRaw() {
		return raw;
	}
	
	public void destroy() {
		if(raw != null) {
			Util.blank(raw);
			raw = null;
		}
		
		crypto = null;
	}

	public CryptoSupport getCrypto() {
		return crypto;
	}
	
	public boolean equals(Object other) {
		if(!(other instanceof Key)) return false;
		return Arrays.equals(raw, ((Key) other).raw);
	}
	
	public String toString() {
		return "Key fingerprint=" + Util.bytesToHex(crypto.hash(raw), 4);
	}
}
