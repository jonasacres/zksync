package com.acrescrypto.zksync.crypto;

import java.nio.ByteBuffer;

public class Key {
	private byte[] raw;
	protected CryptoSupport crypto;
	
	public static Key blank(CryptoSupport crypto) {
		return new Key(crypto, new byte[crypto.symKeyLength()]);
	}
	
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
		return new Key(crypto, crypto.expand(raw, raw.length, saltBuf.array(), "zksync".getBytes()));
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
	
	public byte[] encrypt(byte[] iv, byte[] associatedData, byte[] plaintext, int padSize) {
		return crypto.encrypt(raw, iv, plaintext, associatedData, padSize);
	}
	
	public byte[] encrypt(byte[] iv, byte[] associatedData, byte[] plaintext, int offset, int length, int padSize) {
		int adLen = associatedData == null ? 0 : associatedData.length;
		return crypto.encrypt(raw, iv, plaintext, offset, length, associatedData, 0, adLen, padSize);
	}

	public byte[] decrypt(byte[] iv, byte[] ciphertext) {
		return decrypt(iv, ciphertext, 0, ciphertext.length);
	}
	
	public byte[] decrypt(byte[] iv, byte[] associatedData, byte[] ciphertext) {
		return crypto.decrypt(raw, iv, ciphertext, associatedData, true);
	}
	
	public byte[] decryptUnpadded(byte[] iv, byte[] ciphertext) {
		return decryptUnpadded(iv, ciphertext, 0, ciphertext.length);
	}
	
	public byte[] decryptUnpadded(byte[] iv, byte[] associatedData, byte[] ciphertext) {
		return crypto.decrypt(raw, iv, ciphertext, associatedData, false);
	}

	public byte[] decryptUnpadded(byte[] iv, byte[] associatedData, byte[] ciphertext, int offset, int length) {
		int adLen = associatedData == null ? 0 : associatedData.length;
		return crypto.decrypt(raw, iv, ciphertext, offset, length, associatedData, 0, adLen, false);
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
	
	public boolean isBlank() {
		byte c = 0;
		for(byte b : raw) {
			c |= b;
		}
		
		return c == 0;
	}
	
	@Override
	public boolean equals(Object _other) {
		if(!(_other instanceof Key)) return false;
		Key other = (Key) _other;
		int c = raw.length ^ other.raw.length;
		
		for(int i = 0; i < Math.min(raw.length, other.raw.length); i++) {
			 c |= raw[i] ^ other.raw[i];
		}
		
		return c == 0;
	}

	public CryptoSupport getCrypto() {
		return crypto;
	}
	
	public void replace(byte[] newKey) {
		if(raw.length != newKey.length) {
			destroy();
			raw = new byte[newKey.length];
		}
		
		System.arraycopy(newKey, 0, raw, 0, raw.length);
		for(int i = 0; i < newKey.length; i++) {
			newKey[i] = 0;
		}
	}

	public void destroy() {
		for(int i = 0; i < raw.length; i++) {
			raw[i] = 0x00;
		}
		
		raw = null;
	}
}
