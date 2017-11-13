package com.acrescrypto.zksync.crypto;

import java.nio.ByteBuffer;

public class Key {
	private byte[] raw;
	private CryptoSupport crypto;
	
	public static int KEY_INDEX_SALTED_SUBKEY = 255;
	
	public Key(CryptoSupport crypto) {
		this(crypto, crypto.defaultPrng());
	}
	
	public Key(CryptoSupport crypto, PRNG rng) {
		this.crypto = crypto;
		this.raw = rng.getBytes(crypto.symKeyLength());
	}
	
	public Key(CryptoSupport crypto, byte[] raw) {
		this.crypto = crypto;
		this.raw = raw;
	}
	
	public Key derive(int index) {
		return derive(index, new byte[]{});
	}
	
	public Key derive(int index, byte[] data) {
		if(index > KEY_INDEX_SALTED_SUBKEY) throw new IllegalArgumentException("key derivation index seems way too high");
		ByteBuffer saltBuf = ByteBuffer.allocate(data.length+1);
		saltBuf.put(data);
		saltBuf.put((byte) index);
		return new Key(crypto, crypto.expand(raw, crypto.symKeyLength(), saltBuf.array(), "zksync".getBytes()));
	}
	
	public byte[] wrappedEncrypt(byte[] plaintext, int padSize) {
		return wrappedEncrypt(plaintext, padSize, crypto.defaultPrng());
	}
	
	public byte[] wrappedEncrypt(byte[] plaintext, int padSize, PRNG rng) {
		if(padSize < 0) throw new IllegalArgumentException("pad size cannot be negative for wrappedEncrypt");
		byte[] salt = rng.getBytes(raw.length), iv = rng.getBytes(crypto.symIvLength());
		Key subkey = derive(KEY_INDEX_SALTED_SUBKEY, salt);
		byte[] ciphertext = subkey.encrypt(iv, plaintext, padSize);

		ByteBuffer buffer = ByteBuffer.allocate(2*2 + iv.length + salt.length + ciphertext.length);
		buffer.putShort((short) salt.length);
		buffer.put(salt);
		buffer.putShort((short) iv.length);
		buffer.put(iv);
		buffer.put(ciphertext);
		
		return buffer.array();
	}
	
	public byte[] wrappedDecrypt(byte[] ciphertext) {
		ByteBuffer buffer = ByteBuffer.wrap(ciphertext);
		short saltLen = buffer.getShort();
		byte[] salt = new byte[saltLen];
		buffer.get(salt);
		
		short ivLen = buffer.getShort();
		byte[] iv = new byte[ivLen];
		buffer.get(iv);
		
		Key subkey = derive(KEY_INDEX_SALTED_SUBKEY, salt);
		
		byte[] messageCiphertext = new byte[buffer.remaining()];
		buffer.get(messageCiphertext, 0, messageCiphertext.length);
		return subkey.decrypt(iv, messageCiphertext);
	}
	
	public byte[] encrypt(byte[] iv, byte[] plaintext) {
		return crypto.encrypt(raw, iv, plaintext, null, 0);
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
