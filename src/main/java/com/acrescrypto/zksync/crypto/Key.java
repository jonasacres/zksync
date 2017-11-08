package com.acrescrypto.zksync.crypto;

import java.nio.ByteBuffer;

public class Key {
	private byte[] raw;
	private CryptoSupport crypto;
	
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
		if(index > 255) throw new IllegalArgumentException("key derivation index seems way too high");
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
		/* TODO: this means we're trusting not only our RNG, but everyone else's RNG. what if RNG output is combined
		 * with existing key material?
		 */
		Key subkey = new Key(crypto, rng);
		byte[] outerIv = rng.getBytes(crypto.symIvLength()), innerIv = rng.getBytes(crypto.symIvLength());
		byte[] ciphertext = subkey.encrypt(innerIv, plaintext, padSize);
		byte[] keywrap = encrypt(outerIv, subkey.raw, 0);
		ByteBuffer buffer = ByteBuffer.allocate(outerIv.length + innerIv.length + ciphertext.length + keywrap.length + 2*2);
		buffer.putShort((short) outerIv.length);
		buffer.put(outerIv);
		buffer.put(innerIv);
		buffer.putShort((short) keywrap.length);
		buffer.put(keywrap);
		buffer.put(ciphertext);
		
		return buffer.array();
	}
	
	public byte[] wrappedDecrypt(byte[] ciphertext) {
		ByteBuffer buffer = ByteBuffer.wrap(ciphertext);
		short ivLen = buffer.getShort();
		byte[] outerIv = new byte[ivLen], innerIv = new byte[ivLen];
		buffer.get(outerIv, 0, ivLen);
		buffer.get(innerIv, 0, ivLen);
		short keywrapLen = buffer.getShort();
		byte[] subkeyCiphertext = new byte[keywrapLen];
		buffer.get(subkeyCiphertext, 0, keywrapLen);
		
		Key subkey = new Key(crypto, decrypt(outerIv, subkeyCiphertext));
		
		byte[] messageCiphertext = new byte[buffer.remaining()];
		buffer.get(messageCiphertext, 0, messageCiphertext.length);
		return subkey.decrypt(innerIv, messageCiphertext);
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
