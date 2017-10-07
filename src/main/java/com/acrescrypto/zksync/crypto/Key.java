package com.acrescrypto.zksync.crypto;

import java.nio.ByteBuffer;

public class Key {
	byte[] raw;
	Ciphersuite suite;
	
	public Key(Ciphersuite suite) {
		this.suite = suite;
		this.raw = suite.rng(suite.symKeyLength());
	}
	
	public Key(Ciphersuite suite, byte[] raw) {
		this.suite = suite;
		this.raw = raw;
	}
	
	public Key(Ciphersuite suite, char[] passphrase) {
		// TODO: allow configuration of password costs
		this.suite = suite;
		this.raw = suite.deriveKeyFromPassword(passphrase, "zksync-salt".getBytes());
	}
	
	public Key derive(int index) {
		return derive(index, new byte[]{});
	}
	
	public Key derive(int index, byte[] data) {
		if(index > 255) throw new IllegalArgumentException("key derivation index seems way too high");
		ByteBuffer saltBuf = ByteBuffer.allocate(data.length+1);
		saltBuf.put(data);
		saltBuf.put((byte) index);
		return new Key(suite, suite.hkdf(raw, suite.symKeyLength(), saltBuf.array(), "zksync".getBytes()));
	}
	
	public byte[] wrappedEncrypt(byte[] plaintext, int padSize) {
		Key subkey = new Key(suite);
		byte[] outerIv = suite.rng(suite.symIvLength()), innerIv = suite.rng(suite.symIvLength());
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
		
		Key subkey = new Key(suite, decrypt(outerIv, subkeyCiphertext));
		
		byte[] messageCiphertext = new byte[buffer.remaining()];
		buffer.get(messageCiphertext, 0, messageCiphertext.length);
		return subkey.decrypt(innerIv, messageCiphertext);
	}
	
	public byte[] encrypt(byte[] iv, byte[] plaintext) {
		return suite.encrypt(raw, iv, plaintext, 0);
	}
	
	public byte[] encrypt(byte[] iv, byte[] plaintext, int padSize) {
		return suite.encrypt(raw, iv, plaintext, padSize);
	}
	
	public byte[] decrypt(byte[] iv, byte[] ciphertext) {
		return suite.decrypt(raw, iv, ciphertext);
	}
	
	public byte[] authenticate(byte[] data) {
		return suite.authenticate(raw, data);
	}
	
	public byte[] getRaw() {
		return raw;
	}
}
