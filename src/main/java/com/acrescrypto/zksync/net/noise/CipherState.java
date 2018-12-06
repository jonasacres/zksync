package com.acrescrypto.zksync.net.noise;


import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.utility.Util;

// per section 5.1, Noise specification, http://noiseprotocol.org/noise.html, rev 34, 2018-07-11
public class CipherState {
	private Key key;         // 'k' in specification
	private long nonce;      // 'n' in specification
	private long rekeyInterval; // automatically rekey after this many messages
	
	public CipherState() {
	}
	
	public void initializeKey(Key key) {
		if(key != null) {
			key.destroy();
		}
		
		this.key = key;
		nonce = 0;
	}
	
	public boolean hasKey() {
		return key != null;
	}
	
	public void setNonce(long nonce) {
		this.nonce = nonce;
	}
	
	public CryptoSupport getCrypto() {
		if(key == null) return null;
		return key.getCrypto();
	}
	
	public byte[] encryptWithAssociatedData(byte[] associatedData, byte[] plaintext) {
		return encryptWithAssociatedData(associatedData, plaintext, 0, plaintext.length);
	}
	
	public byte[] encryptWithAssociatedData(byte[] associatedData, byte[] plaintext, int offset, int length) {
		if(key == null) return plaintext;
		if(nonce > 0 && nonce % rekeyInterval == 0) rekey();
		return key.encrypt(
				Util.serializeLong(nonce++),
				associatedData,
				plaintext,
				offset,
				length,
				-1);
	}

	public byte[] decryptWithAssociatedData(byte[] associatedData, byte[] ciphertext) {
		return decryptWithAssociatedData(associatedData, ciphertext, 0, ciphertext.length);
	}

	public byte[] decryptWithAssociatedData(byte[] associatedData, byte[] ciphertext, int offset, int length) {
		if(key == null) return ciphertext;
		if(nonce > 0 && nonce % rekeyInterval == 0) rekey();
		byte[] plaintext = key.decryptUnpadded(
				Util.serializeLong(nonce),
				associatedData,
				ciphertext,
				offset,
				length);
		nonce++; // delay increment so that SecurityException does not increment nonce
		return plaintext;
	}
	
	public void rekey() {
		byte[] maxNonce = Util.hexToBytes("ffffffffffffffff");
		key.replace(key.encrypt(maxNonce, new byte[0], new byte[32], -1));
	}

	public long getRekeyInterval() {
		return rekeyInterval;
	}

	public void setRekeyInterval(long rekeyInterval) {
		this.rekeyInterval = rekeyInterval;
	}
}
