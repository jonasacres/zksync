package com.acrescrypto.zksync.net.noise;


import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.utility.Util;

// per section 5.1, Noise specification, http://noiseprotocol.org/noise.html, rev 34, 2018-07-11
public class CipherState {
	protected Key key;         // 'k' in specification
	private long nonce;      // 'n' in specification
	private long rekeyInterval = -1; // automatically rekey after this many messages
	
	public CipherState() {
	}
	
	protected CipherState(CipherState state) {
		if(state.key != null) {
			this.key = new Key(state.key.getCrypto(), state.key.getRaw().clone());
		}
		
		this.nonce = state.nonce;
		this.rekeyInterval = state.rekeyInterval;
	}
	
	public void initializeKey(Key key) {
		if(this.key != null) {
			this.key.destroy();
		}
		
		this.key = key;
		nonce = 0;
	}
	
	public boolean hasKey() {
		return key != null;
	}
	
	public long getNonce() {
		return nonce;
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
		if(rekeyInterval > 0 && (nonce + 1) % rekeyInterval == 0) rekey();
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
		if(rekeyInterval > 0 && (nonce + 1) % rekeyInterval == 0) rekey();
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
		byte[] maxNonce = Util.serializeLong(-1L); // 0xffff...
		byte[] newKey = key.encrypt(maxNonce, new byte[0], new byte[key.getRaw().length], -1);
		byte[] newKeyShort = new byte[key.getRaw().length]; // cut the tag out
		
		System.arraycopy(newKey, 0, newKeyShort, 0, newKeyShort.length);
		System.arraycopy(new byte[newKey.length], 0, newKey, 0, newKey.length);
		
		key.replace(newKeyShort);
	}

	public long getRekeyInterval() {
		return rekeyInterval;
	}

	public void setRekeyInterval(long rekeyInterval) {
		this.rekeyInterval = rekeyInterval;
	}
	
	public boolean equals(Object _other) {
		if(!(_other instanceof CipherState)) return false;
		CipherState other = (CipherState) _other;
		
		boolean equals = true;
		equals &= nonce == other.nonce;
		equals &= Util.safeEquals(key.getRaw(), other.key.getRaw());
		return equals;
	}
}
