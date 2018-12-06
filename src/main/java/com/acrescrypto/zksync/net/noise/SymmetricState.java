package com.acrescrypto.zksync.net.noise;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.utility.Util;

// per section 5.2, Noise specification, http://noiseprotocol.org/noise.html, rev 34, 2018-07-11
public class SymmetricState {
	protected CryptoSupport crypto;
	protected CipherState cipherState;
	private Key chainingKey;          // 'ck' in Noise specification
	private byte[] hash;              // 'h' in Noise specification
	
	public SymmetricState(CryptoSupport crypto, String protocolName) {
		this.crypto = crypto;
		if(protocolName.getBytes().length <= crypto.hashLength()) {
			byte[] padded = new byte[crypto.hashLength()];
			System.arraycopy(protocolName.getBytes(), 0, padded, 0, protocolName.getBytes().length);
			hash = padded;
		} else {
			hash = crypto.hash(protocolName.getBytes());
		}
		
		this.chainingKey = new Key(crypto, new byte[crypto.hashLength()]);
		cipherState.initializeKey(null);
	}
	
	protected void mixKey(byte[] inputKeyMaterial) {
		byte[][] newMaterial = hkdf(inputKeyMaterial, 2);
		
		chainingKey.replace(newMaterial[0]);
		Key tempKey = new Key(crypto, newMaterial[1]);
		cipherState.initializeKey(tempKey);
	}
	
	protected void mixHash(byte[] data) {
		hash = crypto.hash(Util.concat(hash, data));
	}
	
	protected void mixKeyAndHash(byte[] inputKeyMaterial) {
		byte[][] newMaterial = hkdf(inputKeyMaterial, 3);
		
		chainingKey.replace(newMaterial[0]);
		mixHash(newMaterial[1]);
		cipherState.initializeKey(truncatedKey(newMaterial[2]));
	}
	
	public byte[] getHandshakeHash() {
		return hash;
	}
	
	public byte[] encryptAndHash(byte[] plaintext) {
		byte[] ciphertext = cipherState.encryptWithAssociatedData(hash, plaintext);
		mixHash(ciphertext);
		return ciphertext;
	}
	
	public byte[] decryptAndHash(byte[] ciphertext) {
		byte[] plaintext = cipherState.decryptWithAssociatedData(hash, ciphertext);
		mixHash(ciphertext);
		return plaintext;
	}
	
	public CipherState[] split() {
		byte[][] newMaterial = hkdf(new byte[0], 2);
		CipherState[] newStates = new CipherState[2];
		
		// split() is the end of this state machine's work, so we may as well destroy the key
		chainingKey.destroy();
		
		for(int i = 0; i < 2; i++) {
			newStates[i] = new CipherState();
			newStates[i].initializeKey(truncatedKey(newMaterial[i]));
		}
		
		return newStates;
	}
	
	protected byte[][] hkdf(byte[] inputKeyMaterial, int numResults) {
		byte[] output = crypto.expand(inputKeyMaterial,
				numResults*crypto.hashLength(),
				chainingKey.getRaw(),
				new byte[0]);
		
		byte[][] results = new byte[numResults][];
		for(int i = 0; i < numResults; i++) {
			results[i] = new byte[crypto.hashLength()];
			System.arraycopy(output, i*crypto.hashLength(), results[i], 0, crypto.hashLength());
		}
		
		for(int i = 0; i < output.length; i++) {
			output[i] = 0;
		}
		
		return results;
	}
	
	protected Key truncatedKey(byte[] input) {
		if(input.length > crypto.symKeyLength()) {
			byte[] truncated = new byte[crypto.symKeyLength()];
			System.arraycopy(input, 0, truncated, 0, truncated.length);
			for(int i = 0; i < input.length; i++) {
				input[i] = 0;
			}
			return new Key(crypto, truncated);
		}
		
		return new Key(crypto, input);
	}
}
