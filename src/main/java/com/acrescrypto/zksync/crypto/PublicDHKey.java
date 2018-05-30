package com.acrescrypto.zksync.crypto;

import org.whispersystems.curve25519.Curve25519;

public class PublicDHKey {
	protected byte[] pubKey;
	protected Curve25519 curve25519 = Curve25519.getInstance(Curve25519.BEST);
	protected CryptoSupport crypto;
	
	public PublicDHKey(CryptoSupport crypto, byte[] key) {
		this.crypto = crypto;
		this.pubKey = key;
	}
	
	public byte[] getBytes() {
		return pubKey;
	}
	
	public CryptoSupport getCrypto() {
		return crypto;
	}
}
