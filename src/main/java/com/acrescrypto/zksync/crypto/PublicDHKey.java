package com.acrescrypto.zksync.crypto;

import org.whispersystems.curve25519.Curve25519;

public class PublicDHKey {
	protected byte[] pubKey;
	protected Curve25519 curve25519 = Curve25519.getInstance(Curve25519.BEST);
	
	public PublicDHKey(byte[] key) {
		this.pubKey = key;
	}
	
	public byte[] getBytes() {
		return pubKey;
	}
}
