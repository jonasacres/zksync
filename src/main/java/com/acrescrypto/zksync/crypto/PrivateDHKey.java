package com.acrescrypto.zksync.crypto;

import org.whispersystems.curve25519.Curve25519;

public class PrivateDHKey {
	protected byte[] privKey;
	protected Curve25519 curve25519 = Curve25519.getInstance(Curve25519.BEST);
	protected PublicDHKey pubKey;
	protected CryptoSupport crypto;
	
	public PrivateDHKey(CryptoSupport crypto, byte[] privKey, byte[] pubKey) {
		this.privKey = privKey;
		this.crypto = crypto;
		this.pubKey = new PublicDHKey(crypto, pubKey);
	}
	
	public byte[] sharedSecret(PublicDHKey otherKey) {
		// TODO DHT: (refactor) Hash this with the public keys in a manner consistent with https://doi.org/10.6028/NIST.SP.800-56Cr1, https://doi.org/10.6028/NIST.SP.800-56Ar3 
		return curve25519.calculateAgreement(otherKey.pubKey, privKey);
	}
	
	public PublicDHKey publicKey() {
		return pubKey;
	}
	
	public byte[] getBytes() {
		return privKey;
	}
	
	public CryptoSupport getCrypto() {
		return crypto;
	}
}
