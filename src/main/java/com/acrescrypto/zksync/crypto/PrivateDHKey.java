package com.acrescrypto.zksync.crypto;

import java.nio.ByteBuffer;

import org.bouncycastle.util.Arrays;
import org.whispersystems.curve25519.Curve25519;

import com.acrescrypto.zksync.utility.Util;

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
	
	protected byte[] sharedSecretRaw(PublicDHKey otherKey) {
		return curve25519.calculateAgreement(otherKey.getBytes(), privKey);
	}
	
	public byte[] sharedSecret(PublicDHKey otherKey) {
		byte[] lesserKey, greaterKey, salt = "zksync".getBytes();
		
		if(Arrays.compareUnsigned(pubKey.getBytes(), otherKey.getBytes()) < 0) {
			lesserKey = pubKey.getBytes();
			greaterKey = otherKey.getBytes();
		} else {
			lesserKey = otherKey.getBytes();
			greaterKey = pubKey.getBytes();
		}
		
		ByteBuffer buf = ByteBuffer.allocate(pubKey.getBytes().length + otherKey.getBytes().length);
		buf.put(lesserKey);
		buf.put(greaterKey);
		byte[] rawSecret = curve25519.calculateAgreement(otherKey.pubKey, privKey);
		return crypto.expand(rawSecret, crypto.symKeyLength(), salt, buf.array());
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
	
	public void destroy() {
		Util.blank(privKey);
		pubKey.destroy();
	}
}
