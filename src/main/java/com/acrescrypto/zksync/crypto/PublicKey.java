package com.acrescrypto.zksync.crypto;

import java.security.InvalidKeyException;
import java.security.SignatureException;

import net.i2p.crypto.eddsa.EdDSAEngine;
import net.i2p.crypto.eddsa.EdDSAPublicKey;
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveTable;
import net.i2p.crypto.eddsa.spec.EdDSAParameterSpec;
import net.i2p.crypto.eddsa.spec.EdDSAPublicKeySpec;

// TODO: (refactor) It's somewhat confusing with the existing JCA that we have a "PublicKey" that doesn't fit into the framework at all. Ditto for PrivateKey...
public class PublicKey {
	public final static int KEY_SIZE = 256/8;
	public final static int SIG_SIZE = 512/8;
	protected EdDSAPublicKey pubKey;
	
	public PublicKey(PrivateKey privateKey) {
		this(privateKey.privKey.getA().toByteArray());
	}
	
	public PublicKey(byte[] raw) {
		EdDSAParameterSpec edSpec = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519);
		EdDSAPublicKeySpec pubKeySpec = new EdDSAPublicKeySpec(raw, edSpec);
		this.pubKey = new EdDSAPublicKey(pubKeySpec);
	}

	public boolean verify(byte[] message, byte[] signature) {
		EdDSAEngine engine = new EdDSAEngine();
		try {
			engine.initVerify(pubKey);
			return engine.verifyOneShot(message, signature);
		} catch (SignatureException | InvalidKeyException e) {
			return false;
		}
	}
	
	public byte[] getBytes() {
		return pubKey.getA().toByteArray();
	}
}
