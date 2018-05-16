package com.acrescrypto.zksync.crypto;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.SignatureException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.i2p.crypto.eddsa.EdDSAEngine;
import net.i2p.crypto.eddsa.EdDSAPublicKey;
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveTable;
import net.i2p.crypto.eddsa.spec.EdDSAParameterSpec;
import net.i2p.crypto.eddsa.spec.EdDSAPublicKeySpec;

// TODO: (refactor) It's somewhat confusing with the existing JCA that we have a "PublicKey" that doesn't fit into the framework at all. Ditto for PrivateKey...
public class PublicKey {
	protected EdDSAPublicKey pubKey;
	protected CryptoSupport crypto;
	protected static Logger logger = LoggerFactory.getLogger(PublicKey.class);
	
	protected PublicKey(PrivateKey privateKey) {
		this(privateKey.crypto, privateKey.privKey.getA().toByteArray());
	}
	
	protected PublicKey(CryptoSupport crypto, byte[] raw) {
		EdDSAParameterSpec edSpec = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519);
		EdDSAPublicKeySpec pubKeySpec = new EdDSAPublicKeySpec(raw, edSpec);
		this.crypto = crypto;
		this.pubKey = new EdDSAPublicKey(pubKeySpec);
	}

	public boolean verify(byte[] message, byte[] signature) {
		return verify(message, 0, message.length, signature, 0, signature.length);
	}
	
	public boolean verify(byte[] message, int msgOffset, int msgLen, byte[] signature, int sigOffset, int sigLen) {
		EdDSAEngine engine = new EdDSAEngine();
		try {
			engine.setParameter(EdDSAEngine.ONE_SHOT_MODE);
			engine.initVerify(pubKey);
			return engine.verifyOneShot(message, msgOffset, msgLen, signature, sigOffset, sigLen);
		} catch (SignatureException | InvalidKeyException | InvalidAlgorithmParameterException exc) {
			logger.warn("Error in verifying message", exc);
			return false;
		}
	}
	
	public void assertValid(byte[] message, int msgOffset, int msgLen, byte[] signature, int sigOffset, int sigLen) {
		if(!verify(message, msgOffset, msgLen, signature, sigOffset, sigLen)) {
			throw new SecurityException();
		}
	}
	
	public byte[] getBytes() {
		return pubKey.getA().toByteArray();
	}
	
	public CryptoSupport getCrypto() {
		return crypto;
	}
}
