package com.acrescrypto.zksync.crypto;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.SignatureException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.i2p.crypto.eddsa.EdDSAEngine;
import net.i2p.crypto.eddsa.EdDSAPrivateKey;
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveTable;
import net.i2p.crypto.eddsa.spec.EdDSAParameterSpec;
import net.i2p.crypto.eddsa.spec.EdDSAPrivateKeySpec;

public class PrivateSigningKey {
	protected PublicSigningKey publicKey;
	protected EdDSAPrivateKey privKey;
	protected CryptoSupport crypto;
	protected static Logger logger = LoggerFactory.getLogger(PrivateSigningKey.class);
	
	protected PrivateSigningKey(CryptoSupport crypto, byte[] raw) {
		EdDSAParameterSpec edSpec = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519);
		EdDSAPrivateKeySpec privKeySpec = new EdDSAPrivateKeySpec(edSpec, raw);
		this.crypto = crypto;
		this.privKey = new EdDSAPrivateKey(privKeySpec);
	}
	
	public byte[] sign(byte[] message) {
		return sign(message, 0, message.length);
	}
	
	public byte[] sign(byte[] message, int offset, int length) {
		EdDSAEngine engine = new EdDSAEngine();
		try {
			engine.setParameter(EdDSAEngine.ONE_SHOT_MODE);
			engine.initSign(privKey);
			return engine.signOneShot(message, offset, length);
		} catch (SignatureException | InvalidKeyException | InvalidAlgorithmParameterException exc) {
			// This just plain shouldn't happen.
			logger.error("Error in signing message", exc);
			System.exit(1);
			return null;
		}
	}

	public boolean verify(byte[] message, byte[] signature) {
		return publicKey().verify(message, signature);
	}
	
	public boolean verify(byte[] message, int msgOffset, int msgLen, byte[] signature, int sigOffset, int sigLen) {
		return publicKey().verify(message, msgOffset, msgLen, signature, sigOffset, sigLen);
	}
	
	public PublicSigningKey publicKey() {
		if(publicKey == null) {
			publicKey = new PublicSigningKey(this);
		}
		
		return publicKey;
	}

	public byte[] getBytes() {
		return privKey.getH();
	}
}
