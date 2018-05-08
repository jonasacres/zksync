package com.acrescrypto.zksync.crypto;

import java.security.InvalidKeyException;
import java.security.SignatureException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.i2p.crypto.eddsa.EdDSAEngine;
import net.i2p.crypto.eddsa.EdDSAPrivateKey;
import net.i2p.crypto.eddsa.spec.EdDSANamedCurveTable;
import net.i2p.crypto.eddsa.spec.EdDSAParameterSpec;
import net.i2p.crypto.eddsa.spec.EdDSAPrivateKeySpec;

public class PrivateKey {
	protected PublicKey publicKey;
	protected EdDSAPrivateKey privKey;
	protected CryptoSupport crypto;
	protected static Logger logger = LoggerFactory.getLogger(PrivateKey.class);
	
	protected PrivateKey(CryptoSupport crypto, byte[] raw) {
		EdDSAParameterSpec edSpec = EdDSANamedCurveTable.getByName(EdDSANamedCurveTable.ED_25519);
		EdDSAPrivateKeySpec privKeySpec = new EdDSAPrivateKeySpec(raw, edSpec);
		this.crypto = crypto;
		this.privKey = new EdDSAPrivateKey(privKeySpec);
	}
	
	public byte[] sign(byte[] message) {
		EdDSAEngine engine = new EdDSAEngine();
		try {
			engine.initSign(privKey);
			return engine.signOneShot(message);
		} catch (SignatureException | InvalidKeyException exc) {
			// This just plain shouldn't happen.
			logger.error("Error in signing message", exc);
			System.exit(1);
			return null;
		}
	}
	
	public boolean verify(byte[] message, byte[] signature) {
		return publicKey().verify(message, signature);
	}
	
	public PublicKey publicKey() {
		if(publicKey == null) {
			publicKey = new PublicKey(this);
		}
		
		return publicKey;
	}

	public byte[] getBytes() {
		return privKey.getH();
	}
}
