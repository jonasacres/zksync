package com.acrescrypto.zksync.crypto;

import java.nio.ByteBuffer;

import org.bouncycastle.util.Arrays;

import com.acrescrypto.zksync.utility.Util;

import org.bouncycastle.math.ec.rfc7748.X25519; 

public class PrivateDHKey {
	protected byte[] privKey;
	protected PublicDHKey pubKey;
	protected CryptoSupport crypto;
	public final static int KEY_SIZE = 32;
	public final static int RAW_SECRET_SIZE = 32;
	
	public PrivateDHKey(CryptoSupport crypto) {
		byte[] privKeyRaw = crypto.rng(KEY_SIZE);
		byte[] pubKeyRaw = new byte[PublicDHKey.KEY_SIZE];
		X25519.scalarMultBase(privKeyRaw, 0, pubKeyRaw, 0);
		
		this.crypto = crypto;
		this.privKey = privKeyRaw;
		this.pubKey = new PublicDHKey(crypto, pubKeyRaw);
	}
	
	public PrivateDHKey(CryptoSupport crypto, byte[] privKey, byte[] pubKey) {
		this.privKey = privKey;
		this.crypto = crypto;
		this.pubKey = new PublicDHKey(crypto, pubKey);
	}
	
	protected byte[] sharedSecretRaw(PublicDHKey otherKey) {
		// implementation cribbed from https://github.com/bcgit/bc-java/commit/1f559bba32d601ddc76e1b306c566ba20d23b4ea
		byte[] sharedSecret = new byte[RAW_SECRET_SIZE];
		X25519.scalarMult(privKey, 0, otherKey.pubKey, 0, sharedSecret, 0);
		return sharedSecret;
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
		byte[] rawSecret = sharedSecretRaw(otherKey);
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
		privKey = Util.zero(privKey);
		pubKey.destroy();
	}
}
