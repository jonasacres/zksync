package com.acrescrypto.zksync.crypto;

import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.Blake2bDigest;

public class HashContext {
	private Digest digest = new Blake2bDigest(64*8);
	
	public HashContext(byte[] data) {
		update(data);
	}
	
	public HashContext() {
	}

	public HashContext update(byte[] data) {
		digest.update(data, 0, data.length);
		return this;
	}
	
	public byte[] finish() {
		byte[] finalDigest = new byte[digest.getDigestSize()];
		digest.doFinal(finalDigest, 0);
		return finalDigest;
	}
}
