package com.acrescrypto.zksync.crypto;

import org.bouncycastle.crypto.Digest;
import org.bouncycastle.crypto.digests.Blake2bDigest;

public class HashContext {
	public final static int HASH_SIZE = 64;
	public final static int BLOCK_SIZE = 128;
	
	private Digest digest = new Blake2bDigest(8*HASH_SIZE);
	
	public HashContext(byte[] data) {
		update(data);
	}
	
	public HashContext() {
	}

	public HashContext update(byte[] data) {
		digest.update(data, 0, data.length);
		return this;
	}
	
	public HashContext update(byte[] data, int offset, int length) {
		digest.update(data, offset, length);
		return this;
	}

	public byte[] finish() {
		byte[] finalDigest = new byte[digest.getDigestSize()];
		digest.doFinal(finalDigest, 0);
		return finalDigest;
	}
}
