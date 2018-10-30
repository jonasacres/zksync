package com.acrescrypto.zksync.crypto;

import java.util.Arrays;

public class PublicDHKey {
	protected byte[] pubKey;
	protected CryptoSupport crypto;
	public final static int KEY_SIZE = PrivateDHKey.KEY_SIZE;
	
	public PublicDHKey(CryptoSupport crypto, byte[] key) {
		this.crypto = crypto;
		this.pubKey = key;
	}
	
	public byte[] getBytes() {
		return pubKey;
	}
	
	public CryptoSupport getCrypto() {
		return crypto;
	}
	
	public boolean equals(Object other) {
		if(!(other instanceof PublicDHKey)) return false;
		
		return Arrays.equals(pubKey, ((PublicDHKey) other).pubKey);
	}
}
