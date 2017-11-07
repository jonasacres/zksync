package com.acrescrypto.zksync.crypto;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

public class PRNG {
	SecureRandom rng;
	
	public PRNG() {
		rng = new SecureRandom();
	}
	
	public PRNG(byte[] seed) {
		try {
			rng = SecureRandom.getInstance("SHA1PRNG"); // TODO: replace this with something nice
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.exit(1);
		}
		
		rng.setSeed(seed);
	}
	
	public byte[] getBytes(int length) {
		byte[] output = new byte[length];
		rng.nextBytes(output);
		return output;
	}
}
