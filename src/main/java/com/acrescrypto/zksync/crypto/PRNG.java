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
			rng = SecureRandom.getInstance("SHA1PRNG");
		} catch (NoSuchAlgorithmException exc) {
			exc.printStackTrace();
			System.exit(1);
		}
		
		rng.setSeed(seed);
	}
	
	public byte[] getBytes(int length) {
		byte[] output = new byte[length];
		rng.nextBytes(output);
		return output;
	}

	public int getInt() {
		return rng.nextInt();
	}
	
	public int getInt(int bound) {
		return rng.nextInt(bound);
	}
	
	public long getLong() {
		return rng.nextLong();
	}
}
