package com.acrescrypto.zksync.crypto;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class PRNG {
	Cipher cipher;
	
	public PRNG(byte[] key, byte[] iv) {
		try {
			Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC");
			cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES/OCB/NoPadding") , new IvParameterSpec(iv));
		} catch (InvalidKeyException | InvalidAlgorithmParameterException | NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public byte[] getBytes(int length) {
		byte[] zeros = new byte[length];
		return cipher.update(zeros);
	}
}
