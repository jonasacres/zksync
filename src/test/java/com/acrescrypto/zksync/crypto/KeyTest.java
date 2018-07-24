package com.acrescrypto.zksync.crypto;

import static org.junit.Assert.*;

import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;

public class KeyTest {
	CryptoSupport crypto = new CryptoSupport();
	
	@BeforeClass
	public static void beforeClass() {
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}

	@Test
	@Ignore
	public void testDerive() {
		// TODO: need test vectors validated in another language
	}
	
	@Test
	public void testManualKey() {
		byte[] raw = new byte[crypto.symKeyLength()];
		for(int i = 0; i < raw.length; i++) raw[i] = (byte) i;
		assertTrue(Arrays.equals((new Key(crypto, raw)).getRaw(), raw));
	}
	
	@Test
	public void testEncrypt() {
		// make sure Key is a front-end to CryptoSupport
		byte[] key = new byte[32], iv = new byte[12];
		for(int i = 0; i < 128; i++) {
			byte[] buf = new byte[i];
			key[31] = (byte) i;
			iv[11] = (byte) i;
			byte[] ciphertext = (new Key(crypto, key)).encrypt(iv, buf, -1);
			byte[] expected = crypto.encrypt(key, iv, buf, null, -1);
			assertTrue(Arrays.equals(expected, ciphertext));
		}
	}
	
	@Test
	public void testDecrypt() {
		// make sure we can decrypt what we encrypt
		byte[] keyBytes = new byte[32], iv = new byte[12];
		for(int i = 0; i < 128; i++) {
			byte[] buf = new byte[i];
			keyBytes[31] = (byte) i;
			iv[11] = (byte) i;
			Key key = new Key(crypto, keyBytes);
			byte[] recovered = key.decrypt(iv, key.encrypt(iv, buf, 0));
			assertTrue(Arrays.equals(buf, recovered));
		}
	}
	
	@Test
	public void testDecryptUnpadded() {
		// if we encrypt with -1 padding (no padding), we should be able to call decryptUnpadded
		byte[] keyBytes = new byte[32], iv = new byte[12];
		for(int i = 0; i < 128; i++) {
			byte[] buf = new byte[i];
			keyBytes[31] = (byte) i;
			iv[11] = (byte) i;
			Key key = new Key(crypto, keyBytes);
			byte[] recovered = key.decryptUnpadded(iv, key.encrypt(iv, buf, -1));
			assertTrue(Arrays.equals(buf, recovered));
		}
	}
}
