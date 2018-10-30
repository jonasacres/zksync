package com.acrescrypto.zksync.crypto;

import static org.junit.Assert.*;

import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.utility.Util;

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
	public void testDerive() {
		class KeyDerivationExample {
			public byte[] baseKey, data, expectedResult;
			public int index;
			public KeyDerivationExample(String baseKey, int index, String data, String expectedResult) {
				this.baseKey = Util.hexToBytes(baseKey);
				this.index = index;
				this.data = Util.hexToBytes(data);
				this.expectedResult = Util.hexToBytes(expectedResult);
			}
			
			public void validate() {
				assertArrayEquals(expectedResult, new Key(crypto, baseKey).derive(index, data).getRaw());
			}
		}
		
		// tested against implementation in Python 3.6.5 based on hashlib blake2b support, 512-bit hashes
		new KeyDerivationExample("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", 0, "", "7b631364edb74ad050f72914790f9ded649379120b8ae8ba80f43748714b946a").validate();
	    new KeyDerivationExample("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", 1, "", "7b0ae3920ec7d24eddf74411d0e77be1f564216ab08965f6f0d04a6854b8ef46").validate();
	    new KeyDerivationExample("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", 0xffffffff, "", "6afd93b1ef1940549db025541c0294f211ff9f1f5137178bbd9c5f7cbe4f2e99").validate();
	    new KeyDerivationExample("000102030405060708090a0b0c0d0e0f", 0, "", "3e37684bd87c5dcfa6d7ac353e42d503").validate();
	    new KeyDerivationExample("000102030405060708090a0b0c0d0e0f", 0, "00", "c8027344b5059a558ad69b0da256296f").validate();
	    new KeyDerivationExample("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", 0xffffffff, "808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf", "e683c46fb2865002ac42137cb29758000949e9cd8b9f1784b514fdb6a329cc72").validate();
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
