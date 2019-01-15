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
import com.acrescrypto.zksync.utility.Util;

public class KeyTest {
	CryptoSupport crypto = CryptoSupport.defaultCrypto();
	
	@BeforeClass
	public static void beforeClass() {
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}

	// TODO EasySafe: (test) Recalculate test vectors once new config file is dialed in
	@Test @Ignore
	public void testDerive() {
		class KeyDerivationExample {
			public byte[] baseKey, data, expectedResult;
			public String id;
			public KeyDerivationExample(String baseKey, String id, String data, String expectedResult) {
				this.baseKey = Util.hexToBytes(baseKey);
				this.id = id;
				this.data = Util.hexToBytes(data);
				this.expectedResult = Util.hexToBytes(expectedResult);
			}
			
			public void validate() {
				assertArrayEquals(expectedResult, new Key(crypto, baseKey).derive(id, data).getRaw());
			}
		}
		
		// Test vectors for Key.derive, used in KeyTest.testDerive()
		// Generated from test-vectors.py, Python 3.6.5, commit db67d8c388d18cb428e257e42baf7c40682f9b83
		new KeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			"0",
			"",
			"b6abfc6470a720a02b3c11cc12d62aac86502bcc79bc13670191730695a95ff0").validate();
		new KeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			"1",
			"",
			"fdcad2202cd184924bd7911b222471320c6e4a44871eb6cafbc8435bc9eba6bd").validate();
		new KeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			"",
			"565b06e0e32896d12b1037733459c6fd72d5f92c2494926f9539101232c5cea7").validate();
		new KeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f",
			"0",
			"",
			"7d6828664cd9c40f0a2731551e57dfee").validate();
		new KeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f",
			"0",
			"00",
			"7d6828664cd9c40f0a2731551e57dfee").validate();
		new KeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			"0",
			"808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf",
			"3bbbe1606ae844b6b205a50729bf4722300f9ac130b1909b95889b1181c91f4e").validate();
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
