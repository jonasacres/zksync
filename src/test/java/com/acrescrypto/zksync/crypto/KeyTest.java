package com.acrescrypto.zksync.crypto;

import static org.junit.Assert.*;

import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.utility.Util;

public class KeyTest {
	CryptoSupport crypto = CryptoSupport.defaultCrypto();
	
	@BeforeClass
	public static void beforeClass() {
		TestUtils.startDebugMode();
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}

	@Test
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
		// Generated from test-vectors.py, Python 3.6.5, commit 992e0c73062ce353d42197973eb4308255b85f47
		new KeyDerivationExample(
				"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
				"foo",
				"",
				"010c58af3dcaf904b08b657f9278f18bf2bfb65efbd92000b646f5ac66ebdc2f").validate();
			new KeyDerivationExample(
				"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
				"foo",
				"00",
				"010c58af3dcaf904b08b657f9278f18bf2bfb65efbd92000b646f5ac66ebdc2f").validate();
			new KeyDerivationExample(
				"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
				"bar",
				"",
				"82eb5c004e2890e274faa46e0dd16b8c476d558ff8ecc9ff162d7dc3ad5411f1").validate();
			new KeyDerivationExample(
				"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
				"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
				"",
				"fee63ce71ced6ed84d3cc52bf9af93068f8f252aa293d6c9fd3bddf3d4227773").validate();
			new KeyDerivationExample(
				"000102030405060708090a0b0c0d0e0f",
				"foo",
				"",
				"09ff8d92bb76baa696ef3f66f173d5b1").validate();
			new KeyDerivationExample(
				"000102030405060708090a0b0c0d0e0f",
				"foo",
				"00",
				"09ff8d92bb76baa696ef3f66f173d5b1").validate();
			new KeyDerivationExample(
				"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
				"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
				"808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf",
				"c84b4ee9d379680a9a5abc0d93fda4e5e8fad56cc473878a9709027690e8ff22").validate();
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
