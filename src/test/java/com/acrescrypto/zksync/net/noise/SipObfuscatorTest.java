package com.acrescrypto.zksync.net.noise;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.utility.Util;

import au.com.forward.sipHash.SipHash_2_4;

public class SipObfuscatorTest {
	CryptoSupport crypto;
	SipObfuscator sipInit;
	SipObfuscator sipResp;
	byte[] ikm;
	byte[][] topKeys;
	
	@Before
	public void beforeEach() {
		crypto = CryptoSupport.defaultCrypto();
		ikm = crypto.symNonce(0);
		sipInit = new SipObfuscator(ikm, true);
		sipResp = new SipObfuscator(ikm, false);
		topKeys = deriveTopKeys();
	}
	
	byte[][] deriveTopKeys() {
		byte[][] keys = new byte[2][];
		keys[0] = crypto.authenticate(ikm, new byte[] { 0x01 });
		keys[1] = crypto.authenticate(ikm, Util.concat(keys[0], new byte[] { 0x02 }));
		return keys;
	}
	
	void checkKeyDerivation(byte[] expectedRoot, byte[] key, long iv) {
		byte[] expectedDerivation = crypto.expand(expectedRoot, 24, new byte[0], "siphash".getBytes());
		byte[] expectedKey = new byte[16];
		byte[] expectedIv = new byte[8];
		
		System.arraycopy(expectedDerivation, expectedKey.length, expectedIv, 0, expectedIv.length);
		System.arraycopy(expectedDerivation, 0, expectedKey, 0, expectedKey.length);
		
		assertArrayEquals(expectedKey, key);
		assertArrayEquals(expectedIv, Util.serializeLong(iv));
	}
	
	@Test
	public void testConstructorSetsReadStateFromKey1WhenInitiator() {
		checkKeyDerivation(topKeys[0], sipInit.read.key, sipInit.read.iv);
	}

	@Test
	public void testConstructorSetsReadStateFromKey2WhenResponder() {
		checkKeyDerivation(topKeys[1], sipResp.read.key, sipResp.read.iv);
	}

	@Test
	public void testConstructorSetsWriteStateFromKey1WhenResponder() {
		checkKeyDerivation(topKeys[0], sipResp.write.key, sipResp.write.iv);
	}

	@Test
	public void testConstructorSetsWriteStateFromKey2WhenInitiator() {
		checkKeyDerivation(topKeys[1], sipInit.write.key, sipInit.write.iv);
	}
	
	@Test
	public void testObfuscate2Masks16BitData() {
		int matches = 0;
		for(int i = 0; i < 65536; i++) {
			if(i == sipInit.write().obfuscate2(i)) {
				matches++;
			}
		}
		
		// allow a couple coincidental matches
		assertTrue(matches <= 2);
	}
	
	@Test
	public void testObfuscate2RotatesIV() {
		byte[] iv = Util.serializeLong(sipInit.write().iv), key = sipInit.write().key;
		SipHash_2_4 siphash = new SipHash_2_4();
		siphash.hash(key, iv);
		long newIv = siphash.finish();
		
		sipInit.write().obfuscate2(0);
		assertEquals(newIv, sipInit.write().iv);
	}
	
	@Test
	public void testLooksNoisy() {
		// very simple check to see if each bit is set with 50% probability
		int[] counts = new int[16];
		int iterations = 1000*1000*10; // 10 million
		
		for(int i = 0; i < iterations; i++) {
			int obfuscated = sipInit.write().obfuscate2(0);
			for(int j = 0; j < 16; j++) {
				counts[j] += ((obfuscated >> j) & 1);
			}
		}
		
		int expected = iterations/2;
		for(int i = 0; i < 16; i++) {
			int diff = Math.abs(counts[i] - expected);
			assertTrue(diff < 0.001*iterations); // are we within 0.1% of an even split?
		}
	}
	
	@Test
	public void testSymmetryRespToInit() {
		for(int i = 0; i < 65536; i++) {
			int obf = sipResp.write().obfuscate2(i);
			int recovered = sipInit.read().obfuscate2(obf);
			assertEquals(i, recovered);
		}
	}
	
	@Test
	public void testSymmetryInitToResp() {
		for(int i = 0; i < 65536; i++) {
			int obf = sipInit.write().obfuscate2(i);
			int recovered = sipResp.read().obfuscate2(obf);
			assertEquals(i, recovered);
		}
	}
}
