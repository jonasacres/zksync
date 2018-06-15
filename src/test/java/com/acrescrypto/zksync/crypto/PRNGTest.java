package com.acrescrypto.zksync.crypto;

import static org.junit.Assert.*;

import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;

public class PRNGTest {
	byte[] key = { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
                   0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f };
	byte[] iv = { 0x01, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f };
	PRNG prng = new PRNG(key);
	
	@BeforeClass
	public static void beforeClass() {
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}

	/* These aren't comprehensive tests by any stretch, and I don't know any tests that can "prove" the
	 * security of a cryptographic PRNG empirically. But let's try to at least catch the dumb stuff, e.g. we return
	 * 4 bytes of random and set the rest to 0.
	 */
	
	@Test
	public void testBalancedBits() {
		applyBalancedBitsTest(prng.getBytes(512));
	}
	
	@Test
	public void testCorrelation() {
		// i don't think this will catch all that much, but why not
		applyBalancedBitsTest(CryptoSupport.xor(prng.getBytes(512), prng.getBytes(512)));
		applyBalancedBitsTest(CryptoSupport.xor(shiftArray(prng.getBytes(512), 1), prng.getBytes(512)));
	}
	
	@Test
	public void testOutputLength() {
		for(int i = 0; i < 512; i++) {
			assertEquals(i, prng.getBytes(i).length);
		}
	}
	
	@Test
	public void testConsistency() {
		PRNG prng2 = new PRNG(key);
		assertTrue(Arrays.equals(prng.getBytes(512), prng2.getBytes(512)));
	}
	
	@Test
	public void testRandomInit() {
		PRNG randomA = new PRNG(), randomB = new PRNG();
		assertFalse(Arrays.equals(randomA.getBytes(8), randomB.getBytes(8)));
	}
	
	protected byte[] shiftArray(byte[] stream, int bits) {
		byte carryover = 0, mask = (byte) ((1 << bits) - 1);
		byte[] shifted = new byte[stream.length];
		for(int i = 0; i < stream.length; i++) {
			carryover = (byte) ((stream[i] & mask) << (8-bits));
			shifted[i] = (byte) (carryover | stream[i] >>> bits);
		}
		
		return shifted;
	}
	
	protected void applyBalancedBitsTest(byte[] stream) {
		int ones = 0, bits = 8*stream.length;
		for(byte b : stream) {
			while(b != 0) {
				if((b & 1) != 0) ones++;
				b = (byte) ((b >> 1) & (0x7f)); // don't know why >>> isn't working here
			}
		}
		
		if(!(0.45*bits <= ones && ones <= 0.55*bits)) fail(); 
	}
	
	// TODO: match test vector output
}
