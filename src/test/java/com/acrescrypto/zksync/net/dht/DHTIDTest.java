package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.utility.Util;

public class DHTIDTest {
	static CryptoSupport crypto;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
		crypto = CryptoSupport.defaultCrypto();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Test
	public void testXor() {
		int    len = 64;
		DHTID  id0 = DHTID.withBytes(crypto.rng(len));
		DHTID  id1 = DHTID.withBytes(crypto.rng(len));
		byte[] xor = id0.xor(id1).serialize();
		byte[] ie0 = id0         .serialize(),
			   ie1 = id1         .serialize();
		
		for(int i = 0; i < len; i++) {
			assertEquals(ie0[i] ^ ie1[i], xor[i]);
		}
	}
	
	@Test
	public void testSerialize() {
		for(int i = 1; i < 256; i++) {
			byte[] raw        = crypto.expand(
					              crypto.symNonce(i),
					              i,
					              new byte[0],
					              new byte[0]); // random-looking byte array of length i
			DHTID  id         = DHTID.withBytes(raw);
			byte[] serialized = id.serialize();
			
			assertEquals     (i,   raw       .length);
			assertEquals     (i,   serialized.length);
			assertNotEquals  (raw, serialized); // can't just give us back the same array (i.e. original memory address)
			assertArrayEquals(raw, serialized); // but the arrays need to have identical content
		}
	}
	
	@Test
	public void testCompareTo() {
		String[][] cases = {
				{ "0001", "0000" },
				{ "00ff", "0000" },
				{ "0010", "000f" },
				{ "0100", "00ff" },
				
				{ "ff00", "0000" },
				{ "001f", "0000" },
				{ "01ff", "0000" },

				{ "0001", "0000" },
				{ "001f", "000f" },
				{ "01ff", "00ff" },
			};
		
		for(String[] c : cases) {
			DHTID high = DHTID.withBytes(Util.hexToBytes(c[0]));
			DHTID low  = DHTID.withBytes(Util.hexToBytes(c[1]));
			assertTrue(high.compareTo(low)  >  0);
			assertTrue(high.compareTo(high) == 0);
			assertTrue(low .compareTo(high) <  0);
			assertTrue(low .compareTo(low)  == 0);
		}
	}
}
