package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.utility.Util;

public class DHTIDTest {
	static CryptoSupport crypto;
	
	@BeforeClass
	public static void beforeAll() {
		crypto = new CryptoSupport();
	}
	
	@Test
	public void testXor() {
		int len = 64;
		DHTID id0 = new DHTID(crypto.rng(len));
		DHTID id1 = new DHTID(crypto.rng(len));
		byte[] xor = id0.xor(id1).rawId;
		
		for(int i = 0; i < len; i++) {
			assertEquals(id0.rawId[i] ^ id1.rawId[i], xor[i]);
		}
	}
	
	@Test
	public void testOrder() {
		int len = 32;
		
		assertEquals(-1, new DHTID(new byte[len]).order());
		
		for(int i = 0; i < 8*len; i++) {
			byte[] id = new byte[len];
			int byteIdx = len - i/8 - 1, bitIdx = (i%8);
			
			id[byteIdx] |= (1 << bitIdx);
			assertEquals(i, new DHTID(id).order());
		}
	}
	
	@Test
	public void testSerialize() {
		byte[] id = crypto.rng(Util.unsignByte(crypto.rng(1)[0]));
		assertNotEquals(id, new DHTID(id).serialize());
		assertTrue(Arrays.equals(id, new DHTID(id).serialize()));
	}
	
	@Test
	public void testCompareTo() {
		String[][] cases = {
				{ "0001", "0000" },
				{ "0010", "000f" },
				{ "0100", "00ff" },
				
				{ "0001", "0000" },
				{ "001f", "0000" },
				{ "01ff", "0000" },

				{ "0001", "0000" },
				{ "001f", "000f" },
				{ "01ff", "00ff" },
			};
		
		for(String[] c : cases) {
			DHTID high = new DHTID(Util.hexToBytes(c[0]));
			DHTID low = new DHTID(Util.hexToBytes(c[1]));
			assertTrue(high.compareTo(low) > 0);
			assertTrue(low.compareTo(high) < 0);
		}
	}
}
