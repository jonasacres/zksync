package com.acrescrypto.zksync.crypto;

import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.HashContext;
import com.acrescrypto.zksync.utility.Util;

public class HashContextTest {
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}

	@Test
	public void testMatchesReference() {
		byte[][][] vectors = getTestVectors();
		for(int i = 0; i < vectors.length; i++) {
			byte[] digest = new HashContext(vectors[i][0]).finish();
			assertTrue(Arrays.equals(digest, vectors[i][1]));
		}
	}
	
	@Test
	public void testMatchesReferenceWhenBuiltByUpdate() {
		byte[][][] vectors = getTestVectors();
		for(int i = 0; i < vectors.length; i++) {
			byte[] digest = new HashContext().update(vectors[i][0]).finish();
			assertTrue(Arrays.equals(digest, vectors[i][1]));
		}
	}
	
	@Test
	public void testMatchesReferenceWhenBuiltByConstructorAndUpdate() {
		byte[][][] vectors = getTestVectors();
		for(int i = 0; i < vectors.length; i++) {
			if(vectors[i][0].length < 2) continue;
			int div = vectors[i][0].length / 2;
			
			ByteBuffer firstHalf = ByteBuffer.allocate(div), secondHalf = ByteBuffer.allocate(vectors[i][0].length - div);
			firstHalf.put(vectors[i][0], 0, firstHalf.capacity());
			secondHalf.put(vectors[i][0], div, secondHalf.capacity());
			
			byte[] digest = new HashContext(firstHalf.array()).update(secondHalf.array()).finish();
			assertTrue(Arrays.equals(digest, vectors[i][1]));
		}
	}
	
	@Test
	public void testMatchesReferenceWhenReallyLong() {
		ByteBuffer chunk = ByteBuffer.allocate(256);
		for(int b = 0; b < chunk.capacity(); b++)
			chunk.put((byte) b);

		HashContext context = new HashContext();
		for(int i = 0; i < 65536/chunk.capacity(); i++) context.update(chunk.array());
		byte[] digest = context.finish();
		
		// test vector created using pyblake2 on python 2.7.13
		byte[] testVector = Util.hexToBytes("628791b04211d62163c51e6f91f8f95ddc431774410e2d1031a394f6c9ad0090a5dff68b26eb81e83692e4ce6feff6a28ef6182756670ae78a0a702be145deaf");
		assertTrue(Arrays.equals(digest, testVector));
	}
	
	public byte[][][] getTestVectors() {
		// BLAKE2b test vectors, taken from https://github.com/openssl/openssl/blob/2d0b44126763f989a4cbffbffe9d0c7518158bb7/test/evptests.txt
		return new byte[][][] {
			{
				Util.hexToBytes(""),
				Util.hexToBytes("786a02f742015903c6c6fd852552d272912f4740e15847618a86e217f71f5419d25e1031afee585313896444934eb04b903a685b1448b755d56f701afe9be2ce")
			},
			{
				Util.hexToBytes("61"),
				Util.hexToBytes("333fcb4ee1aa7c115355ec66ceac917c8bfd815bf7587d325aec1864edd24e34d5abe2c6b1b5ee3face62fed78dbef802f2a85cb91d455a8f5249d330853cb3c")
			},
			{
				Util.hexToBytes("616263"),
				Util.hexToBytes("ba80a53f981c4d0d6a2797b69f12f6e94c212f14685ac4b74b12bb6fdbffa2d17d87c5392aab792dc252d5de4533cc9518d38aa8dbf1925ab92386edd4009923")
			},
			{
				Util.hexToBytes("6d65737361676520646967657374"),
				Util.hexToBytes("3c26ce487b1c0f062363afa3c675ebdbf5f4ef9bdc022cfbef91e3111cdc283840d8331fc30a8a0906cff4bcdbcd230c61aaec60fdfad457ed96b709a382359a")
			},
			{
				Util.hexToBytes("6162636465666768696a6b6c6d6e6f707172737475767778797a"),
				Util.hexToBytes("c68ede143e416eb7b4aaae0d8e48e55dd529eafed10b1df1a61416953a2b0a5666c761e7d412e6709e31ffe221b7a7a73908cb95a4d120b8b090a87d1fbedb4c")
			},
			{
				Util.hexToBytes("4142434445464748494a4b4c4d4e4f505152535455565758595a6162636465666768696a6b6c6d6e6f707172737475767778797a30313233343536373839"),
				Util.hexToBytes("99964802e5c25e703722905d3fb80046b6bca698ca9e2cc7e49b4fe1fa087c2edf0312dfbb275cf250a1e542fd5dc2edd313f9c491127c2e8c0c9b24168e2d50")
			},
			{
				Util.hexToBytes("3132333435363738393031323334353637383930313233343536373839303132333435363738393031323334353637383930313233343536373839303132333435363738393031323334353637383930"),
				Util.hexToBytes("686f41ec5afff6e87e1f076f542aa466466ff5fbde162c48481ba48a748d842799f5b30f5b67fc684771b33b994206d05cc310f31914edd7b97e41860d77d282")
			}
		};
	}
}
