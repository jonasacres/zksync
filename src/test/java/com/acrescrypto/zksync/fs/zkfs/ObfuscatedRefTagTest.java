package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.exceptions.InvalidSignatureException;

public class ObfuscatedRefTagTest {
	ZKMaster master;
	RefTag refTag;
	ObfuscatedRefTag obfTag;

	@BeforeClass
	public static void beforeClass() {
		ZKFSTest.cheapenArgon2Costs();
		Security.addProvider(new BouncyCastleProvider());
	}

	@AfterClass
	public static void afterClass() {
		ZKFSTest.restoreArgon2Costs();
	}

	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		ZKFS fs = archive.openBlank();
		fs.write("test", "test".getBytes());
		refTag = fs.commit();
		obfTag = new ObfuscatedRefTag(refTag);
	}
	
	@Test
	public void testSerialization() {
		byte[] serialized = obfTag.serialize();
		ObfuscatedRefTag deserialized = new ObfuscatedRefTag(refTag.archive, serialized);
		assertTrue(Arrays.equals(deserialized.ciphertext, obfTag.ciphertext));
		assertTrue(Arrays.equals(deserialized.signature, obfTag.signature));
		assertTrue(Arrays.equals(serialized, deserialized.serialize()));
	}
	
	@Test
	public void testSignatureValidation() throws InvalidSignatureException {
		// test every single-bit modification (naive test to ensure signature applies to entire tag)
		obfTag.assertValid();
		byte[] serialized = obfTag.serialize();
		byte[] mangled = serialized.clone();
		for(int i = 0; i < 8*serialized.length; i++) {
			int mask = 1 << (i & 7);
			int offset = i/8;
			mangled[offset] ^= mask;
			ObfuscatedRefTag tag = new ObfuscatedRefTag(refTag.archive, mangled);
			assertFalse(tag.verify());
			mangled[offset] ^= mask;
		}
	}
	
	@Test
	public void testObfuscation() {
		int matchedBits = 0;
		for(int i = 0; i < refTag.getBytes().length; i++) {
			byte b = (byte) (refTag.getBytes()[i] ^ obfTag.ciphertext[i]);
			for(int j = 0; j < 8; j++) {
				if((b & (1 << j)) == 0) {
					matchedBits++;
				}
			}
		}
		
		int deviation = Math.abs(matchedBits - (int) (0.5*8*refTag.getBytes().length));
		assertTrue(deviation < 48);
	}
	
	@Test
	public void testAssertValidThrowsException() throws InvalidSignatureException {
		obfTag.assertValid();
		obfTag.ciphertext[0] ^= 1;
		try {
			obfTag.assertValid();
			fail();
		} catch(InvalidSignatureException exc) {
		}
	}
	
	@Test
	public void testRevealRecoversRefTag() throws InvalidSignatureException {
		RefTag revealed = obfTag.reveal();
		assertTrue(Arrays.equals(revealed.getBytes(), refTag.getBytes()));
	}
	
	@Test
	public void testRevealThrowsExceptionWithInvalidSig() {
		obfTag.ciphertext[1] ^= 3;
		try {
			obfTag.reveal();
			fail();
		} catch(InvalidSignatureException exc) {
		}
	}
	
	@Test
	public void testEquals() {
		ObfuscatedRefTag obfTag2 = new ObfuscatedRefTag(refTag);
		assertTrue(obfTag.equals(obfTag2));
		obfTag2.ciphertext[0] ^= 1;
		assertFalse(obfTag.equals(obfTag2));
		obfTag2.ciphertext[0] ^= 1;
		assertTrue(obfTag.equals(obfTag2));
		obfTag2.signature[0] ^= 1;
		assertFalse(obfTag.equals(obfTag2));
	}
	
	@Test
	public void testCompareTo() {
		ObfuscatedRefTag obfTag2 = new ObfuscatedRefTag(refTag);
		assertEquals(obfTag.compareTo(obfTag2), 0);
		obfTag.ciphertext[0] = 1;
		obfTag2.ciphertext[0] = 0;
		assertTrue(obfTag.compareTo(obfTag2) > 0);
		assertTrue(obfTag2.compareTo(obfTag) < 0);
	}
}
