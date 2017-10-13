package com.acrescrypto.zksync.crypto;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.zkfs.config.PubConfig;

public class KeyTest {
	CryptoSupport crypto = new CryptoSupport(new PubConfig());
	
	@BeforeClass
	public static void beforeClass() {
		Security.addProvider(new BouncyCastleProvider());
	}

	@Test
	public void testDerive() {
		// TODO: need test vectors validated in another language
	}
	
	@Test
	public void testRandomKey() {
		Key[] keys = { new Key(crypto), new Key(crypto) };
		assertFalse(Arrays.equals(keys[0].getRaw(), keys[1].getRaw()));
	}
	
	@Test
	public void testManualKey() {
		byte[] raw = new byte[crypto.symKeyLength()];
		for(int i = 0; i < raw.length; i++) raw[i] = (byte) i;
		assertTrue(Arrays.equals((new Key(crypto, raw)).getRaw(), raw));
	}
	
	@Test
	public void testEncrypt() {
		// TODO: test encrypt. wait for some 256-bit test vectors tho...
	}
	
	@Test
	public void testDecrypt() {
		// TODO: test decrypt. also needs those test vectors...
	}
	
	@Test
	public void testWrappedEncryptIsStructurallyCorrect() {
		/* This is a monster test, but it's not easy to split up.
		 * This proves:
		 *   - wrappedEncrypt generates random inner and outer IVs (which we test as "not identical from one call to the next")
		 *   - those IVs are appropriately sized
		 *   - random inner keys are generated
		 *   - the inner keys are encrypted correctly using the outer key and outer IV
		 *   - the ciphertext is encrypted with the inner key and inner IV
		 *   - the ciphertext, when decrypted, matches the plaintext
		 */
		Key key = new Key(crypto);
		byte[] plaintext = "how bout them initialization vectors tho".getBytes();
		byte[][] samples = { key.wrappedEncrypt(plaintext, 0), key.wrappedEncrypt(plaintext, 0) };
		
		ByteBuffer[] bufs = { ByteBuffer.wrap(samples[0]), ByteBuffer.wrap(samples[1]) };
		short[] lens = { bufs[0].getShort(), bufs[1].getShort() };
		assertTrue(lens[0] == lens[1]);
		assertTrue(lens[0] == crypto.symIvLength());
		
		byte[][] outerIVs = { new byte[lens[0]], new byte[lens[0]] };
		for(int i = 0; i < outerIVs.length; i++) bufs[i].get(outerIVs[i], 0, outerIVs[i].length);
		assertFalse(Arrays.equals(outerIVs[0], outerIVs[1]));

		byte[][] innerIVs = { new byte[lens[0]], new byte[lens[0]] };
		for(int i = 0; i < innerIVs.length; i++) bufs[i].get(innerIVs[i], 0, innerIVs[i].length);
		assertFalse(Arrays.equals(innerIVs[0], innerIVs[1]));
		assertFalse(Arrays.equals(outerIVs[0], innerIVs[0]));
		assertFalse(Arrays.equals(outerIVs[1], innerIVs[1]));
		
		short[] keyLens = { bufs[0].getShort(), bufs[1].getShort() };
		assertTrue(keyLens[0] == keyLens[1]);
		byte[][] keyWraps = { new byte[keyLens[0]], new byte[keyLens[1]] };
		for(int i = 0; i < keyWraps.length; i++) bufs[i].get(keyWraps[i], 0, keyWraps[i].length);
		byte[][] innerKeys = { null, null };
		for(int i = 0; i < innerKeys.length; i++) innerKeys[i] = key.decrypt(outerIVs[i], keyWraps[i]);
		assertFalse(Arrays.equals(innerKeys[0], innerKeys[1]));
		
		byte[][] ciphertexts = { new byte[bufs[0].remaining()], new byte[bufs[1].remaining()] };
		for(int i = 0; i < ciphertexts.length; i++) bufs[i].get(ciphertexts[i]);

		Key[] subkeys = { new Key(crypto, innerKeys[0]), new Key(crypto, innerKeys[1]) };
		for(int i = 0; i < subkeys.length; i++) assertTrue(Arrays.equals(subkeys[i].decrypt(innerIVs[i], ciphertexts[i]), plaintext));
	}
	
	@Test
	public void testWrappedEncryptIsPadded() {
		Key key = new Key(crypto);
		String plaintext = "some text";
		byte[] unpadded = key.wrappedEncrypt(plaintext.getBytes(), 0);
		byte[] paddedALittle = key.wrappedEncrypt(plaintext.getBytes(), 1024);
		byte[] paddedALot = key.wrappedEncrypt(plaintext.getBytes(), 65536);
		assertTrue((unpadded.length < 128));
		assertTrue((paddedALittle.length >= 1024 && paddedALittle.length < 2048));
		assertTrue((paddedALot.length >= 65536));
	}
	
	@Test(expected=SecurityException.class)
	public void testWrappedEncryptIsAuthenticated() {
		Key key = new Key(crypto);
		byte[] ciphertext = key.wrappedEncrypt("some text".getBytes(), 65536);
		ciphertext[1000] ^= 0x04; // diddle one bit of ciphertext
		key.wrappedDecrypt(ciphertext);
	}
}
