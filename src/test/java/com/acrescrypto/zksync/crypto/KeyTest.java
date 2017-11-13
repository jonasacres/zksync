package com.acrescrypto.zksync.crypto;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
	@Ignore
	public void testDerive() {
		// TODO: need test vectors validated in another language
	}
	
	@Test
	public void testRandomKey() {
		Key[] keys = { new Key(crypto), new Key(crypto) };
		assertFalse(Arrays.equals(keys[0].getRaw(), keys[1].getRaw()));
	}
	
	@Test
	public void testPseudoRandomKey() {
		Key[] keys = { new Key(crypto, crypto.prng(new byte[] { 0x01 })), new Key(crypto, crypto.prng(new byte[] { 0x01 })) };
		assertTrue(Arrays.equals(keys[0].getRaw(), keys[1].getRaw()));

		PRNG rng = crypto.prng(new byte[] { 0x01 });
		keys = new Key[] { new Key(crypto, rng), new Key(crypto, rng) };
		assertFalse(Arrays.equals(keys[0].getRaw(), keys[1].getRaw()));
	}
	
	@Test
	public void testManualKey() {
		byte[] raw = new byte[crypto.symKeyLength()];
		for(int i = 0; i < raw.length; i++) raw[i] = (byte) i;
		assertTrue(Arrays.equals((new Key(crypto, raw)).getRaw(), raw));
	}
	
	@Test
	@Ignore
	public void testEncrypt() {
		// TODO: test encrypt. wait for some 256-bit test vectors tho...
	}
	
	@Test
	@Ignore
	public void testDecrypt() {
		// TODO: test decrypt. also needs those test vectors...
	}
	
	@Test
	public void testWrappedEncryptIsStructurallyCorrect() {
		/* This is a monster test, but it's not easy to split up.
		 * This proves:
		 *   - wrappedEncrypt generates random salt and IV (which we test as "not identical from one call to the next")
		 *   - those values are appropriately sized
		 *   - the ciphertext is encrypted with the salted key and IV
		 *   - the ciphertext, when decrypted, matches the plaintext
		 */
		Key key = new Key(crypto);
		byte[] plaintext = "how bout them initialization vectors tho".getBytes();
		byte[][] samples = { key.wrappedEncrypt(plaintext, 0), key.wrappedEncrypt(plaintext, 0) };
		
		ByteBuffer[] bufs = { ByteBuffer.wrap(samples[0]), ByteBuffer.wrap(samples[1]) };
		short[] saltLens = { bufs[0].getShort(), bufs[1].getShort() };
		assertTrue(saltLens[0] == saltLens[1]);
		assertTrue(saltLens[0] == crypto.symKeyLength());
		
		byte[][] salts = { new byte[saltLens[0]], new byte[saltLens[0]] };
		for(int i = 0; i < salts.length; i++) bufs[i].get(salts[i], 0, salts[i].length);
		assertFalse(Arrays.equals(salts[0], salts[1]));

		short[] ivLens = { bufs[0].getShort(), bufs[1].getShort() };
		assertTrue(ivLens[0] == ivLens[1]);
		assertTrue(ivLens[0] == crypto.symIvLength());

		byte[][] ivs = { new byte[ivLens[0]], new byte[ivLens[0]] };
		for(int i = 0; i < ivs.length; i++) bufs[i].get(ivs[i], 0, ivs[i].length);
		assertFalse(Arrays.equals(ivs[0], ivs[1]));
		assertFalse(Arrays.equals(salts[0], ivs[0]));
		assertFalse(Arrays.equals(salts[1], ivs[1]));
				
		byte[][] ciphertexts = { new byte[bufs[0].remaining()], new byte[bufs[1].remaining()] };
		for(int i = 0; i < ciphertexts.length; i++) bufs[i].get(ciphertexts[i]);

		Key[] subkeys = { key.derive(Key.KEY_INDEX_SALTED_SUBKEY, salts[0]), key.derive(Key.KEY_INDEX_SALTED_SUBKEY, salts[1]) };
		for(int i = 0; i < subkeys.length; i++) assertTrue(Arrays.equals(subkeys[i].decrypt(ivs[i], ciphertexts[i]), plaintext));
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
	
	@Test
	public void testWrappedEncryptAcceptsRNG() {
		Key key = new Key(crypto);
		byte[] ct1 = key.wrappedEncrypt("some text".getBytes(), 0, new PRNG(new byte[] {0x01}));
		byte[] ct2 = key.wrappedEncrypt("some text".getBytes(), 0, new PRNG(new byte[] {0x01}));
		assertTrue(Arrays.equals(ct1, ct2));
	}
	
	@Test
	public void testWrappedEncryptIsRandomByDefault() {
		Key key = new Key(crypto);
		byte[] ct1 = key.wrappedEncrypt("some text".getBytes(), 0);
		byte[] ct2 = key.wrappedEncrypt("some text".getBytes(), 0);
		assertFalse(Arrays.equals(ct1, ct2));
	}
}
