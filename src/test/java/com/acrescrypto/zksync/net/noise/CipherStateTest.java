package com.acrescrypto.zksync.net.noise;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.utility.Util;

public class CipherStateTest {
	CryptoSupport crypto;
	CipherState ct;
	
	@Before
	public void beforeEach() {
		crypto = new CryptoSupport();
		ct = new CipherState();
	}
	
	@Test
	public void testInitializeKeySetsKey() {
		Key key = new Key(crypto);
		ct.initializeKey(key);
		assertEquals(key, ct.key);
	}
	
	@Test
	public void testInitializeKeyDestroysOldKey() {
		Key key1 = new Key(crypto), key2 = new Key(crypto);
		ct.initializeKey(key1);
		ct.initializeKey(key2);
		assertEquals(key2, ct.key);
		assertNull(key1.getRaw());
	}
	
	@Test
	public void testInitializeKeyResetsNonce() {
		ct.setNonce(1234);
		ct.initializeKey(new Key(crypto));
		assertEquals(0, ct.getNonce());
	}
	
	@Test
	public void testHasKeyReturnsFalseIfKeyEmpty() {
		assertFalse(ct.hasKey());
	}
	
	@Test
	public void testHasKeyReturnsTrueIfKeyNonEmpty() {
		ct.initializeKey(new Key(crypto));
		assertTrue(ct.hasKey());
	}
	
	@Test
	public void testSetNonceOverwritesNonce() {
		ct.setNonce(0);
		assertEquals(0, ct.getNonce());
		
		ct.setNonce(1234567890);
		assertEquals(1234567890, ct.getNonce());
	}
	
	@Test
	public void testEncryptWithAssociatedDataReturnsPlaintextIfKeyEmpty() {
		byte[] plaintext = "i'm a lumberjack and i'm ok".getBytes();
		byte[] ad = "associated data".getBytes();
		assertArrayEquals(plaintext, ct.encryptWithAssociatedData(ad, plaintext));
	}
	
	@Test
	public void testEncryptWithAssociatedDataDoesNotIncrementNonceIfKeyEmpty() {
		byte[] plaintext = "i'm a lumberjack and i'm ok".getBytes();
		byte[] ad = "associated data".getBytes();
		ct.encryptWithAssociatedData(ad, plaintext);
		assertEquals(0, ct.getNonce());
	}
	
	@Test
	public void testEncryptWithAssociatedDataReturnsEncryptedDataIfKeyNotEmpty() {
		Key key = new Key(crypto);
		byte[] plaintext = "i'm a lumberjack and i'm ok".getBytes();
		byte[] ad = "associated data".getBytes();
		
		ct.initializeKey(key);
		byte[] ciphertext = ct.encryptWithAssociatedData(ad, plaintext);
		byte[] expected = key.encrypt(Util.serializeLong(0),
				ad,
				plaintext,
				-1);
		
		assertArrayEquals(expected, ciphertext);
	}

	@Test
	public void testEncryptWithAssociatedDataIncrementsNonceIfKeyNotEmpty() {
		Key key = new Key(crypto);
		byte[] plaintext = "i'm a lumberjack and i'm ok".getBytes();
		byte[] ad = "associated data".getBytes();
		
		ct.initializeKey(key);
		ct.encryptWithAssociatedData(ad, plaintext);
		assertEquals(1, ct.getNonce());
	}
	
	@Test
	public void testEncryptWithAssociatedDataTriggersAutoRekeyAfterIntervalWhenIntervalSet() {
		Key key = new Key(crypto);
		byte[] originalKey = key.getRaw().clone();
		byte[] plaintext = "i'm a lumberjack and i'm ok".getBytes();
		byte[] ad = "associated data".getBytes();
		
		ct.initializeKey(key);
		ct.setRekeyInterval(3);
		for(int i = 0; i < ct.getRekeyInterval(); i++) {
			assertArrayEquals(originalKey, key.getRaw());
			ct.encryptWithAssociatedData(ad, plaintext);
		}
		
		assertFalse(Arrays.equals(originalKey, key.getRaw()));
	}
	
	@Test
	public void testEncryptWithAssociatedDataDoesNotTriggerAutoRekeyIfNoIntervalSet() {
		Key key = new Key(crypto);
		byte[] originalKey = key.getRaw().clone();
		byte[] plaintext = "i'm a lumberjack and i'm ok".getBytes();
		byte[] ad = "associated data".getBytes();
		
		ct.initializeKey(key);
		for(int i = 0; i < 1000; i++) {
			ct.encryptWithAssociatedData(ad, plaintext);
		}
		
		assertArrayEquals(originalKey, key.getRaw());
	}
	
	@Test
	public void testDecryptWithAssociatedDataReturnsCiphertextIfKeyEmpty() {
		Key key = new Key(crypto);
		byte[] plaintext = "some plaintext".getBytes();
		byte[] ad = "some data".getBytes();
		byte[] ciphertext = key.encrypt(Util.serializeLong(0),
				ad,
				plaintext,
				-1);
		
		byte[] decrypted = ct.decryptWithAssociatedData(ad, ciphertext);
		assertArrayEquals(ciphertext, decrypted);
	}
	
	@Test
	public void testDecryptWithAssociatedDataDoesNotIncrementNonceIfKeyEmpty() {
		Key key = new Key(crypto);
		byte[] plaintext = "some plaintext".getBytes();
		byte[] ad = "some data".getBytes();
		byte[] ciphertext = key.encrypt(Util.serializeLong(0),
				ad,
				plaintext,
				-1);
		
		ct.decryptWithAssociatedData(ad, ciphertext);
		assertEquals(0, ct.getNonce());
	}
	
	@Test
	public void testDecryptWithAssociatedDataReturnsDecryptedDataIfKeyNonEmpty() {
		Key key = new Key(crypto);
		ct.initializeKey(key);
		
		byte[] plaintext = "some plaintext".getBytes();
		byte[] ad = "some data".getBytes();
		byte[] ciphertext = key.encrypt(Util.serializeLong(0),
				ad,
				plaintext,
				-1);
		
		byte[] decrypted = ct.decryptWithAssociatedData(ad, ciphertext);
		assertArrayEquals(plaintext, decrypted);
	}
	
	@Test(expected=SecurityException.class)
	public void testDecryptWithAssociatedDataValidatesAssociatedData() {
		Key key = new Key(crypto);
		ct.initializeKey(key);
		
		byte[] plaintext = "some plaintext".getBytes();
		byte[] ad = "some data".getBytes();
		byte[] ciphertext = key.encrypt(Util.serializeLong(0),
				ad,
				plaintext,
				-1);
		
		// throws SecurityException due to invalid associated data
		ct.decryptWithAssociatedData(new byte[0], ciphertext);
	}
	
	@Test(expected=SecurityException.class)
	public void testDecryptWithAssociatedDataValidatesTagIntegrity() {
		Key key = new Key(crypto);
		ct.initializeKey(key);
		
		byte[] plaintext = "some plaintext".getBytes();
		byte[] ad = "some data".getBytes();
		byte[] ciphertext = key.encrypt(Util.serializeLong(0),
				ad,
				plaintext,
				-1);
		
		ciphertext[ciphertext.length-1] ^= 0x80;
		// throws SecurityException due to invalid tag
		ct.decryptWithAssociatedData(ad, ciphertext);
	}
	
	@Test
	public void testDecryptWithAssociatedDataIncrementsNonceIfKeyNonEmpty() {
		Key key = new Key(crypto);
		ct.initializeKey(key);
		
		byte[] plaintext = "some plaintext".getBytes();
		byte[] ad = "some data".getBytes();
		byte[] ciphertext = key.encrypt(Util.serializeLong(0),
				ad,
				plaintext,
				-1);
		
		// throws SecurityException due to invalid tag
		ct.decryptWithAssociatedData(ad, ciphertext);
		assertEquals(1, ct.getNonce());
	}
	
	@Test
	public void testDecryptWithAssociatedDataTriggersAutoRekeyAfterIntervalWhenIntervalSet() {
		Key key = new Key(crypto);
		byte[] origKey = key.getRaw().clone();
		ct.initializeKey(key);
		ct.setRekeyInterval(4);
		
		byte[] plaintext = "some plaintext".getBytes();
		byte[] ad = "some data".getBytes();
		
		for(int i = 0; i < ct.getRekeyInterval(); i++) {
			assertArrayEquals(origKey, key.getRaw());
			byte[] ciphertext = key.encrypt(Util.serializeLong(i),
					ad,
					plaintext,
					-1);
			try {
				ct.decryptWithAssociatedData(ad, ciphertext);
				assertTrue(i < ct.getRekeyInterval()-1);
			} catch(SecurityException exc) {
				assertEquals(i, ct.getRekeyInterval()-1);
			}
		}
		
		assertFalse(Arrays.equals(origKey, key.getRaw()));
	}
	
	@Test
	public void testRekeySetsNewKeyAppropriately() {
		Key key = new Key(crypto);
		byte[] derived = key.encrypt(Util.serializeLong(0xffffffffffffffffL),
				new byte[0],
				new byte[key.getRaw().length],
				-1);
		byte[] truncated = new byte[key.getRaw().length];
		System.arraycopy(derived, 0, truncated, 0, truncated.length);
		
		ct.initializeKey(key);
		ct.rekey();
		
		assertArrayEquals(truncated, key.getRaw());
	}
	
	@Test
	public void testEncryptMatchesDecrypt() {
		Key key = new Key(crypto);
		CipherState ct2 = new CipherState();
		
		ct.initializeKey(key);
		ct2.initializeKey(new Key(crypto, key.getRaw().clone()));
		
		ct.setRekeyInterval(16);
		ct2.setRekeyInterval(16);
		
		for(int i = 0; i < 1024; i++) {
			byte[] plaintext = Util.serializeLong(i);
			byte[] ad = Util.serializeInt(-i);
			
			byte[] ciphertext = ct.encryptWithAssociatedData(ad, plaintext);
			byte[] decrypted = ct2.decryptWithAssociatedData(ad, ciphertext);
			
			assertArrayEquals(plaintext, decrypted);
		}
	}
}
