package com.acrescrypto.zksync.net.noise;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.utility.Util;

public class SymmetricStateTest {
	CryptoSupport crypto;
	SymmetricState symState;
	
	@Before
	public void beforeEach() {
		crypto = new CryptoSupport();
		symState = new SymmetricState(crypto, "Noise_Dummy_WhateverWeHappenToBeUsing");
	}
	
	@Test
	public void testInitializesHashWithProtocolNameIfProtocolNameEqualsHashLength() {
		String name = new String(new char[crypto.hashLength()]).replace('\0', 'X');
		symState = new SymmetricState(crypto, name);
		assertArrayEquals(name.getBytes(), symState.getHandshakeHash());
	}
	
	@Test
	public void testInitializesHashWithNullPaddedNameIfProtocolNameLessThanHashLength() {
		String protocolName = "Noise_Some_ProtocolName";
		byte[] padded = new byte[crypto.hashLength()];
		System.arraycopy(protocolName.getBytes(), 0, padded, 0, protocolName.getBytes().length);
		
		symState = new SymmetricState(crypto, protocolName);
		assertArrayEquals(padded, symState.getHandshakeHash());
	}
	
	@Test
	public void testInitializesHashWithHashOfProtocolNameIfProtocolNameExceedsHashLength() {
		String name = new String(new char[crypto.hashLength() + 1]).replace('\0', 'X');
		byte[] hash = crypto.hash(name.getBytes());
		symState = new SymmetricState(crypto, name);
		assertArrayEquals(hash, symState.getHandshakeHash());
	}
	
	@Test
	public void testInitializeCreatesEmptyCipherState() {
		assertFalse(symState.cipherState.hasKey());
	}
	
	@Test
	public void testMixKeyUpdatesChainingKeyWithHKDF() {
		byte[] hkdf = crypto.expand(new byte[1],
				2*crypto.hashLength(),
				symState.chainingKey.getRaw(),
				new byte[0]);
		byte[] out1 = new byte[crypto.hashLength()], out2 = new byte[crypto.hashLength()];
		System.arraycopy(hkdf, 0, out1, 0, out1.length);
		System.arraycopy(hkdf, crypto.hashLength(), out2, 0, out2.length);
		
		symState.mixKey(new byte[1]);
		assertArrayEquals(out1, symState.chainingKey.getRaw());
	}
	
	@Test
	public void testMixKeyUpdatesCipherKeyWithHKDF() {
		byte[] hkdf = crypto.expand(new byte[1],
				2*crypto.hashLength(),
				symState.chainingKey.getRaw(),
				new byte[0]);
		byte[] out1 = new byte[crypto.hashLength()], out2 = new byte[crypto.symKeyLength()];
		System.arraycopy(hkdf, 0, out1, 0, out1.length);
		System.arraycopy(hkdf, crypto.hashLength(), out2, 0, out2.length);
		
		symState.mixKey(new byte[1]);
		assertArrayEquals(out2, symState.cipherState.key.getRaw());
	}

	@Test
	public void testMixHashUpdatesHashAppropriately() {
		byte[] input = Util.serializeLong(1234L);
		byte[] expected = crypto.hash(Util.concat(symState.getHandshakeHash(), input));
		symState.mixHash(input);
		
		assertArrayEquals(expected, symState.getHandshakeHash());
	}
	
	@Test
	public void testMixKeyAndHashUpdatesChainingKeyFromHkdf() {
		byte[] input = Util.serializeLong(4321L);
		byte[] hkdf = crypto.expand(input,
				3*crypto.hashLength(),
				symState.chainingKey.getRaw(),
				new byte[0]);
		byte[] out1 = new byte[crypto.hashLength()];
		System.arraycopy(hkdf, 0*crypto.hashLength(), out1, 0, out1.length);
		
		symState.mixKeyAndHash(input);
		
		assertArrayEquals(out1, symState.chainingKey.getRaw());
	}
	
	@Test
	public void testMixKeyAndHashUpdatesHashFromHkdf() {
		byte[] input = Util.serializeLong(4321L);
		byte[] hkdf = crypto.expand(input,
				3*crypto.hashLength(),
				symState.chainingKey.getRaw(),
				new byte[0]);
		byte[] out2 = new byte[crypto.hashLength()];
		System.arraycopy(hkdf, 1*crypto.hashLength(), out2, 0, out2.length);
		
		byte[] expectedHash = crypto.hash(Util.concat(symState.getHandshakeHash(), out2));
		
		symState.mixKeyAndHash(input);
		
		assertArrayEquals(expectedHash, symState.getHandshakeHash());
	}

	@Test
	public void testMixKeyAndHashUpdatesCipherStateKeyFromHkdf() {
		byte[] input = Util.serializeLong(4321L);
		byte[] hkdf = crypto.expand(input,
				3*crypto.hashLength(),
				symState.chainingKey.getRaw(),
				new byte[0]);
		byte[] out3 = new byte[crypto.symKeyLength()];
		System.arraycopy(hkdf, 2*crypto.hashLength(), out3, 0, out3.length);
		
		symState.mixKeyAndHash(input);
		assertArrayEquals(out3, symState.cipherState.key.getRaw());
	}
	
	@Test
	public void testGetHandshakeHashReturnsHash() {
		symState.hash = crypto.hash(Util.serializeLong(0));
		assertArrayEquals(symState.hash, symState.getHandshakeHash());
	}
	
	@Test
	public void testEncryptAndHashReturnsPlaintextBeforeKeySet() {
		byte[] plaintext = "i am a happy plaintext".getBytes();
		byte[] ciphertext = symState.encryptAndHash(plaintext);
		assertArrayEquals(plaintext, ciphertext);
	}
	
	@Test
	public void testEncryptAndHashUpdatesHashWithPlaintextBeforeKeySet() {
		byte[] plaintext = "i am a happy plaintext".getBytes();
		byte[] expectedHash = crypto.hash(Util.concat(symState.hash, plaintext));
		symState.encryptAndHash(plaintext);
		assertArrayEquals(expectedHash, symState.getHandshakeHash());
	}
	
	@Test
	public void testEncryptAndHashReturnsCiphertextWhenKeySet() {
		symState.mixKey(Util.serializeInt(0));
		byte[] plaintext = "i am a happy plaintext".getBytes();
		byte[] expectedCiphertext = symState.cipherState.key.encrypt(
				Util.serializeLong(0),
				symState.getHandshakeHash(),
				plaintext,
				-1);
		byte[] ciphertext = symState.encryptAndHash(plaintext);
		assertArrayEquals(expectedCiphertext, ciphertext);
	}
	
	@Test
	public void testEncryptAndHashUpdatesHashWithCiphertextWhenKeySet() {
		symState.mixKey(Util.serializeInt(0));
		byte[] oldHash = symState.getHandshakeHash().clone();
		byte[] plaintext = "i am a happy plaintext".getBytes();
		byte[] ciphertext = symState.encryptAndHash(plaintext);
		byte[] expectedHash = crypto.hash(Util.concat(oldHash, ciphertext));
		assertArrayEquals(expectedHash, symState.getHandshakeHash());
	}
	
	@Test
	public void testSplitReturnsTwoCipherStates() {
		symState.mixKey(Util.serializeInt(0));
		CipherState[] states = symState.split();
		assertEquals(2, states.length);
		assertNotNull(states[0]);
		assertNotNull(states[1]);
	}
	
	@Test
	public void testSplitReturnsCorrectlyInitializedCipherStates() {
		symState.mixKey(Util.serializeInt(0));

		byte[] hkdf = crypto.expand(new byte[0],
				2*crypto.hashLength(),
				symState.chainingKey.getRaw(),
				new byte[0]);
		byte[][] out = new byte[2][crypto.symKeyLength()];
		System.arraycopy(hkdf, 0, out[0], 0, out[0].length);
		System.arraycopy(hkdf, crypto.hashLength(), out[1], 0, out[1].length);

		CipherState[] states = symState.split();
		for(int i = 0; i < states.length; i++) {
			assertArrayEquals(out[i], states[i].key.getRaw());
		}
	}
}
