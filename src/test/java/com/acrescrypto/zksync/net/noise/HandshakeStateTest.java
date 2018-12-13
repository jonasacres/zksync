package com.acrescrypto.zksync.net.noise;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.utility.Util;

public class HandshakeStateTest {
	HandshakeState handshakeState;
	CryptoSupport crypto;
	
	PrivateDHKey localEphKey, localStaticKey;
	PublicDHKey remoteEphKey, remoteStaticKey;
	
	boolean isInitiator;
	String protocolName;
	String messagePatterns;
	byte[] prologue, psk;
	byte[] expectedHash;
	ByteBuffer buf = ByteBuffer.allocate(65536+16);
	
	OutputStream bufOut = new OutputStream() {
		@Override
		public void write(int b) { buf.put((byte) b); }
	};
	
	@Before
	public void beforeEach() {
		crypto = CryptoSupport.defaultCrypto();
		
		localEphKey = new PrivateDHKey(crypto);
		localStaticKey = new PrivateDHKey(crypto);
		remoteEphKey = new PrivateDHKey(crypto).publicKey();
		remoteStaticKey = new PrivateDHKey(crypto).publicKey();
		
		isInitiator = true;
		prologue = "In the beginning".getBytes();
		psk = crypto.hash("shhh".getBytes());
		protocolName = "Noise_XX_AndHeresSomeCryptoStuff";
		messagePatterns = NoiseHandshakes.XX;
		
		remakeHandshakeState();
	}
	
	void remakeHandshakeState() {
		handshakeState = makeHandshakeState();
		
		byte[] padded = new byte[crypto.hashLength()];
		System.arraycopy(protocolName.getBytes(), 0, padded, 0, protocolName.length());
		expectedHash = crypto.hash(Util.concat(padded, prologue == null ? new byte[0] : prologue));
	}
	
	void updateHash(byte[] data) {
		expectedHash = crypto.hash(Util.concat(expectedHash, data));
	}
	
	public byte[] readBuf() {
		byte[] trimBuf = new byte[buf.position()];
		System.arraycopy(buf.array(), 0, trimBuf, 0, trimBuf.length);
		return trimBuf;
	}

	HandshakeState makeHandshakeState() {
		return new HandshakeState(crypto,
				protocolName,
				messagePatterns,
				isInitiator,
				prologue,
				localStaticKey,
				localEphKey,
				remoteStaticKey,
				remoteEphKey,
				psk
				);
	}
	
	@Test
	public void testInitializeSetIsInitiatorTrueWhenRequested() {
		isInitiator = true;
		remakeHandshakeState();
		assertTrue(handshakeState.isInitiator());
	}
	
	@Test
	public void testInitializeSetIsInitiatorFalseWhenRequested() {
		isInitiator = false;
		remakeHandshakeState();
		assertFalse(handshakeState.isInitiator());
	}
	
	@Test
	public void testInitializeCallsMixHashWithPrologue() {
		assertArrayEquals(expectedHash, handshakeState.symmetricState.getHandshakeHash());
	}
	
	@Test
	public void testInitializeTreatsNullPrologueAsZeroLengthArray() {
		prologue = null;
		remakeHandshakeState();
		byte[] padded = new byte[crypto.hashLength()];
		System.arraycopy(protocolName.getBytes(), 0, padded, 0, protocolName.length());
		byte[] expectedHash = crypto.hash(Util.concat(padded, new byte[0]));
		assertArrayEquals(expectedHash, handshakeState.symmetricState.getHandshakeHash());
	}
	
	@Test
	public void testInitializeSetsProtocolName() {
		assertEquals(protocolName, handshakeState.getProtocolName());
	}
	
	@Test
	public void testInitializeCallsMixHashForPremessageLocalEphemeralKeyWhenInitiator() {
		messagePatterns = "Example:\n"
			+ "  -> e\n"
			+ "  ...\n";
		remakeHandshakeState();
		
		updateHash(localEphKey.publicKey().getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testInitializeCallsMixHashForPremessageLocalStaticKeyWhenInitiator() {
		messagePatterns = "Example:\n"
			+ "  -> s\n"
			+ "  ...\n";
		remakeHandshakeState();
		
		updateHash(localStaticKey.publicKey().getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testInitializeCallsMixHashForPremessageRemoteEphemeralKeyWhenInitiator() {
		messagePatterns = "Example:\n"
			+ "  <- e\n"
			+ "  ...\n";
		remakeHandshakeState();
		
		updateHash(remoteEphKey.getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}

	@Test
	public void testInitializeCallsMixHashForPremessageRemoteStaticKeyWhenInitiator() {
		messagePatterns = "Example:\n"
			+ "  <- s\n"
			+ "  ...\n";
		remakeHandshakeState();
		
		updateHash(remoteStaticKey.getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testInitializeCallsMixHashForMultiplePremessageKeysWhenInitiator() {
		messagePatterns = "Example:\n"
			+ "  -> e, s\n"
			+ "  <- e, s\n"
			+ "  ...\n";
		remakeHandshakeState();
		
		updateHash(localEphKey.publicKey().getBytes());
		updateHash(localStaticKey.publicKey().getBytes());
		updateHash(remoteEphKey.getBytes());
		updateHash(remoteStaticKey.getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}

	@Test
	public void testInitializeCallsMixHashForPremessageLocalEphemeralKeyWhenResponder() {
		isInitiator = false;
		messagePatterns = "Example:\n"
			+ "  <- e\n"
			+ "  ...\n";
		remakeHandshakeState();
		
		updateHash(localEphKey.publicKey().getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testInitializeCallsMixHashForPremessageLocalStaticKeyWhenResponder() {
		isInitiator = false;
		messagePatterns = "Example:\n"
			+ "  <- s\n"
			+ "  ...\n";
		remakeHandshakeState();
		
		updateHash(localStaticKey.publicKey().getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testInitializeCallsMixHashForPremessageRemoteEphemeralKeyWhenResponder() {
		isInitiator = false;
		messagePatterns = "Example:\n"
			+ "  -> e\n"
			+ "  ...\n";
		remakeHandshakeState();
		
		updateHash(remoteEphKey.getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}

	@Test
	public void testInitializeCallsMixHashForPremessageRemoteStaticKeyWhenResponder() {
		isInitiator = false;
		messagePatterns = "Example:\n"
			+ "  -> s\n"
			+ "  ...\n";
		remakeHandshakeState();
		
		updateHash(remoteStaticKey.getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testInitializeCallsMixHashForMultiplePremessageKeysWhenResponder() {
		isInitiator = false;
		messagePatterns = "Example:\n"
			+ "  -> e, s\n"
			+ "  <- e, s\n"
			+ "  ...\n";
		remakeHandshakeState();
		
		updateHash(remoteEphKey.getBytes());
		updateHash(remoteStaticKey.getBytes());
		updateHash(localEphKey.publicKey().getBytes());
		updateHash(localStaticKey.publicKey().getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testInitializeSetsMessagePatternsWithNonPremessageMessagePatternsWhenPremessagePresent() {
		isInitiator = false;
		messagePatterns = "KK:\n"
			+ "  -> s\n"
			+ "  <- s\n"
			+ "  ...\n"
			+ "  -> e, es, ss\n"
			+ "  <- e, ee, se";
		remakeHandshakeState();
		assertEquals(2, handshakeState.messagePatterns.size());
		assertEquals("-> e es ss", String.join(" ", handshakeState.messagePatterns.poll()));
		assertEquals("<- e ee se", String.join(" ", handshakeState.messagePatterns.poll()));
	}
	
	@Test
	public void testInitializeSetsMessagePatternsWithNonPremessageMessagePatternsWhenPremessageNotPresent() {
		isInitiator = false;
		messagePatterns = "IN:\n"
			+ "  -> e, s\n"
			+ "  <- e, ee, se";
		remakeHandshakeState();
		assertEquals(2, handshakeState.messagePatterns.size());
		assertEquals("-> e s", String.join(" ", handshakeState.messagePatterns.poll()));
		assertEquals("<- e ee se", String.join(" ", handshakeState.messagePatterns.poll()));
	}
	
	@Test
	public void testWriteMessageDequeuesNextMessagePattern() throws IOException {
		int count = handshakeState.messagePatterns.size();
		handshakeState.writeMessage(bufOut);
		assertEquals(count-1, handshakeState.messagePatterns.size());
	}
	
	@Test
	public void testWriteMessageProcesses_e_TokenAppropriately() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> e\n";
		handshakeState.localEphemeralKey = null; // ensure that we're regenerating this
		handshakeState.writeMessage(bufOut);
		
		assertArrayEquals(handshakeState.localEphemeralKey.publicKey().getBytes(), readBuf());
		
		updateHash(handshakeState.localEphemeralKey.publicKey().getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testWriteMessageProcesses_e_TokenUsingSuppliedObfuscator() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> e";
		handshakeState.setObfuscation((key)->{
			assertEquals(handshakeState.localEphemeralKey.publicKey(), key);
			return Util.serializeLong(1234);
		}, (serialized)->null);
		handshakeState.writeMessage(bufOut);
		
		assertArrayEquals(Util.serializeLong(1234), readBuf());

		updateHash(Util.serializeLong(1234));
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}

	@Test
	public void testWriteMessageProcesses_s_TokenAppropriately() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> s\n";
		remakeHandshakeState();

		CipherState clone = new CipherState(handshakeState.symmetricState.cipherState);
		byte[] expectedCiphertext = clone.encryptWithAssociatedData(handshakeState.symmetricState.hash,
				handshakeState.localStaticKey.publicKey().getBytes());
		updateHash(expectedCiphertext);

		handshakeState.writeMessage(bufOut);
		assertArrayEquals(expectedCiphertext, readBuf());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testWriteMessageProcesses_s_TokenWhenObfuscatorSet() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> s\n";
		remakeHandshakeState();
		
		byte[] obfuscation = Util.serializeLong(1234);
		handshakeState.setObfuscation((key)->obfuscation, (in)->null);
		updateHash(obfuscation);

		handshakeState.writeMessage(bufOut);
		assertArrayEquals(obfuscation, readBuf());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testWriteMessageProcesses_s_TokenWithoutObfuscatorIfSymKeySet() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> s\n";
		remakeHandshakeState();

		handshakeState.symmetricState.cipherState.key = new Key(crypto);
		CipherState clone = new CipherState(handshakeState.symmetricState.cipherState);
		byte[] obfuscation = Util.serializeLong(1234);
		handshakeState.setObfuscation((key)->obfuscation, (in)->null);
		byte[] expectedCiphertext = clone.encryptWithAssociatedData(handshakeState.symmetricState.hash,
				handshakeState.localStaticKey.publicKey().getBytes());
		updateHash(expectedCiphertext);

		handshakeState.writeMessage(bufOut);
		assertArrayEquals(expectedCiphertext, readBuf());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testWriteMessageProcesses_ee_TokenAppropriately() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> ee\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKey(localEphKey.sharedSecret(remoteEphKey));
		handshakeState.writeMessage(bufOut);
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
		assertArrayEquals(new byte[0], readBuf());
	}
	
	@Test
	public void testWriteMessageProcesses_es_TokenAppropriatelyAsInitiator() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> es\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKey(localEphKey.sharedSecret(remoteStaticKey));
		handshakeState.writeMessage(bufOut);
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
		assertArrayEquals(new byte[0], readBuf());
	}

	@Test
	public void testWriteMessageProcesses_es_TokenAppropriatelyAsResponder() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> es\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		isInitiator = false;
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKey(localStaticKey.sharedSecret(remoteEphKey));
		handshakeState.writeMessage(bufOut);
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
		assertArrayEquals(new byte[0], readBuf());
	}

	@Test
	public void testWriteMessageProcesses_se_TokenAppropriatelyAsInitiator() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> se\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKey(localStaticKey.sharedSecret(remoteEphKey));
		handshakeState.writeMessage(bufOut);
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
		assertArrayEquals(new byte[0], readBuf());
	}

	@Test
	public void testWriteMessageProcesses_se_TokenAppropriatelyAsResponder() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> se\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		isInitiator = false;
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKey(localEphKey.sharedSecret(remoteStaticKey));
		handshakeState.writeMessage(bufOut);
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
		assertArrayEquals(new byte[0], readBuf());
	}
	
	@Test
	public void testWriteMessageProcesses_ss_TokenAppropriatelyAsResponder() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> ss\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		isInitiator = false;
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKey(localStaticKey.sharedSecret(remoteStaticKey));
		handshakeState.writeMessage(bufOut);
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
		assertArrayEquals(new byte[0], readBuf());
	}
	
	@Test
	public void testWriteMessageProcesses_psk_TokenAppropriately() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> psk\n"
				+ "  <- ss"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKeyAndHash(psk);
		handshakeState.writeMessage(bufOut);
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
		assertArrayEquals(new byte[0], readBuf());
	}
	
	@Test
	public void testWriteMessageAppendsEncryptedPayload() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> psk\n"
				+ "  <- ss"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		byte[] plaintext = Util.serializeLong(4321);
		handshakeState.setPayload((round)->plaintext, (round, in, decrypter)->{});
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKeyAndHash(psk);
		byte[] ciphertext = clone.encryptAndHash(plaintext);
		handshakeState.writeMessage(bufOut);
		
		assertEquals(1, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
		assertArrayEquals(ciphertext, readBuf());
	}
	
	@Test
	public void testWriteMessageReturnsNullIfRemainingMessagePatterns() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> psk\n"
				+ "  <- ss"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		assertNull(handshakeState.writeMessage(bufOut));
	}
	
	@Test
	public void testWriteMessageReturnsCipherStatesIfNoRemainingMessagePatternsWhenInitiator() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> foo\n"; // fake token gets dropped, symmetric state remains unchanged
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		CipherState[] states = handshakeState.writeMessage(bufOut);
		CipherState[] expectedStates = clone.split();
		
		assertEquals(2, states.length);
		assertEquals(expectedStates[0], states[0]);
		assertEquals(expectedStates[1], states[1]);
	}
	
	@Test
	public void testWriteMessageReturnsCipherStatesIfNoRemainingMessagePatternsWhenResponder() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> foo\n"; // fake token gets dropped, symmetric state remains unchanged
		isInitiator = false;
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		CipherState[] states = handshakeState.writeMessage(bufOut);
		CipherState[] expectedStates = clone.split();
		
		// note that the states come in inverted order compared to the initiator
		assertEquals(2, states.length);
		assertEquals(expectedStates[1], states[0]);
		assertEquals(expectedStates[0], states[1]);
	}
	
	@Test
	public void testReadMessageDequeuesNextMessagePattern() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> psk\n"
				+ "  <- psk";
		remakeHandshakeState();

		int count = handshakeState.messagePatterns.size();
		handshakeState.readMessage(new ByteArrayInputStream(new byte[0]));
		assertEquals(count-1, handshakeState.messagePatterns.size());
	}
	
	@Test
	public void testReadMessageProcesses_e_TokenAppropriately() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> e\n"
				+ "  <- psk";
		remoteEphKey = null;
		remakeHandshakeState();
		
		PublicDHKey re = new PrivateDHKey(crypto).publicKey();
		updateHash(re.getBytes());
		
		handshakeState.readMessage(new ByteArrayInputStream(re.getBytes()));
		assertArrayEquals(re.getBytes(), handshakeState.remoteEphemeralKey.getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testReadMessageProcesses_e_TokenAppropriatelyWithDeobfuscatorPresent() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> e\n"
				+ "  <- psk";
		remoteEphKey = null;
		remakeHandshakeState();
		
		PublicDHKey re = new PrivateDHKey(crypto).publicKey();
		byte[] obfuscated = Util.serializeLong(1234L);
		handshakeState.setObfuscation((key)->null, (in)->new byte[][] { re.getBytes(), obfuscated });
		updateHash(obfuscated);
		
		handshakeState.readMessage(new ByteArrayInputStream(new byte[0]));
		assertArrayEquals(re.getBytes(), handshakeState.remoteEphemeralKey.getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testReadMessageProcesses_s_TokenAppropriately() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> s\n"
				+ "  <- psk";
		remoteStaticKey = null;
		remakeHandshakeState();
		
		PublicDHKey rs = new PrivateDHKey(crypto).publicKey();
		updateHash(rs.getBytes());
		
		handshakeState.readMessage(new ByteArrayInputStream(rs.getBytes()));
		assertArrayEquals(rs.getBytes(), handshakeState.remoteStaticKey.getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testReadMessageProcesses_s_TokenAppropriatelyWhenDeobfuscatorSet() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> s\n"
				+ "  <- psk";
		remoteStaticKey = null;
		remakeHandshakeState();
		
		PublicDHKey rs = new PrivateDHKey(crypto).publicKey();
		byte[] serialization = Util.serializeLong(1234);
		handshakeState.setObfuscation((key)->null, (in)->{
			return new byte[][] { rs.getBytes(), serialization };
		});
		updateHash(serialization);
		
		handshakeState.readMessage(new ByteArrayInputStream(serialization));
		assertArrayEquals(rs.getBytes(), handshakeState.remoteStaticKey.getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testReadMessageProcesses_s_TokenWithoutObfuscatorIfSymKeySet() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> s\n"
				+ "  <- psk";
		remoteStaticKey = null;
		remakeHandshakeState();
		
		PublicDHKey rs = new PrivateDHKey(crypto).publicKey();
		Key key = new Key(crypto);
		handshakeState.symmetricState.cipherState.key = key;
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		byte[] ciphertext = clone.encryptAndHash(rs.getBytes());
		updateHash(ciphertext);
		
		byte[] serialization = Util.serializeLong(1234);
		handshakeState.setObfuscation((k)->null, (in)->{
			return new byte[][] { rs.getBytes(), serialization };
		});
		
		handshakeState.readMessage(new ByteArrayInputStream(ciphertext));
		assertArrayEquals(rs.getBytes(), handshakeState.remoteStaticKey.getBytes());
		assertArrayEquals(expectedHash, handshakeState.symmetricState.hash);
	}
	
	@Test
	public void testReadMessageProcesses_ee_TokenAppropriately() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> ee\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKey(localEphKey.sharedSecret(remoteEphKey));
		handshakeState.readMessage(new ByteArrayInputStream(new byte[0]));
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
	}
	
	@Test
	public void testReadMessageProcesses_es_TokenAppropriatelyWhenInitiator() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> es\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKey(localEphKey.sharedSecret(remoteStaticKey));
		handshakeState.readMessage(new ByteArrayInputStream(new byte[0]));
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
	}

	@Test
	public void testReadMessageProcesses_es_TokenAppropriatelyWhenResponder() throws IOException {
		isInitiator = false;
		messagePatterns = "Example:\n"
				+ "  -> es\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKey(localStaticKey.sharedSecret(remoteEphKey));
		handshakeState.readMessage(new ByteArrayInputStream(new byte[0]));
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
	}

	public void testReadMessageProcesses_se_TokenAppropriatelyWhenInitiator() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> se\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKey(localStaticKey.sharedSecret(remoteEphKey));
		handshakeState.readMessage(new ByteArrayInputStream(new byte[0]));
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
	}

	@Test
	public void testReadMessageProcesses_se_TokenAppropriatelyWhenResponder() throws IOException {
		isInitiator = false;
		messagePatterns = "Example:\n"
				+ "  -> se\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKey(localEphKey.sharedSecret(remoteStaticKey));
		handshakeState.readMessage(new ByteArrayInputStream(new byte[0]));
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
	}

	@Test
	public void testReadMessageProcesses_ss_TokenAppropriately() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> ss\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKey(localStaticKey.sharedSecret(remoteStaticKey));
		handshakeState.readMessage(new ByteArrayInputStream(new byte[0]));
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
	}
	
	@Test
	public void testReadMessageProcesses_psk_TokenAppropriately() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> psk\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKeyAndHash(psk);
		handshakeState.readMessage(new ByteArrayInputStream(new byte[0]));
		
		assertEquals(0, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
	}
	
	@Test
	public void testReadMessageDecryptsPayload() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> psk\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		
		byte[] plaintext = crypto.hash("lumber".getBytes());
		
		MutableBoolean invoked = new MutableBoolean();
		handshakeState.setPayload((round)->plaintext, (round, in, decrypter)->{
			byte[] ciphertext = new byte[plaintext.length + crypto.symTagLength()];
			in.read(ciphertext);
			byte[] recovered = decrypter.decrypt(ciphertext);
			assertArrayEquals(recovered, plaintext);
			invoked.setTrue();
		});
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		clone.mixKeyAndHash(psk);
		byte[] ciphertext = clone.encryptAndHash(plaintext);
		handshakeState.readMessage(new ByteArrayInputStream(ciphertext));
		
		assertEquals(1, handshakeState.symmetricState.cipherState.getNonce());
		assertEquals(clone, handshakeState.symmetricState);
	}
	
	@Test
	public void testReadMessageReturnsNullIfRemainingMessagePatterns() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> psk\n"
				+ "  <- psk"; // need an extra line to stop from destroying state
		remakeHandshakeState();
		assertNull(handshakeState.readMessage(new ByteArrayInputStream(new byte[0])));
	}

	@Test
	public void testReadMessageReturnsCipherStatesIfNoRemainingMessagePatternsWhenInitiator() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> foo\n"; // fake token gets dropped, symmetric state remains unchanged
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		CipherState[] states = handshakeState.readMessage(new ByteArrayInputStream(new byte[0]));
		CipherState[] expectedStates = clone.split();
		
		assertEquals(2, states.length);
		assertEquals(expectedStates[0], states[0]);
		assertEquals(expectedStates[1], states[1]);
	}
	
	@Test
	public void testReadMessageReturnsCipherStatesIfNoRemainingMessagePatternsWhenResponder() throws IOException {
		messagePatterns = "Example:\n"
				+ "  -> foo\n"; // fake token gets dropped, symmetric state remains unchanged
		isInitiator = false;
		remakeHandshakeState();
		
		SymmetricState clone = new SymmetricState(handshakeState.symmetricState);
		CipherState[] states = handshakeState.readMessage(new ByteArrayInputStream(new byte[0]));
		CipherState[] expectedStates = clone.split();
		
		// note that the states come in inverted order compared to the initiator
		assertEquals(2, states.length);
		assertEquals(expectedStates[1], states[0]);
		assertEquals(expectedStates[0], states[1]);
	}
	
	@Test
	public void testHandshakeStateResponderAndInitiatorGetMatchingResults() throws IOException {
		messagePatterns = NoiseHandshakes.XKpsk3;
		PrivateDHKey responderStaticPrivKey = new PrivateDHKey(crypto);
		localEphKey = null;
		remoteEphKey = null;
		remoteStaticKey = responderStaticPrivKey.publicKey();
		HandshakeState initiator = makeHandshakeState();
		HandshakeState responder = new HandshakeState(crypto,
					protocolName,
					messagePatterns,
					false,
					prologue,
					responderStaticPrivKey,
					null,
					null,
					null,
					psk);
		PipedInputStream initiatorIn = new PipedInputStream(), responderIn = new PipedInputStream();
		PipedOutputStream initiatorOut = new PipedOutputStream(), responderOut = new PipedOutputStream();
		initiatorIn.connect(responderOut);
		responderIn.connect(initiatorOut);
		
		CipherState[][] stateSets = new CipherState[2][];
		Key[] additional = new Key[2];

		initiator.setDerivationCallback((key)->additional[0] = new Key(crypto, key.getRaw().clone()));
		responder.setDerivationCallback((key)->additional[1] = new Key(crypto, key.getRaw().clone()));

		new Thread(()->{
			try {
				stateSets[0] = initiator.handshake(initiatorIn, initiatorOut);
			} catch (IOException e) {
				fail();
			}
		}).start();
		
		new Thread(()->{
			try {
				stateSets[1] = responder.handshake(responderIn, responderOut);
			} catch (IOException e) {
				fail();
			}
		}).start();
		
		assertTrue(Util.waitUntil(5000, ()->stateSets[0] != null));
		assertTrue(Util.waitUntil(5000, ()->stateSets[1] != null));
		assertEquals(stateSets[0][0], stateSets[1][1]);
		assertEquals(stateSets[0][1], stateSets[1][0]);
		assertNotNull(additional[0]);
		assertEquals(additional[0], additional[1]);
	}

	// testHandshakeResultsMatchTestVectorsForXKpsk3
}
