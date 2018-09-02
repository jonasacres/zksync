package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.HashMap;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.PassphraseProvider;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.Util;

public class TCPHandshakeContextTest {
	CryptoSupport crypto;
	ZKMaster cMaster, sMaster;
	ZKArchive cArchive, sArchive;
	ZKArchiveConfig cConfig, sConfig;
	PeerSwarm cSwarm, sSwarm;
	TCPHandshakeContext cHandshake, sHandshake;
	Socket cSocket, sSocket; // cSocket -> socket held by client (server is remote peer)
	Exception sException;
	Key sSecret;
	MeteredInputStream clientReadMeter, serverReadMeter;
	int serverTamperOffset=-1, serverTamperMask;
	
	class TamperedInputStream extends InputStream {
		InputStream parentStream;
		int tamperedOffset, currentOffset;
		int mask;
		
		public TamperedInputStream(InputStream parentStream, int offset, int mask) {
			this.parentStream = parentStream;
			this.tamperedOffset = offset;
			this.mask = mask;
		}
		
		@Override
		public int read() throws IOException {
			int v = parentStream.read();
			if(v > 0 && currentOffset == tamperedOffset) {
				v ^= mask;
			} 
			
			currentOffset++;
			return v;
		}
	}
	
	class MeteredInputStream extends InputStream {
		int count;
		InputStream parentStream;
		
		public MeteredInputStream(InputStream parentStream) {
			this.parentStream = parentStream;
		}
		
		@Override
		public int read() throws IOException {
			int v = parentStream.read();
			if(v >= 0) count++;
			return v;
		}
	}
	
	class ManipulatedArchiveAccessor extends ArchiveAccessor {
		int modifier, delayCounter;
		
		public ManipulatedArchiveAccessor(ZKMaster master, Key root, int type, int modifier) {
			super(master, root, type);
			this.modifier = modifier;
		}
		
		@Override
		public int timeSliceIndex() {
			if(delayCounter > 0) {
				delayCounter--;
				return super.timeSliceIndex();
			}
			
			return super.timeSliceIndex() + modifier;
		}
	}
	
	class DummyTCPPeerSocketListener extends TCPPeerSocketListener {
		public DummyTCPPeerSocketListener(ZKMaster master, int port) throws IOException {
			super(master, port);
		}
		
		@Override
		protected void peerThread(Socket peerSocketRaw) {
			try {
				sSocket = peerSocketRaw;
				sHandshake = new TCPHandshakeContext(this, peerSocketRaw);
				sHandshake.in = serverReadMeter = new MeteredInputStream(sHandshake.in);
			} catch (IOException exc) {
				exc.printStackTrace();
				fail();
			}
		}
	}
	
	class DummyMaster extends ZKMaster {
		public DummyMaster(CryptoSupport crypto, FS storage, PassphraseProvider passphraseProvider)
				throws IOException, InvalidBlacklistException {
			super(crypto, storage, passphraseProvider);
		}
		
		@Override
		public void listenOnTCP(int port) throws IOException {
			listener = new DummyTCPPeerSocketListener(this, port);
		}
	}
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException, InvalidBlacklistException {
		TCPHandshakeContext.handshakeTimeoutMs = 1000;
		crypto = new CryptoSupport();

		serverTamperOffset = -1;
		serverTamperMask = 0;
		
		cMaster = ZKMaster.openBlankTestVolume("client");
		cArchive = cMaster.createDefaultArchive();
		cConfig = cArchive.getConfig();
		cSwarm = cConfig.getSwarm();
		
		RAMFS.removeVolume("server");
		sMaster = new DummyMaster(crypto, RAMFS.volumeWithName("server"), cMaster.getPassphraseProvider());
		sArchive = sMaster.createDefaultArchive();
		sConfig = sArchive.getConfig();
		sSwarm = sConfig.getSwarm();
		
		sMaster.listenOnTCP(0);
		sMaster.getTCPListener().advertise(sSwarm);
		
		assertTrue(Util.waitUntil(100, ()->sMaster.getTCPListener().port > 0));
		cSocket = new Socket("localhost", sMaster.getTCPListener().getPort());
		cHandshake = new TCPHandshakeContext(cSwarm, cSocket, sSwarm.getPublicIdentityKey());
		cHandshake.in = clientReadMeter = new MeteredInputStream(cHandshake.in);
	}
	
	@After
	public void afterEach() throws IOException {
		cConfig.close();
		cMaster.close();
		
		sConfig.close();
		sMaster.close();
		
		cSocket.close();
		if(sSocket != null) sSocket.close();
		
		TCPHandshakeContext.handshakeTimeoutMs = TCPHandshakeContext.HANDSHAKE_TIMEOUT_MS_DEFAULT;
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	public void doServerHandshake() {
		new Thread(()->{
			try {
				assertTrue(Util.waitUntil(500, ()->sHandshake != null));
				if(serverTamperOffset >= 0) {
					sHandshake.in = new TamperedInputStream(sHandshake.in, serverTamperOffset, serverTamperMask);
				}
				
				sSecret = sHandshake.establishSecret();
			} catch (IOException | ProtocolViolationException exc) {
				sException = exc;
			}
		}).start();
	}
	
	public void tamperServerRead(int offset, int mask) {
		serverTamperOffset = offset;
		serverTamperMask = mask;
	}
	
	public void tamperClientRead(int offset, int mask) {
		cHandshake.in = new TamperedInputStream(cHandshake.in, offset, mask);
	}
	
	@Test
	public void testEstablishesSharedSecret() throws IOException, ProtocolViolationException {
		doServerHandshake();
		Key cSecret = cHandshake.establishSecret();
		assertEquals(cSecret, sSecret);
	}
	
	@Test
	public void testSetsPeerTypeToFullIfBothPartiesHavePassphrase() throws IOException, ProtocolViolationException {
		doServerHandshake();
		cHandshake.establishSecret();
		assertEquals(PeerConnection.PEER_TYPE_FULL, cHandshake.peerType);
		assertEquals(PeerConnection.PEER_TYPE_FULL, sHandshake.peerType);
	}
	
	@Test
	public void testSetsPeerTypeToBlindIfServerHasPassphraseAndClientDoesNot() throws IOException, ProtocolViolationException {
		cConfig.getAccessor().becomeSeedOnly();
		cConfig.close();
		cConfig = ZKArchiveConfig.openExisting(cConfig.getAccessor(), cConfig.getArchiveId());
		
		doServerHandshake();
		Key cSecret = cHandshake.establishSecret();
		assertEquals(PeerConnection.PEER_TYPE_BLIND, cHandshake.peerType);
		assertEquals(PeerConnection.PEER_TYPE_BLIND, sHandshake.peerType);
		assertEquals(cSecret, sSecret);
	}
	
	@Test
	public void testSetsPeerTypeToBlindIfClientHasPassphraseAndServerDoesNot() throws IOException, ProtocolViolationException {
		sConfig.getAccessor().becomeSeedOnly();
		sConfig.close();
		sConfig = ZKArchiveConfig.openExisting(sConfig.getAccessor(), sConfig.getArchiveId());
		
		doServerHandshake();
		Key cSecret = cHandshake.establishSecret();
		assertEquals(PeerConnection.PEER_TYPE_BLIND, cHandshake.peerType);
		assertEquals(PeerConnection.PEER_TYPE_BLIND, sHandshake.peerType);
		assertEquals(cSecret, sSecret);
	}
	
	@Test
	public void testSetsPeerTypeToBlindIfNeitherPartyHasPassphrase() throws IOException, ProtocolViolationException {
		sConfig.getAccessor().becomeSeedOnly();
		sConfig.close();
		sConfig = ZKArchiveConfig.openExisting(sConfig.getAccessor(), sConfig.getArchiveId());
		
		cConfig.getAccessor().becomeSeedOnly();
		cConfig.close();
		cConfig = ZKArchiveConfig.openExisting(cConfig.getAccessor(), cConfig.getArchiveId());
		
		doServerHandshake();
		Key cSecret = cHandshake.establishSecret();
		assertEquals(PeerConnection.PEER_TYPE_BLIND, cHandshake.peerType);
		assertEquals(PeerConnection.PEER_TYPE_BLIND, sHandshake.peerType);
		assertEquals(cSecret, sSecret);
	}
	
	@Test
	public void testServerTriggersProtocolViolationIfClientUsesWrongServerStaticKey() throws IOException, ProtocolViolationException {
		cHandshake = new TCPHandshakeContext(cSwarm, cSocket, crypto.makePrivateDHKey().publicKey());
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(IOException exc) {
		}
		assertTrue(sException instanceof ProtocolViolationException);
	}
	
	@Test
	public void testServerTriggersProtocolViolationIfClientUsesWrongSeedKey() throws IOException, ProtocolViolationException {
		ZKArchive otherArchive = cMaster.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		cHandshake = new TCPHandshakeContext(otherArchive.getConfig().getSwarm(), cSocket, sSwarm.getPublicIdentityKey());
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(IOException exc) {
		}
		
		assertTrue(sException instanceof ProtocolViolationException);
		otherArchive.close();
	}
	
	@Test
	public void testServerTriggersIOExceptionIfTimedOutWaitingForRequest() {
		TCPHandshakeContext.handshakeTimeoutMs = 50;
		doServerHandshake();
		assertTrue(Util.waitUntil(TCPHandshakeContext.handshakeTimeoutMs + 10, ()->sException instanceof IOException));
	}
	
	@Test
	public void testClientTriggersIOExceptionIfTimedOutWaitingForRequest() {
		TCPHandshakeContext.handshakeTimeoutMs = 50;
		MutableBoolean passed = new MutableBoolean();
		
		new Thread(()-> {
			try {
				cHandshake.establishSecret();
				fail();
			} catch (IOException exc) {
				passed.setTrue();
			} catch(Exception exc) {
				exc.printStackTrace();
				fail();
			}
		}).start();
		
		assertTrue(Util.waitUntil(TCPHandshakeContext.handshakeTimeoutMs + 10, ()->passed.isTrue()));
	}
	
	@Test
	public void testServerTriggersProtocolViolationIfClientRequestIsTamperedInRandomSection() throws ProtocolViolationException {
		tamperServerRead(4, 0x80);
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(IOException exc) {}
		assertTrue(sException instanceof ProtocolViolationException);
	}
	
	@Test
	public void testServerTriggersProtocolViolationIfClientRequestIsTamperedInBootstrapSection() throws ProtocolViolationException {
		tamperServerRead(TCPHandshakeContext.BOOTSTRAP_RESERVED_LEN+2, 0x04);
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(IOException exc) {}
		assertTrue(sException instanceof ProtocolViolationException);
	}
	
	@Test
	public void testServerTriggersProtocolViolationIfClientRequestIsTamperedInAuthSection() throws ProtocolViolationException {
		tamperServerRead(crypto.hashLength()
				+ crypto.symUnpaddedCiphertextSize(cHandshake.bootstrapPlaintextLen()
				+ 9), 0x02);
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(IOException exc) {}
		assertTrue(sException instanceof ProtocolViolationException);
	}
	
	@Test
	public void testClientTriggersProtocolViolationIfClientRequestIsTamperedInPadding() throws ProtocolViolationException, IOException {
		tamperServerRead(crypto.hashLength()
				+ crypto.symUnpaddedCiphertextSize(cHandshake.bootstrapPlaintextLen()
				+ crypto.symUnpaddedCiphertextSize(cHandshake.authRecordPlaintextLen())
				+ 1), 0x20);
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(ProtocolViolationException exc) {}
	}
	
	@Test
	public void testClientTriggersProtocolViolationIfServerResponseIsTamperedInBootstrapRecord() throws IOException {
		tamperClientRead(2, 0x10);
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(ProtocolViolationException exc) {}
	}
	
	@Test
	public void testClientTriggersProtocolViolationIfServerResponseIsTamperedInAuthRecord() throws IOException {
		tamperClientRead(crypto.symUnpaddedCiphertextSize(cHandshake.bootstrapPlaintextLen())
				+ 4,
				0x08);
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(ProtocolViolationException exc) {}
	}
	
	@Test
	public void testClientTriggersProtocolViolationIfServerResponseIsTamperedInPadding() throws IOException {
		tamperClientRead(crypto.symUnpaddedCiphertextSize(cHandshake.bootstrapPlaintextLen())
				+ crypto.symUnpaddedCiphertextSize(cHandshake.authRecordPlaintextLen()), // fringe chance that this actually modifies secret confirmation since padding length could be zero
				0x08);
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(ProtocolViolationException exc) {}
	}
	
	@Test
	public void testServerBlacklistsClientOnProtocolViolation() throws IOException {
		tamperServerRead(0, 0x01);
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(Exception exc) {}
		
		assertTrue(sMaster.getBlacklist().contains(sSocket.getInetAddress().getHostAddress()));
	}

	@Test
	public void testClientBlacklistsClientOnProtocolViolation() throws IOException {
		tamperClientRead(0, 0x01);
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(Exception exc) {}
		
		assertTrue(cMaster.getBlacklist().contains(sSocket.getInetAddress().getHostAddress()));
	}
	
	@Test
	public void testClientRequestLengthIsRandomInExpectedRange() throws IOException, InvalidBlacklistException, ProtocolViolationException {
		int samples = 50;
		int minLength = crypto.hashLength()
				+ crypto.symUnpaddedCiphertextSize(cHandshake.bootstrapPlaintextLen())
				+ crypto.symUnpaddedCiphertextSize(cHandshake.authRecordPlaintextLen());
		int maxLength = minLength + TCPHandshakeContext.MAX_PADDING_LEN;
		
		HashMap<Integer,Integer> lengths = new HashMap<>(samples);
		for(int i = 0; i < samples; i++) {
			if(i != 0) {
				afterEach();
				beforeEach();
				Util.sleep(2); // a bit of delay in the loop seems to help some socket issues; extend if we have ITFs related to socket closure
			}
			
			doServerHandshake();
			cHandshake.establishSecret();
			
			int length = serverReadMeter.count;
			assertTrue(length >= minLength);
			assertTrue(length <= maxLength);
			lengths.putIfAbsent(length, 0);
			lengths.put(length, 1 + lengths.get(length));
		}
		
		for(int length : lengths.keySet()) {
			assert(lengths.get(length) < 5);
		}
	}
	
	@Test
	public void testServerResponseLengthIsRandomInExpectedRange() throws IOException, ProtocolViolationException, InvalidBlacklistException {
		int samples = 50;
		int minLength = crypto.hashLength()
				+ crypto.symUnpaddedCiphertextSize(cHandshake.bootstrapPlaintextLen())
				+ crypto.symUnpaddedCiphertextSize(cHandshake.authRecordPlaintextLen());
		int maxLength = minLength + TCPHandshakeContext.MAX_PADDING_LEN;
		
		HashMap<Integer,Integer> lengths = new HashMap<>(samples);
		for(int i = 0; i < samples; i++) {
			if(i != 0) {
				afterEach();
				beforeEach();
				Util.sleep(2); // a bit of delay in the loop seems to help some socket issues; extend if we have ITFs related to socket closure
			}
			
			doServerHandshake();
			cHandshake.establishSecret();
			
			int length = clientReadMeter.count;
			assertTrue(length >= minLength);
			assertTrue(length <= maxLength);
			lengths.putIfAbsent(length, 0);
			lengths.put(length, 1 + lengths.get(length));
		}
		
		for(int length : lengths.keySet()) {
			assert(lengths.get(length) < 5);
		}
	}
	
	@Test
	public void testServerTriggersProtocolViolationIfTimeIndexBeyondGraceAfterRange() throws IOException, ProtocolViolationException {
		Util.setCurrentTimeMillis((long) (1.5 * ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS));
		byte[] ppRootRaw = crypto.deriveKeyFromPassphrase(cMaster.getPassphraseProvider().requestPassphrase("Passphrase for new archive"));
		Key ppRoot = new Key(crypto, ppRootRaw);

		ManipulatedArchiveAccessor fakeAccessor = new ManipulatedArchiveAccessor(cMaster, ppRoot, ArchiveAccessor.KEY_ROOT_PASSPHRASE, 1);
		cConfig.setAccessor(fakeAccessor);
		
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(Exception exc) {}
		
		assertTrue(sException instanceof ProtocolViolationException);
	}
	
	@Test
	public void testServerTriggersProtocolViolationIfTimeIndexBeyondGraceBeforeRange() throws IOException, ProtocolViolationException {
		Util.setCurrentTimeMillis((long) (1.5 * ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS));
		byte[] ppRootRaw = crypto.deriveKeyFromPassphrase(cMaster.getPassphraseProvider().requestPassphrase("Passphrase for new archive"));
		Key ppRoot = new Key(crypto, ppRootRaw);

		ManipulatedArchiveAccessor fakeAccessor = new ManipulatedArchiveAccessor(cMaster, ppRoot, ArchiveAccessor.KEY_ROOT_PASSPHRASE, -1);
		cConfig.setAccessor(fakeAccessor);
		
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(Exception exc) {}
		
		assertTrue(sException instanceof ProtocolViolationException);
	}
	
	@Test
	public void testClientTriggersProtocolViolationIfTimeIndexBeyondGraceBeforeRange() throws IOException, ProtocolViolationException {
		Util.setCurrentTimeMillis((long) (1.5 * ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS));
		byte[] ppRootRaw = crypto.deriveKeyFromPassphrase(sMaster.getPassphraseProvider().requestPassphrase("Passphrase for new archive"));
		Key ppRoot = new Key(crypto, ppRootRaw);

		ManipulatedArchiveAccessor fakeAccessor = new ManipulatedArchiveAccessor(sMaster, ppRoot, ArchiveAccessor.KEY_ROOT_PASSPHRASE, -1);
		fakeAccessor.delayCounter = 1;
		sConfig.setAccessor(fakeAccessor);
		
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(ProtocolViolationException exc) {}
	}
	
	@Test
	public void testClientTriggersProtocolViolationIfTimeIndexBeyondGraceAfterRange() throws IOException, ProtocolViolationException {
		Util.setCurrentTimeMillis((long) (1.5 * ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS));
		byte[] ppRootRaw = crypto.deriveKeyFromPassphrase(sMaster.getPassphraseProvider().requestPassphrase("Passphrase for new archive"));
		Key ppRoot = new Key(crypto, ppRootRaw);

		ManipulatedArchiveAccessor fakeAccessor = new ManipulatedArchiveAccessor(sMaster, ppRoot, ArchiveAccessor.KEY_ROOT_PASSPHRASE, 1);
		fakeAccessor.delayCounter = 1;
		sConfig.setAccessor(fakeAccessor);
		
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(ProtocolViolationException exc) {}
	}
	
	@Test
	public void testClientDoesNotTriggerProtocolViolationIfTimeIndexIsWithinGraceBeforeRange() throws IOException, ProtocolViolationException {
		Util.setCurrentTimeMillis(ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS
				+ TCPHandshakeContext.TIMESLICE_EXPIRATION_GRACE_MS
				- 1);

		byte[] ppRootRaw = crypto.deriveKeyFromPassphrase(sMaster.getPassphraseProvider().requestPassphrase("Passphrase for new archive"));
		Key ppRoot = new Key(crypto, ppRootRaw);
		ManipulatedArchiveAccessor fakeAccessor = new ManipulatedArchiveAccessor(sMaster, ppRoot, ArchiveAccessor.KEY_ROOT_PASSPHRASE, -1);
		sConfig.setAccessor(fakeAccessor);

		doServerHandshake();
		cHandshake.establishSecret();
	}

	@Test
	public void testClientDoesNotTriggerProtocolViolationIfTimeIndexIsWithinGraceAfterRange() throws IOException, ProtocolViolationException {
		Util.setCurrentTimeMillis(ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS
				- TCPHandshakeContext.TIMESLICE_EXPIRATION_GRACE_MS
				+ 1);

		byte[] ppRootRaw = crypto.deriveKeyFromPassphrase(sMaster.getPassphraseProvider().requestPassphrase("Passphrase for new archive"));
		Key ppRoot = new Key(crypto, ppRootRaw);
		ManipulatedArchiveAccessor fakeAccessor = new ManipulatedArchiveAccessor(sMaster, ppRoot, ArchiveAccessor.KEY_ROOT_PASSPHRASE, 1);
		sConfig.setAccessor(fakeAccessor);

		doServerHandshake();
		cHandshake.establishSecret();
	}

	
	@Test
	public void testServerDoesNotTriggerProtocolViolationIfTimeIndexIsWithinGraceBeforeRange() throws IOException, ProtocolViolationException {
		Util.setCurrentTimeMillis(ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS
				+ TCPHandshakeContext.TIMESLICE_EXPIRATION_GRACE_MS
				- 1);

		byte[] ppRootRaw = crypto.deriveKeyFromPassphrase(cMaster.getPassphraseProvider().requestPassphrase("Passphrase for new archive"));
		Key ppRoot = new Key(crypto, ppRootRaw);
		ManipulatedArchiveAccessor fakeAccessor = new ManipulatedArchiveAccessor(cMaster, ppRoot, ArchiveAccessor.KEY_ROOT_PASSPHRASE, -1);
		cConfig.setAccessor(fakeAccessor);

		doServerHandshake();
		cHandshake.establishSecret();
	}

	@Test
	public void testServerDoesNotTriggerProtocolViolationIfTimeIndexIsWithinGraceAfterRange() throws IOException, ProtocolViolationException {
		Util.setCurrentTimeMillis(ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS
				- TCPHandshakeContext.TIMESLICE_EXPIRATION_GRACE_MS
				+ 1);

		byte[] ppRootRaw = crypto.deriveKeyFromPassphrase(cMaster.getPassphraseProvider().requestPassphrase("Passphrase for new archive"));
		Key ppRoot = new Key(crypto, ppRootRaw);
		ManipulatedArchiveAccessor fakeAccessor = new ManipulatedArchiveAccessor(cMaster, ppRoot, ArchiveAccessor.KEY_ROOT_PASSPHRASE, 1);
		cConfig.setAccessor(fakeAccessor);

		doServerHandshake();
		cHandshake.establishSecret();
	}

	@Test
	public void testServerScrubsEphemeralKeyDataOnProtocolViolation() throws IOException, ProtocolViolationException {
		tamperServerRead(0, 1);
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(Exception exc) {}
		assertTrue(sException instanceof ProtocolViolationException);
		if(sHandshake.localEphKey != null) { // allow the implementation to defer init of localEphKey until handshake commences
			assertArrayEquals(new byte[crypto.symKeyLength()], sHandshake.localEphKey.getBytes());
		}
	}
	
	@Test
	public void testServerScrubsEphemeralKeyDataOnTimeout() throws IOException, ProtocolViolationException {
		TCPHandshakeContext.handshakeTimeoutMs = 1;
		doServerHandshake();
		assertTrue(Util.waitUntil(100, ()->sException != null));
		if(sHandshake.localEphKey != null) { // allow the implementation to defer init of localEphKey until handshake commences
			assertArrayEquals(new byte[crypto.symKeyLength()], sHandshake.localEphKey.getBytes());
		}
	}
	
	@Test
	public void testServerScrubsEphemeralKeyDataOnSuccessfulHandshake() throws IOException, ProtocolViolationException {
		doServerHandshake();
		cHandshake.establishSecret();
		assertArrayEquals(new byte[crypto.symKeyLength()], sHandshake.localEphKey.getBytes());
	}
	
	@Test
	public void testClientScrubsEphemeralKeyDataOnProtocolViolation() throws IOException, ProtocolViolationException {
		tamperClientRead(0, 1);
		doServerHandshake();
		try {
			cHandshake.establishSecret();
			fail();
		} catch(ProtocolViolationException exc) {}
		assertArrayEquals(new byte[crypto.symKeyLength()], cHandshake.localEphKey.getBytes());
	}
	
	@Test
	public void testClientScrubsEphemeralKeyDataOnTimeout() throws IOException, ProtocolViolationException {
		TCPHandshakeContext.handshakeTimeoutMs = 1;
		try {
			cHandshake.establishSecret();
			fail();
		} catch(IOException exc) {}
		assertArrayEquals(new byte[crypto.symKeyLength()], cHandshake.localEphKey.getBytes());
	}
	
	@Test
	public void testClientScrubsEphemeralKeyDataOnSuccessfulHandshake() throws IOException, ProtocolViolationException {
		doServerHandshake();
		cHandshake.establishSecret();
		assertArrayEquals(new byte[crypto.symKeyLength()], cHandshake.localEphKey.getBytes());
	}
	
	@Test
	public void testClientReportsPortZeroIfNotListeningForInboundConnections() throws IOException, ProtocolViolationException {
		doServerHandshake();
		cHandshake.establishSecret();
		assertEquals(0, sHandshake.remotePort);
	}
	
	@Test
	public void testClientReportsBoundTCPPortNumberIfListeningForInboundConnections() throws IOException, ProtocolViolationException {
		cMaster.listenOnTCP(0);
		cMaster.getTCPListener().advertise(cSwarm);

		doServerHandshake();
		cHandshake.establishSecret();
		assertEquals(cMaster.getTCPListener().port, sHandshake.remotePort);
	}
	
	@Test
	public void testClientReportsPortZeroIfListeningForInboundConnectionsButNotAdvertisingSwarm() throws IOException, ProtocolViolationException {
		cMaster.listenOnTCP(0);

		doServerHandshake();
		cHandshake.establishSecret();
		assertEquals(0, sHandshake.remotePort);
	}
}
