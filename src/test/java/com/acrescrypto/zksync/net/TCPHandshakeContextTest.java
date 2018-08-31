package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.Socket;

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
	
	class DummyTCPPeerSocketListener extends TCPPeerSocketListener {
		public DummyTCPPeerSocketListener(ZKMaster master, int port) throws IOException {
			super(master, port);
		}
		
		@Override
		protected void peerThread(Socket peerSocketRaw) {
			try {
				sHandshake = new TCPHandshakeContext(this, peerSocketRaw);
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
		
		cMaster = ZKMaster.openBlankTestVolume("client");
		cArchive = cMaster.createDefaultArchive();
		cConfig = cArchive.getConfig();
		cSwarm = cConfig.getSwarm();
		
		sMaster = new DummyMaster(crypto, RAMFS.volumeWithName("server"), cMaster.getPassphraseProvider());
		sArchive = sMaster.createDefaultArchive();
		sConfig = sArchive.getConfig();
		sSwarm = sConfig.getSwarm();
		
		cMaster.listenOnTCP(0);
		cMaster.getTCPListener().advertise(cSwarm);
		
		sMaster.listenOnTCP(0);
		sMaster.getTCPListener().advertise(sSwarm);
		
		assertTrue(Util.waitUntil(100, ()->sMaster.getTCPListener().port > 0));
		assertTrue(Util.waitUntil(100, ()->cMaster.getTCPListener().port > 0));
		cSocket = new Socket("localhost", sMaster.getTCPListener().getPort());
		cHandshake = new TCPHandshakeContext(cSwarm, cSocket, sSwarm.getPublicIdentityKey());
	}
	
	@After
	public void afterEach() {
		cConfig.close();
		cMaster.close();
		
		sConfig.close();
		sMaster.close();
		
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
				sSecret = sHandshake.establishSecret();
			} catch (IOException | ProtocolViolationException exc) {
				sException = exc;
			}
		}).start();
	}
	
	// TODO: establishes shared secret
	@Test
	public void testEstablishesSharedSecret() throws IOException, ProtocolViolationException {
		doServerHandshake();
		Key cSecret = cHandshake.establishSecret();
		assertEquals(cSecret, sSecret);
	}
	
	// TODO: sets peer type to FULL if both parties have passphrase
	// TODO: sets peer type to BLIND if server has passphrase and client does not
	// TODO: sets peer type to BLIND if client has passphrase and server does not
	// TODO: sets peer type to BLIND if neither peer has passphrase
	
	// TODO: server triggers protocol violation if client uses wrong server static key
	// TODO: server triggers protocol violation if client uses wrong seed key
	// TODO: server triggers protocol violation if client uses mismatched seed key / server static key
	
	// TODO: server triggers protocol violation if timed out waiting for request
	// TODO: client triggers protocol violation if timed out waiting for response
	
	// TODO: server triggers protocol violation if client request is tampered in random section
	// TODO: server triggers protocol violation if client request is tampered in bootstrap record
	// TODO: server triggers protocol violation if client request is tampered in auth record
	// TODO: mismatched shared secret if client request is tampered in padding
	
	// TODO: client triggers protocol violation if server response is tampered in bootstrap record
	// TODO: client triggers protocol violation if server response is tampered in auth record
	// TODO: client triggers protocol violation if server does not provide bootstrap record
	// TODO: client triggers protocol violation if server does not provide auth record
	// TODO: mismatched shared secret if server response is tampered in padding
	
	// TODO: client request length is random in expected range
	// TODO: server response length is random in expected range
	
	// TODO: server triggers protocol violation if client sends duplicate bootstrap
	// TODO: server triggers protocol violation if client sends duplicate auth
	
	// TODO: client triggers protocol violation if server sends duplicate bootstrap
	// TODO: client triggers protocol violation if server sends duplicate auth
	
	// TODO: server triggers protocol violation if timeindex is before range
	// TODO: server triggers protocol violation if timeindex is after range

	// TODO: server scrubs ephemeral key data on protocol violation
	// TODO: server scrubs ephemeral key data on timeout
	// TODO: server scrubs ephemeral key data on successful handshake
	
	// TODO: client scrubs ephemeral key data on protocol violation
	// TODO: client scrubs ephemeral key data on timeout
	// TODO: client scrubs ephemeral key data on successful handshake
	
	// TODO: localPortNum returns 0 if not listening for inbound connections
	// TODO: localPortNum returns TCP port number if listening for inbound connections
}
