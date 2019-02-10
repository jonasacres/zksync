package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerAdvertisementListenerTest {
	class DummySwarm extends PeerSwarm {
		PeerAdvertisement selfAd;
		Thread announceThread;

		public DummySwarm(ZKArchiveConfig config) throws IOException { super(config); }
		@Override public void advertiseSelf(PeerAdvertisement ad) {
			announceThread = Thread.currentThread();
			selfAd = ad;
		}
	}
	
	class DummyTCPPeerSocketListener extends TCPPeerSocketListener {
		public DummyTCPPeerSocketListener(ZKMaster master, int port) throws IOException {
			super(master);
			this.port = port;
		}
		
		@Override protected void listenThread() {}
		@Override public boolean isListening() { return true; }
	}
	
	static ZKMaster master;
	static ZKArchive archive;
	DummySwarm swarm;
	
	TCPPeerAdvertisementListener listener;
	DummyTCPPeerSocketListener socketListener;
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		TestUtils.startDebugMode();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
	}
	
	@Before
	public void before() throws IOException {
		swarm = new DummySwarm(archive.getConfig());
		socketListener = new DummyTCPPeerSocketListener(master, 1);
		listener = new TCPPeerAdvertisementListener(swarm, socketListener);
	}
	
	@After
	public void afterEach() {
		swarm.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.stopDebugMode();
		archive.close();
		master.close();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testMatchesKeyHashReturnsTrueIfKeyHashIsValid() {
		byte[] hsHash = master.getCrypto().hash("some stuff".getBytes());
		byte[] idHash = master.getCrypto().hash(Util.concat(hsHash, swarm.config.getArchiveId()));
		
		assertTrue(listener.matchesIdHash(hsHash, idHash));
	}
	
	@Test
	public void testMatchesKeyHashReturnsFalseIfKeyHashIsNotValid() {
		byte[] invalidKeyHash = master.getCrypto().rng(master.getCrypto().hashLength());
		assertFalse(listener.matchesIdHash(invalidKeyHash, invalidKeyHash));
	}
	
	@Test
	public void testLocalAdReturnsWellFormattedAdvertisement() throws UnconnectableAdvertisementException {
		TCPPeerAdvertisement ad = listener.localAd();
		assertTrue(Arrays.equals(listener.swarm.getPublicIdentityKey().getBytes(), ad.pubKey.getBytes()));
		assertEquals("localhost", ad.host);
		assertEquals(socketListener.getPort(), ad.port);
	}
	
	@Test
	public void testConstructorTriggersAnnounce() throws InterruptedException {
		long endTime = System.currentTimeMillis() + 100;
		while(swarm.selfAd == null && System.currentTimeMillis() < endTime) {
			Thread.sleep(1);
		}
		
		assertNotNull(swarm.selfAd);
		assertTrue(swarm.selfAd instanceof TCPPeerAdvertisement);
		
		TCPPeerAdvertisement selfAd = (TCPPeerAdvertisement) swarm.selfAd;
		assertTrue(Arrays.equals(listener.swarm.getPublicIdentityKey().getBytes(), selfAd.pubKey.getBytes()));
		assertEquals(socketListener.getPort(), selfAd.port);
		assertEquals("localhost", selfAd.host);
		
		assertNotNull(swarm.announceThread);
		assertNotEquals(Thread.currentThread(), swarm.announceThread);
	}

	@Test
	public void testAnnounceNotifiesSwarmToAdvertiseSelfInNewThread() throws InterruptedException {
		// constructor triggered announce and we want to run it again manually, so wait for old result and clear

		long endTime = System.currentTimeMillis() + 100;
		while(swarm.selfAd == null && System.currentTimeMillis() < endTime) {
			Thread.sleep(1);
		}
		
		swarm.selfAd = null;
		swarm.announceThread = null;
		
		listener.announce();
		
		endTime = System.currentTimeMillis() + 100;
		while(swarm.selfAd == null && System.currentTimeMillis() < endTime) {
			Thread.sleep(1);
		}
		
		assertNotNull(swarm.selfAd);
		assertTrue(swarm.selfAd instanceof TCPPeerAdvertisement);
		
		TCPPeerAdvertisement selfAd = (TCPPeerAdvertisement) swarm.selfAd;
		assertTrue(Arrays.equals(listener.swarm.getPublicIdentityKey().getBytes(), selfAd.pubKey.getBytes()));
		assertEquals(socketListener.getPort(), selfAd.port);
		assertEquals("localhost", selfAd.host);
		
		assertNotNull(swarm.announceThread);
		assertNotEquals(Thread.currentThread(), swarm.announceThread);
	}
}
