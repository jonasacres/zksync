package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;

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
			super(master, -1);
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
		ZKFSTest.cheapenArgon2Costs();
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
		ZKFSTest.restoreArgon2Costs();
		archive.close();
		master.close();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testMatchesKeyHashReturnsTrueIfKeyHashIsValid() {
		PrivateDHKey privKey = master.getCrypto().makePrivateDHKey();
		ByteBuffer buf = ByteBuffer.allocate(2*master.getCrypto().asymPublicDHKeySize()+master.getCrypto().hashLength()+4);
		buf.put(privKey.publicKey().getBytes());
		buf.put(listener.swarm.identityKey.publicKey().getBytes());
		buf.put(swarm.config.getArchiveId());
		buf.putInt(0);
		
		Key keyHashKey = swarm.config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_SEED);
		byte[] keyHash = keyHashKey.authenticate(buf.array());
		assertTrue(listener.matchesKeyHash(privKey.publicKey(), keyHash));
	}
	
	@Test
	public void testMatchesKeyHashReturnsFalseIfKeyHashIsNotValid() {
		PrivateDHKey privKey = master.getCrypto().makePrivateDHKey();
		byte[] invalidKeyHash = master.getCrypto().rng(master.getCrypto().hashLength());
		assertFalse(listener.matchesKeyHash(privKey.publicKey(), invalidKeyHash));
	}
	
	@Test
	public void testLocalAdReturnsWellFormattedAdvertisement() throws UnconnectableAdvertisementException {
		TCPPeerAdvertisement ad = listener.localAd();
		assertTrue(Arrays.equals(listener.swarm.identityKey.publicKey().getBytes(), ad.pubKey.getBytes()));
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
		assertTrue(Arrays.equals(listener.swarm.identityKey.publicKey().getBytes(), selfAd.pubKey.getBytes()));
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
		assertTrue(Arrays.equals(listener.swarm.identityKey.publicKey().getBytes(), selfAd.pubKey.getBytes()));
		assertEquals(socketListener.getPort(), selfAd.port);
		assertEquals("localhost", selfAd.host);
		
		assertNotNull(swarm.announceThread);
		assertNotEquals(Thread.currentThread(), swarm.announceThread);
	}
	
	@Test
	public void testGeneratesNewKeypairOnFirstInstanceForArchiveId() throws IOException {
		ZKArchive archive2 = master.createArchive(65536, "");
		DummySwarm swarm2 = new DummySwarm(archive2.getConfig());
		TCPPeerAdvertisementListener listener2 = new TCPPeerAdvertisementListener(swarm2, socketListener);
		assertFalse(Arrays.equals(listener.swarm.identityKey.getBytes(), listener2.swarm.identityKey.getBytes()));
		assertFalse(Arrays.equals(listener.swarm.identityKey.publicKey().getBytes(), listener2.swarm.identityKey.publicKey().getBytes()));
		archive2.close();
		swarm2.close();
	}
	
	@Test
	public void testGeneratesReusesExistingKeypairOnRepeatedInstancesForArchiveId() throws IOException {
		TCPPeerAdvertisementListener listener2 = new TCPPeerAdvertisementListener(swarm, socketListener);
		assertTrue(Arrays.equals(listener.swarm.identityKey.getBytes(), listener2.swarm.identityKey.getBytes()));
		assertTrue(Arrays.equals(listener.swarm.identityKey.publicKey().getBytes(), listener2.swarm.identityKey.publicKey().getBytes()));
	}
}
