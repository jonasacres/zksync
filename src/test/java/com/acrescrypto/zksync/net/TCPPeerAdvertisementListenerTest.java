package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
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
	
	static ZKMaster master;
	static ZKArchive archive;
	DummySwarm swarm;
	
	TCPPeerAdvertisementListener listener;
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
	}
	
	@Before
	public void before() throws IOException {
		swarm = new DummySwarm(archive.getConfig());
		listener = new TCPPeerAdvertisementListener(swarm, 0);
	}
	
	@Test
	public void testMatchesKeyHashReturnsTrueIfKeyHashIsValid() {
		PrivateDHKey privKey = master.getCrypto().makePrivateDHKey();
		ByteBuffer buf = ByteBuffer.allocate(2*master.getCrypto().asymPublicDHKeySize());
		buf.put(privKey.publicKey().getBytes());
		buf.put(listener.dhPrivateKey.publicKey().getBytes());
		
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
		assertTrue(Arrays.equals(listener.dhPrivateKey.publicKey().getBytes(), ad.pubKey.getBytes()));
		assertEquals("localhost", ad.host);
		assertEquals(listener.port, ad.port);
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
		assertTrue(Arrays.equals(listener.dhPrivateKey.publicKey().getBytes(), selfAd.pubKey.getBytes()));
		assertEquals(listener.port, selfAd.port);
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
		assertTrue(Arrays.equals(listener.dhPrivateKey.publicKey().getBytes(), selfAd.pubKey.getBytes()));
		assertEquals(listener.port, selfAd.port);
		assertEquals("localhost", selfAd.host);
		
		assertNotNull(swarm.announceThread);
		assertNotEquals(Thread.currentThread(), swarm.announceThread);
	}
	
	@Test
	public void testGeneratesNewKeypairOnFirstInstanceForArchiveId() throws IOException {
		ZKArchive archive2 = master.createArchive(65536, "");
		DummySwarm swarm2 = new DummySwarm(archive2.getConfig());
		TCPPeerAdvertisementListener listener2 = new TCPPeerAdvertisementListener(swarm2, 0);
		assertFalse(Arrays.equals(listener.dhPrivateKey.getBytes(), listener2.dhPrivateKey.getBytes()));
		assertFalse(Arrays.equals(listener.dhPrivateKey.publicKey().getBytes(), listener2.dhPrivateKey.publicKey().getBytes()));
	}
	
	@Test
	public void testGeneratesReusesExistingKeypairOnRepeatedInstancesForArchiveId() throws IOException {
		TCPPeerAdvertisementListener listener2 = new TCPPeerAdvertisementListener(swarm, 0);
		assertTrue(Arrays.equals(listener.dhPrivateKey.getBytes(), listener2.dhPrivateKey.getBytes()));
		assertTrue(Arrays.equals(listener.dhPrivateKey.publicKey().getBytes(), listener2.dhPrivateKey.publicKey().getBytes()));
	}
}
