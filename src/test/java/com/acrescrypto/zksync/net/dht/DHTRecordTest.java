package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.net.TCPPeerAdvertisement;

public class DHTRecordTest {
	class DummyClient extends DHTClient {
		public DummyClient() {
			this.crypto = DHTRecordTest.crypto;
		}
	}
	
	static CryptoSupport crypto;
	DummyClient client;
	
	@BeforeClass
	public static void beforeAll() {
		crypto = CryptoSupport.defaultCrypto();
	}
	
	@Before
	public void beforeEach() {
		client = new DummyClient();
	}
	
	@After
	public void afterEach() {
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}
	
	@Test
	public void testDeserializeRecordHandlesAdvertisements() throws UnsupportedProtocolException {
		CryptoSupport crypto = CryptoSupport.defaultCrypto();
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "localhost", 1234, crypto.rng(crypto.hashLength()));
		DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(crypto, ad);
		
		DHTAdvertisementRecord deserialized = (DHTAdvertisementRecord) DHTRecord.deserializeRecord(crypto, ByteBuffer.wrap(adRecord.serialize()));
		assertTrue(deserialized instanceof DHTAdvertisementRecord);
		assertEquals(ad, deserialized.ad);
	}
	
	@Test(expected=UnsupportedProtocolException.class)
	public void testDeserializeRecordThrowsExceptionIfUnsupportedType() throws UnsupportedProtocolException {
		CryptoSupport crypto = CryptoSupport.defaultCrypto();
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "localhost", 1234, crypto.rng(crypto.hashLength()));
		DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(crypto, ad);
		byte[] serialized = adRecord.serialize();
		
		serialized[0] += 1;
		DHTRecord.deserializeRecord(crypto, ByteBuffer.wrap(serialized));
	}

	@Test
	public void testDeserializeRecordWithPeerHandlesAdvertisements() throws UnsupportedProtocolException {
		CryptoSupport crypto = CryptoSupport.defaultCrypto();
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "localhost", 1234, crypto.rng(crypto.hashLength()));
		DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(crypto, ad);
		DHTPeer peer = new DHTPeer(client, "127.0.0.1", 4321, crypto.makePrivateDHKey().publicKey().getBytes());
		
		DHTAdvertisementRecord deserialized = (DHTAdvertisementRecord) DHTRecord.deserializeRecordWithPeer(peer, ByteBuffer.wrap(adRecord.serialize()));
		assertTrue(deserialized instanceof DHTAdvertisementRecord);
		
		TCPPeerAdvertisement deserializedAd = (TCPPeerAdvertisement) deserialized.ad;
		assertEquals("127.0.0.1", deserializedAd.getHost());
		assertEquals(ad.getPort(), deserializedAd.getPort());
		assertEquals(ad.getVersion(), deserializedAd.getVersion());
		assertArrayEquals(ad.getPubKey().getBytes(), deserializedAd.getPubKey().getBytes());
		assertArrayEquals(ad.getEncryptedArchiveId(), deserializedAd.getEncryptedArchiveId());
	}
	
	@Test(expected=UnsupportedProtocolException.class)
	public void testDeserializeRecorThrowsExceptionIfUnsupportedType() throws UnsupportedProtocolException {
		CryptoSupport crypto = CryptoSupport.defaultCrypto();
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "localhost", 1234, crypto.rng(crypto.hashLength()));
		DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(crypto, ad);
		DHTPeer peer = new DHTPeer(client, "127.0.0.1", 4321, crypto.makePrivateDHKey().publicKey().getBytes());
		byte[] serialized = adRecord.serialize();
		
		serialized[0] += 1;
		DHTRecord.deserializeRecordWithPeer(peer, ByteBuffer.wrap(serialized));
	}
}
