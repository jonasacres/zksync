package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.net.TCPPeerAdvertisement;

public class DHTAdvertisementRecordTest {
	// construct with ad
	static CryptoSupport crypto;
	static TCPPeerAdvertisement ad;
	
	@BeforeClass
	public static void beforeAll() {
		crypto = CryptoSupport.defaultCrypto();
		ad = new TCPPeerAdvertisement(crypto.makePrivateDHKey().publicKey(), "localhost", 1234, crypto.rng(crypto.hashLength()));
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}
	
	@Test
	public void testConstructWithAd() {
		DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(crypto, ad);
		assertEquals(ad, adRecord.ad);
	}
	
	@Test
	public void testConstructWithSerialization() throws UnsupportedProtocolException {
		DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(crypto, ad);
		DHTAdvertisementRecord deserialized = new DHTAdvertisementRecord(crypto, ByteBuffer.wrap(adRecord.serialize()));
		assertEquals(ad, deserialized.ad);
	}
	
	@Test
	public void testConstructWithSerializationAddressAndPort() throws UnsupportedProtocolException {
		DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(crypto, ad);
		ByteBuffer serialization = ByteBuffer.wrap(adRecord.serialize());
		DHTAdvertisementRecord deserialized = new DHTAdvertisementRecord(crypto, serialization, "127.0.0.1");
		TCPPeerAdvertisement deserializedAd = (TCPPeerAdvertisement) deserialized.ad;
		
		assertEquals("127.0.0.1", deserializedAd.getHost());
		assertEquals(adRecord.asTcp().getPort(), deserializedAd.getPort());
		assertEquals(ad.getVersion(), deserializedAd.getVersion());
		
		assertArrayEquals(ad.getEncryptedArchiveId(), deserializedAd.getEncryptedArchiveId());
		assertArrayEquals(ad.getPubKey().getBytes(), deserializedAd.getPubKey().getBytes());
	}
	
	@Test(expected=UnsupportedProtocolException.class)
	public void testDeserializationThrowsUnsupportedProtocolExceptionIfTypeNotSetCorrectly() throws UnsupportedProtocolException {
		DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(crypto, ad);
		byte[] serialized = adRecord.serialize();
		serialized[0] += 1;
		new DHTAdvertisementRecord(crypto, ByteBuffer.wrap(serialized));
	}
	
	@Test(expected=UnsupportedProtocolException.class)
	public void testDeserializationThrowsUnsupportedProtocolExceptionIfLengthIsShort() throws UnsupportedProtocolException {
		DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(crypto, ad);
		byte[] serialized = adRecord.serialize();
		serialized[2] -= 1;
		new DHTAdvertisementRecord(crypto, ByteBuffer.wrap(serialized));
	}
	
	@Test(expected=UnsupportedProtocolException.class)
	public void testDeserializationThrowsUnsupportedProtocolExceptionIfLengthExceedsBuffer() throws UnsupportedProtocolException {
		DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(crypto, ad);
		byte[] serialized = adRecord.serialize();
		serialized[2] += 1;
		new DHTAdvertisementRecord(crypto, ByteBuffer.wrap(serialized));
	}
	
	@Test(expected=UnsupportedProtocolException.class)
	public void testDeserializationThrowsUnsupportedProtocolExceptionIfLengthExceedsRecordLength() throws UnsupportedProtocolException {
		DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(crypto, ad);
		byte[] serialized = adRecord.serialize();
		serialized[2] += 1;
		ByteBuffer buf = ByteBuffer.allocate(2*serialized.length);
		buf.put(serialized);
		new DHTAdvertisementRecord(crypto, buf);
	}

	@Test(expected=UnsupportedProtocolException.class)
	public void testDeserializationTrhowsUnsupportedProtocolExceptionIfAdTypeNotSupported() throws UnsupportedProtocolException {
		DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(crypto, ad);
		byte[] serialized = adRecord.serialize();
		serialized[3] = Byte.MIN_VALUE;
		new DHTAdvertisementRecord(crypto, ByteBuffer.wrap(serialized));
	}
}
