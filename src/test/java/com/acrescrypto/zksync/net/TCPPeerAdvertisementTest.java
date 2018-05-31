package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;

public class TCPPeerAdvertisementTest {
	static byte[] pubKey;
	static byte[] encryptedArchiveId;
	static String host;
	static int port;
	static CryptoSupport crypto;

	TCPPeerAdvertisement ad;
	Blacklist blacklist;
	
	@BeforeClass
	public static void beforeAll() {
		crypto = new CryptoSupport();
		pubKey = crypto.rng(crypto.asymPublicSigningKeySize());
		host = "localhost";
		port = 12345;
		encryptedArchiveId = crypto.hash(new byte[0]);
	}
	
	@Before
	public void beforeEach() throws UnconnectableAdvertisementException, IOException, InvalidBlacklistException {
		blacklist = new Blacklist(new RAMFS(), "blacklist", new Key(crypto, crypto.makeSymmetricKey()));
		ad = new TCPPeerAdvertisement(crypto.makePublicDHKey(pubKey), host, port, encryptedArchiveId).resolve();
	}
	
	@Test
	public void testSerialization() throws UnconnectableAdvertisementException {
		TCPPeerAdvertisement deserialized = new TCPPeerAdvertisement(crypto, ByteBuffer.wrap(ad.serialize()));
		assertEquals(ad.host, deserialized.host);
		assertEquals(ad.port, deserialized.port);
		assertTrue(Arrays.equals(ad.encryptedArchiveId, deserialized.encryptedArchiveId));
		assertTrue(Arrays.equals(ad.pubKey.getBytes(), deserialized.pubKey.getBytes()));
	}
	
	@Test
	public void testIsBlacklistedReturnsTrueIfIPIsBlacklisted() throws UnconnectableAdvertisementException, IOException, InvalidBlacklistException {
		ad = new TCPPeerAdvertisement(crypto.makePublicDHKey(pubKey), "127.0.0.1", port, encryptedArchiveId).resolve();
		blacklist.add(ad.ipAddress, 1000);
		assertTrue(ad.isBlacklisted(blacklist));
	}
	
	@Test
	public void testIsBlacklistedReturnsFalseIfIpIsNotBlacklisted() throws IOException, UnconnectableAdvertisementException {
		ad = new TCPPeerAdvertisement(crypto.makePublicDHKey(pubKey), "127.0.0.1", port, encryptedArchiveId).resolve();
		assertFalse(ad.isBlacklisted(blacklist));
	}
	
	@Test
	public void testIsBlacklistedReturnsTrueIfHostnameResolvesToBlacklistedIP() throws IOException {
		assertFalse(ad.isBlacklisted(blacklist));
		blacklist.add(ad.ipAddress, 5000);
		assertTrue(ad.isBlacklisted(blacklist));
	}
	
	@Test
	public void testHashCodeIsBasedOnPublicKey() throws UnconnectableAdvertisementException {
		PublicDHKey modifiedPubKey = crypto.makePrivateDHKey().publicKey();
		assertNotEquals(ad.hashCode(), new TCPPeerAdvertisement(modifiedPubKey, ad.host, ad.port, encryptedArchiveId).hashCode());
	}
	
	@Test
	public void testHashCodeIsBasedOnPort() throws UnconnectableAdvertisementException {
		assertNotEquals(ad.hashCode(), new TCPPeerAdvertisement(ad.pubKey, "127.0.0.2", ad.port, encryptedArchiveId).hashCode());
	}

	@Test
	public void testHashCodeIsBasedOnHostname() throws UnconnectableAdvertisementException {
		assertNotEquals(ad.hashCode(), new TCPPeerAdvertisement(ad.pubKey, ad.host, ad.port+1, encryptedArchiveId).hashCode());
	}
	
	@Test
	public void testHashCodeIsBasedOnEncryptedArchiveId() throws UnconnectableAdvertisementException {
		byte[] modifiedEncryptedArchiveId = encryptedArchiveId.clone();
		modifiedEncryptedArchiveId[3] ^= 0x04;
		assertNotEquals(ad.hashCode(), new TCPPeerAdvertisement(ad.pubKey, ad.host, ad.port, modifiedEncryptedArchiveId).hashCode());
	}
	
	@Test
	public void testHashCodeIsNotBasedOnInstance() throws UnconnectableAdvertisementException {
		assertEquals(ad.hashCode(), new TCPPeerAdvertisement(ad.pubKey, ad.host, ad.port, encryptedArchiveId).hashCode());
	}
	
	@Test
	public void testMatchesAddressReturnsTrueIfAddressEqualsHost() {
		assertTrue(ad.matchesAddress(host));
	}
	
	@Test
	public void testMatchesAddressReturnsTrueIfHostResolvesToAddress() {
		assertTrue(ad.matchesAddress("127.0.0.1"));
	}
	
	@Test
	public void testMatchesAddressReturnsFalseIfHostDoesNotResolveToAddress() {
		assertFalse(ad.matchesAddress("127.0.0.2"));
	}
	
	@Test
	public void testGetTypeReturnsTypeTCPPeer() {
		assertEquals(PeerAdvertisement.TYPE_TCP_PEER, ad.getType());
	}
	
	@Test
	public void testGetVersionReturnsVersion() {
		assertEquals(0, ad.getVersion());
	}
	
	@Test(expected=UnconnectableAdvertisementException.class)
	public void testConstructorThrowsExceptionIfUnsupportedVersion() throws UnconnectableAdvertisementException {
		ad.version = 1;
		byte[] serialized = ad.serialize();
		new TCPPeerAdvertisement(crypto, ByteBuffer.wrap(serialized));
	}
}
