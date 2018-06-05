package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.utility.Util;

public class DHTMessageStubTest {
	class DummyClient extends DHTClient {
		DHTMessageStub missed;
		DatagramPacket sent;
		
		public DummyClient() {
			this.crypto = new CryptoSupport();
			this.tagKey = new Key(crypto);
			this.key = crypto.makePrivateDHKey();
		}
		
		@Override public void missedResponse(DHTMessageStub stub) { this.missed = stub; }
		@Override public void sendDatagram(DatagramPacket packet) { this.sent = packet; }
	}
	
	CryptoSupport crypto;
	DummyClient client;
	DHTPeer peer;
	DHTMessage req;
	DHTMessage resp;
	DHTMessageStub stub;
	
	DHTMessage makeResponse() {
		DHTMessage resp = req.makeResponse(null);
		resp.numExpected = 4;
		return resp;
	}
	
	@BeforeClass
	public static void beforeAll() {
		DHTClient.MESSAGE_EXPIRATION_TIME_MS = 100;
		DHTClient.MESSAGE_RETRY_TIME_MS = 50;
	}
	
	@Before
	public void beforeEach() throws UnknownHostException {
		crypto = new CryptoSupport();
		client = new DummyClient();
		peer = new DHTPeer(client, "127.0.0.1", 12345, crypto.rng(crypto.asymPublicDHKeySize()));
		req = new DHTMessage(peer, DHTMessage.CMD_GET_RECORDS, new byte[0], (response)->{resp = response;});
		
		byte[] serialized = req.serialize(1, ByteBuffer.allocate(0));
		
		InetAddress address = InetAddress.getByName(peer.address);
		DatagramPacket packet = new DatagramPacket(serialized, serialized.length, address, peer.port);
		stub = new DHTMessageStub(req, packet);
	}
	
	@AfterClass
	public static void afterAll() {
		DHTClient.MESSAGE_EXPIRATION_TIME_MS = DHTClient.DEFAULT_MESSAGE_EXPIRATION_TIME_MS;
		DHTClient.MESSAGE_RETRY_TIME_MS = DHTClient.DEFAULT_MESSAGE_RETRY_TIME_MS;
	}
	
	@Test
	public void testConstructorSetsFields() {
		assertEquals(peer, stub.peer);
		assertEquals(req.cmd, stub.cmd);
		assertEquals(req.msgId, stub.msgId);
		assertNotNull(stub.packet);
		assertNotNull(stub.callback);
	}
	
	@Test
	public void testConstructorSetsRetryTimerToResendMessage() {
		assertNull(client.sent);
		assertTrue(Util.waitUntil(DHTClient.MESSAGE_RETRY_TIME_MS+5, ()->stub.packet.equals(client.sent)));
	}
	
	@Test
	public void testConstructorSetsExpirationTimerToMarkMessageAsMissed() {
		assertNull(client.missed);
		assertTrue(Util.waitUntil(DHTClient.MESSAGE_EXPIRATION_TIME_MS+5, ()->stub.equals(client.missed)));
	}
	
	@Test
	public void testDispatchResponseIfMatchesIgnoresMessageIfNoMsgIdMatch() throws ProtocolViolationException {
		DHTMessage fakeResponse = makeResponse();
		fakeResponse.msgId++;
		stub.dispatchResponseIfMatches(fakeResponse);
		assertNull(resp);
	}
	
	@Test
	public void testDispatchResponseIfMatchesIgnoresMessageIfNoCmdMatch() throws ProtocolViolationException {
		DHTMessage fakeResponse = makeResponse();
		fakeResponse.cmd++;
		stub.dispatchResponseIfMatches(fakeResponse);
		assertNull(resp);
	}
	
	@Test
	public void testDispatchResponseIfMatchesIgnoresMessageIfNoPeerMatch() throws ProtocolViolationException {
		DHTMessage fakeResponse = makeResponse();
		fakeResponse.peer = new DHTPeer(client, "10.0.0.1", 12345, crypto.rng(crypto.asymPublicDHKeySize()));
		stub.dispatchResponseIfMatches(fakeResponse);
		assertNull(resp);
	}
	
	@Test
	public void testDispatchResponseIfMatchesCancelsExpirationMonitorIfMatch() throws ProtocolViolationException {
		stub.dispatchResponseIfMatches(makeResponse());
		assertFalse(Util.waitUntil(DHTClient.MESSAGE_EXPIRATION_TIME_MS+5, ()->stub.equals(client.missed)));
	}
	
	@Test
	public void testDispatchResponseIfMatchesDoesNotMarkMessageFinalIfNotLastExpected() throws ProtocolViolationException {
		stub.dispatchResponseIfMatches(makeResponse());
		assertFalse(resp.isFinal);
	}
	
	@Test
	public void testDispatchResponseIfMatchesMarksMessageFinalIfLastExpected() throws ProtocolViolationException {
		DHTMessage response = makeResponse();
		for(int i = 0; i < response.numExpected; i++) {
			assertTrue(resp == null || !resp.isFinal);
			stub.dispatchResponseIfMatches(response);
		}
		
		assertTrue(resp.isFinal);
	}
	
	@Test
	public void testDispatchResponseIfMatchesInvokesCallback() throws ProtocolViolationException {
		DHTMessage response = makeResponse();
		stub.dispatchResponseIfMatches(response);
		assertEquals(resp, response);
	}
}
