package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.LinkedList;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigDefaults;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksync.utility.Util;

public class DHTMessageStubTest {
	class DummyMaster extends ZKMaster {
		public DummyMaster() {
			this.storage = new RAMFS();
			try {
				this.globalConfig = new ConfigFile(storage, "config.json");
			} catch (IOException exc) {
				fail();
			}
			globalConfig.apply(ConfigDefaults.getActiveDefaults());
		}
		
		@Override
		public void close() {}
	}

	class DummyClient extends DHTClient {
		DHTMessageStub missed;
		LinkedList<DatagramPacket> sent = new LinkedList<>();
		
		public DummyClient() {
			this.master          = new DummyMaster();
			this.crypto          = CryptoSupport.defaultCrypto();
			this.tagKey          = new Key(crypto);
			this.networkId       = new byte[crypto.hashLength()];
			this.privateKey      = crypto.makePrivateDHKey();
			
			this.socketManager   = new DummySocketManager(this);
			this.protocolManager = new DummyProtocolManager(this);
		}
	}
	
	class DummyProtocolManager extends DHTProtocolManager {
		DummyClient client;
		
		public DummyProtocolManager(DummyClient client) {
			this.client = client;
		}

		@Override public void missedResponse(DHTMessageStub stub) { client.missed = stub; }
	}
	
	class DummySocketManager extends DHTSocketManager {
		DummyClient client;
		
		public DummySocketManager(DummyClient client) {
			this.client = client;
		}

		@Override public void sendDatagram(DatagramPacket packet) { client.sent.add(packet); }
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
		TestUtils.startDebugMode();
		ConfigDefaults.getActiveDefaults().setDefault("net.dht.messageExpirationTimeMs", 100);
		ConfigDefaults.getActiveDefaults().setDefault("net.dht.messageRetryTimeMs",       50);
	}
	
	@Before
	public void beforeEach() throws UnknownHostException {
		crypto = CryptoSupport.defaultCrypto();
		client = new DummyClient();
		peer = new DHTPeer(client, "127.0.0.1", 12345, crypto.rng(crypto.asymPublicDHKeySize()));
		req = new DHTMessage(peer, DHTMessage.CMD_FIND_NODE, new byte[0], (response)->{resp = response;});
		stub = new DHTMessageStub(req);
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Test
	public void testConstructorSetsFields() {
		assertEquals(req, stub.msg);
		assertNotNull(stub.callback);
	}
	
	@Test
	public void testConstructorSetsRetryTimerToResendMessage() {
		int messageRetryTimeMs = client.getMaster().getGlobalConfig().getInt("net.dht.messageRetryTimeMs");
		assertTrue(client.sent.isEmpty());
		assertTrue(Util.waitUntil(messageRetryTimeMs+50, ()->client.sent.size() > 0));
	}
	
	@Test
	public void testConstructorSetsExpirationTimerToMarkMessageAsMissed() {
		int messageExpirationTimeMs = client.getMaster().getGlobalConfig().getInt("net.dht.messageExpirationTimeMs");
		assertNull(client.missed);
		assertTrue(Util.waitUntil(messageExpirationTimeMs+50, ()->stub.equals(client.missed)));
	}
	
	@Test
	public void testMatchesMessageRetursnFalseIfNoMsgIdMatch() throws ProtocolViolationException {
		DHTMessage fakeResponse = makeResponse();
		fakeResponse.msgId++;
		assertFalse(stub.matchesMessage(fakeResponse));
	}
	
	@Test
	public void testMatchesMessageRetursnFalseIfNoCmdMatch() throws ProtocolViolationException {
		DHTMessage fakeResponse = makeResponse();
		fakeResponse.cmd++;
		assertFalse(stub.matchesMessage(fakeResponse));
	}
	
	@Test
	public void testMatchesMessageRetursnFalseIfNoPeerMatch() throws ProtocolViolationException, UnknownHostException {
		DHTMessage fakeResponse = makeResponse();
		fakeResponse.peer = new DHTPeer(client, "10.0.0.1", 12345, crypto.rng(crypto.asymPublicDHKeySize()));
		assertFalse(stub.matchesMessage(fakeResponse));
	}
	
	@Test
	public void testDispatchResponseIfMatchesCancelsExpirationMonitorIfMatch() throws ProtocolViolationException {
		int messageExpirationTimeMs = client.getMaster().getGlobalConfig().getInt("net.dht.messageExpirationTimeMs");
		stub.dispatchResponse(makeResponse());
		assertFalse(Util.waitUntil(messageExpirationTimeMs+50, ()->stub.equals(client.missed)));
	}
	
	@Test
	public void testDispatchResponseIfMatchesDoesNotMarkMessageFinalIfNotLastExpected() throws ProtocolViolationException {
		stub.dispatchResponse(makeResponse());
		assertFalse(resp.isFinal);
	}
	
	@Test
	public void testDispatchResponseIfMatchesMarksMessageFinalIfLastExpected() throws ProtocolViolationException {
		DHTMessage response = makeResponse();
		for(int i = 0; i < response.numExpected; i++) {
			assertTrue(resp == null || !resp.isFinal);
			stub.dispatchResponse(response);
		}
		
		assertTrue(resp.isFinal);
	}
	
	@Test
	public void testDispatchResponseIfMatchesInvokesCallback() throws ProtocolViolationException {
		DHTMessage response = makeResponse();
		stub.dispatchResponse(response);
		assertEquals(resp, response);
	}
	
	@Test
	public void testRetryReencryptsMessage() {
		stub.retry();
		stub.retry();
		stub.retry();
		
		assertEquals(3, client.sent.size());
		DatagramPacket a = client.sent.get(0),
				       b = client.sent.get(1),
				       c = client.sent.get(2);
		
		assertFalse(Arrays.equals(a.getData(), b.getData()));
		assertFalse(Arrays.equals(a.getData(), c.getData()));
		assertFalse(Arrays.equals(b.getData(), c.getData()));
		
		int matchedSizes = 0;
		if(a.getLength() == b.getLength()) matchedSizes += 1;
		if(a.getLength() == c.getLength()) matchedSizes += 1;
		if(b.getLength() == c.getLength()) matchedSizes += 1;
		
		assertTrue(matchedSizes <= 1);
	}
}
