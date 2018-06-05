package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageCallback;
import com.acrescrypto.zksync.utility.Util;

public class DHTPeerTest {
	class DummyClient extends DHTClient {
		DummyMessage msg;
		DHTPeer reqPeer;
		DHTID reqId;
		DHTRecord reqRecord;
		DummyRoutingTable routingTable;
		
		public DummyClient() {
			this.crypto = DHTPeerTest.crypto;
			super.routingTable = this.routingTable = new DummyRoutingTable();
			this.id = new DHTID(crypto.rng(crypto.hashLength()));
			this.tagKey = new Key(crypto);
		}
		
		@Override
		protected DHTMessage pingMessage(DHTPeer recipient, DHTMessageCallback callback) {
			reqId = id;
			reqPeer = recipient;
			return msg = new DummyMessage(recipient, DHTMessage.CMD_PING, new byte[0], callback);
		}
		
		@Override
		protected DHTMessage findNodeMessage(DHTPeer recipient, DHTID id, DHTMessageCallback callback) {
			reqId = id;
			reqPeer = recipient;
			return msg = new DummyMessage(recipient, DHTMessage.CMD_FIND_NODE, id.rawId, callback);
		}
		
		@Override
		protected DHTMessage getRecordsMessage(DHTPeer recipient, DHTID id, DHTMessageCallback callback) {
			reqId = id;
			reqPeer = recipient;
			return msg = new DummyMessage(recipient, DHTMessage.CMD_GET_RECORDS, id.rawId, callback);
		}
		
		@Override
		protected DHTMessage addRecordMessage(DHTPeer recipient, DHTID id, DHTRecord record, DHTMessageCallback callback) {
			reqId = id;
			reqRecord = record;
			reqPeer = recipient;
			
			byte[] serializedRecord = record.serialize();
			ByteBuffer buf = ByteBuffer.allocate(id.rawId.length + serializedRecord.length + recipient.remoteAuthTag.length);
			buf.put(recipient.remoteAuthTag);
			buf.put(id.rawId);
			buf.put(record.serialize());
			return msg = new DummyMessage(recipient, DHTMessage.CMD_ADD_RECORD, buf.array(), callback);
		}
		
		@Override
		protected DHTRecord deserializeRecord(ByteBuffer serialized) throws UnsupportedProtocolException {
			return new DummyRecord(serialized);
		}
	}
	
	class DummyRecord extends DHTRecord {
		byte[] contents;
		
		public DummyRecord(int i) {
			ByteBuffer buf = ByteBuffer.allocate(32);
			buf.putInt(i);
			buf.put(crypto.prng(ByteBuffer.allocate(4).putInt(i).array()).getBytes(buf.remaining()));
			contents = buf.array();
		}
		
		public DummyRecord(ByteBuffer serialized) throws UnsupportedProtocolException {
			deserialize(serialized);
		}
		
		public void corrupt() {
			contents[0] |= 0x80;
		}

		@Override
		public byte[] serialize() {
			ByteBuffer serialized = ByteBuffer.allocate(2+contents.length);
			serialized.putShort((short) contents.length);
			serialized.put(contents);
			return serialized.array();
		}

		@Override
		public void deserialize(ByteBuffer serialized) throws UnsupportedProtocolException {
			int len = Util.unsignShort(serialized.getShort());
			contents = new byte[len];
			serialized.get(contents);
			if((contents[0] & 0x80) != 0) throw new UnsupportedProtocolException();
		}

		@Override public boolean isValid() { return true; }
		@Override public boolean isReachable() { return true; }
		public boolean equals(Object o) { return Arrays.equals(contents, ((DummyRecord) o).contents); }
	}
	
	class DummyRoutingTable extends DHTRoutingTable {
		ArrayList<DHTPeer> suggestedPeers = new ArrayList<>();
		
		@Override public void suggestPeer(DHTPeer peer) { suggestedPeers.add(peer); }
	}
	
	class DummyMessage extends DHTMessage {
		boolean sent;
		
		public DummyMessage(DHTPeer recipient, byte cmd, byte[] payload, DHTMessageCallback callback) {
			super(recipient, cmd, payload, callback);
		}
		
		@Override
		public void send() {
			sent = true;
		}
	}
	
	static CryptoSupport crypto;
	DummyClient client;
	
	public DHTID makeId() {
		return new DHTID(crypto.rng(crypto.hashLength()));
	}
	
	public DHTPeer makeTestPeer() {
		return makeTestPeer(0);
	}
	
	public DHTPeer makeTestPeer(int i) {
		byte[] seed = ByteBuffer.allocate(4).putInt(i).array();
		byte[] pubKey = crypto.prng(seed).getBytes(crypto.asymPublicDHKeySize());
		return new DHTPeer(client, "10.0.0."+i, 1000+i, pubKey);
	}
	
	public DHTMessage makeResponse(byte[] payload) {
		return makeResponse(payload, false);
	}
	
	public DHTMessage makeResponse(byte[] payload, boolean isFinal) {
		DHTMessage msg = new DHTMessage(client.reqPeer, client.msg.cmd, payload, null);
		msg.isFinal = isFinal;
		return msg;
	}
	
	public DummyRecord makeAdRecord(int i) {
		return new DummyRecord(i);
	}

	@BeforeClass
	public static void beforeAll() {
		crypto = new CryptoSupport();
	}
	
	@Before
	public void beforeEach() {
		client = new DummyClient();
	}
	
	@After
	public void afterEach() {
		Util.setCurrentTimeNanos(-1);
	}

	@Test
	public void testInitializeWithAddressInfo() {
		PublicDHKey pubKey = crypto.makePrivateDHKey().publicKey();
		DHTPeer peer = new DHTPeer(client, "10.0.0.1", 1000, pubKey.getBytes());
		
		assertEquals("10.0.0.1", peer.address);
		assertEquals(1000, peer.port);
		assertArrayEquals(pubKey.getBytes(), peer.key.getBytes());
		assertEquals(new DHTID(pubKey), peer.id);
	}
	
	@Test
	public void testSerialization() throws EINVALException {
		PublicDHKey pubKey = crypto.makePrivateDHKey().publicKey();
		DHTPeer peer = new DHTPeer(client, "10.0.0.1", 1000, pubKey.getBytes());
		
		DHTPeer deserialized = new DHTPeer(client, ByteBuffer.wrap(peer.serialize()));
		assertEquals(peer.address, deserialized.address);
		assertEquals(peer.port, deserialized.port);
		assertArrayEquals(peer.key.getBytes(), deserialized.key.getBytes());
		assertEquals(peer.id, deserialized.id);
		assertEquals(peer, deserialized);
	}
	
	@Test
	public void testIsBadReturnsFalseIfMissedFewerThanTwoMessages() {
		DHTPeer peer = makeTestPeer();
		assertFalse(peer.isBad());
		peer.missedMessage();
		assertFalse(peer.isBad());
	}
	
	@Test
	public void testIsBadReturnsTrueIfMissedTwoOrMoreMessages() {
		DHTPeer peer = makeTestPeer();
		peer.missedMessage();
		
		for(int i = 0; i < 32; i++) {
			peer.missedMessage();
			assertTrue(peer.isBad());
		}
	}
	
	@Test
	public void testAcknowledgedMessageClearsIsBad() {
		DHTPeer peer = makeTestPeer();
		peer.missedMessage();
		peer.missedMessage();
		
		assertTrue(peer.isBad());
		peer.acknowledgedMessage();
		assertFalse(peer.isBad());
	}
	
	@Test
	public void testAcknowledgedMessageUpdatesLastSeen() {
		DHTPeer peer = makeTestPeer();
		assertEquals(0, peer.lastSeen);
		
		Util.setCurrentTimeMillis(12345);
		peer.acknowledgedMessage();
		assertEquals(Util.currentTimeMillis(), peer.lastSeen);
	}

	@Test
	public void testPingSendsPingRequestToPeer() {
		DHTPeer peer = makeTestPeer();
			
		peer.ping();
		assertNotNull(client.msg);
		assertEquals(peer, client.reqPeer);
		assertEquals(DHTMessage.CMD_PING, client.msg.cmd);
		assertTrue(client.msg.sent);
	}
	
	@Test
	public void testFindNodeSendsFindNodeRequestToPeer() {
		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		
		peer.findNode(id, (results, isFinal)->{});
		assertNotNull(client.msg);
		assertEquals(peer, client.reqPeer);
		assertEquals(DHTMessage.CMD_FIND_NODE, client.msg.cmd);
		assertEquals(id, client.reqId);
		assertTrue(client.msg.sent);
	}
	
	@Test
	public void testFindNodeSuggestsDeserializedPeersToRoutingTable() throws ProtocolViolationException {
		int numRecords = 4;

		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		ArrayList<DHTPeer> respPeers = new ArrayList<>(numRecords);
		
		ByteBuffer response = ByteBuffer.allocate(numRecords*(2+peer.serialize().length));
		for(int i = 0; i < numRecords; i++) {
			DHTPeer newPeer = makeTestPeer(i+1);
			respPeers.add(newPeer);
			response.putShort((short) newPeer.serialize().length);
			response.put(newPeer.serialize());
		}
		
		peer.findNode(id, (results, isFinal)->{});
		client.msg.callback.responseReceived(makeResponse(response.array()));
		for(DHTPeer respPeer : respPeers) {
			assertTrue(client.routingTable.suggestedPeers.contains(respPeer));
		}
	}
	
	@Test
	public void testFindNodeReportsReceivedPeers() throws ProtocolViolationException {
		int numRecords = 4;

		MutableBoolean receivedCallback = new MutableBoolean();
		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		ArrayList<DHTPeer> respPeers = new ArrayList<>(numRecords);
		
		ByteBuffer response = ByteBuffer.allocate(numRecords*(2+peer.serialize().length));
		for(int i = 0; i < numRecords; i++) {
			DHTPeer newPeer = makeTestPeer(i+1);
			respPeers.add(newPeer);
			response.putShort((short) newPeer.serialize().length);
			response.put(newPeer.serialize());
		}
		
		peer.findNode(id, (results, isFinal)->{
			assert(results.size() == numRecords);
			receivedCallback.setTrue();
			for(DHTPeer result : results) {
				assertTrue(respPeers.contains(result));
			}
		});
		
		client.msg.callback.responseReceived(makeResponse(response.array()));
		assertTrue(receivedCallback.booleanValue());
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testFindNodeThrowsProtocolViolationExceptionIfRecordCantDeserialize() throws ProtocolViolationException {
		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		
		ByteBuffer response = ByteBuffer.allocate(2+peer.serialize().length-1);
		DHTPeer newPeer = makeTestPeer(1);
		response.putShort((short) newPeer.serialize().length);
		response.put(newPeer.serialize(), 0, response.remaining());
		
		peer.findNode(id, (results, isFinal)->{});
		client.msg.callback.responseReceived(makeResponse(response.array()));
	}
	
	@Test
	public void testFindNodeThrowsProtocolViolationIfRecordDoesntDeserializeInExpectedLength() throws ProtocolViolationException {
		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		
		ByteBuffer response = ByteBuffer.allocate(2+peer.serialize().length);
		DHTPeer newPeer = makeTestPeer(1);
		response.putShort((short) (newPeer.serialize().length+1));
		response.put(newPeer.serialize(), 0, response.remaining());
		
		peer.findNode(id, (results, isFinal)->{});
		try {
			client.msg.callback.responseReceived(makeResponse(response.array()));
			fail();
		} catch(ProtocolViolationException exc) {
		}

		response = ByteBuffer.allocate(2+peer.serialize().length);
		response.putShort((short) (newPeer.serialize().length-1));
		response.put(newPeer.serialize(), 0, response.remaining());
		
		peer.findNode(id, (results, isFinal)->{});
		try {
			client.msg.callback.responseReceived(makeResponse(response.array()));
			fail();
		} catch(ProtocolViolationException exc) {
		}
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testFindNodeThrowsProtocolViolationIfRecordHasZeroLength() throws ProtocolViolationException {
		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		
		DHTPeer newPeer = makeTestPeer(1);
		ByteBuffer response = ByteBuffer.allocate(2+newPeer.serialize().length+2);
		response.putShort((short) (newPeer.serialize().length));
		response.put(newPeer.serialize());
		response.putShort((short) 0);
		
		peer.findNode(id, (results, isFinal)->{});
		client.msg.callback.responseReceived(makeResponse(response.array()));
	}
	
	@Test
	public void testFindNodeHandlerDoesNotSetFinalFlagIfMessageNotMarkedFinal() throws ProtocolViolationException {
		MutableBoolean callbackReceived = new MutableBoolean();
		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		peer.findNode(id, (results, isFinal)->{
			assertFalse(isFinal);
			callbackReceived.setTrue();
		});
		
		assertFalse(callbackReceived.booleanValue());
		client.msg.callback.responseReceived(makeResponse(new byte[0], false));
		assertTrue(callbackReceived.booleanValue());
	}
	
	@Test
	public void testFindNodeHandlerSetsFinalFlagIfMessageMarkedFinal() throws ProtocolViolationException {
		MutableBoolean callbackReceived = new MutableBoolean();
		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		peer.findNode(id, (results, isFinal)->{
			assertTrue(isFinal);
			callbackReceived.setTrue();
		});
		
		assertFalse(callbackReceived.booleanValue());
		client.msg.callback.responseReceived(makeResponse(new byte[0], true));
		assertTrue(callbackReceived.booleanValue());
	}
	
	@Test
	public void testGetRecordsSendsGetRecordsRequestToPeer() {
		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		
		peer.getRecords(id, (results, isFinal)->{});
		assertNotNull(client.msg);
		assertEquals(peer, client.reqPeer);
		assertEquals(DHTMessage.CMD_GET_RECORDS, client.msg.cmd);
		assertEquals(id, client.reqId);
		assertTrue(client.msg.sent);
	}
	
	@Test
	public void testGetRecordsHandlerReportsReceivedRecords() throws ProtocolViolationException {
		int numRecords = 4;

		MutableBoolean receivedCallback = new MutableBoolean();
		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		ArrayList<DHTRecord> records = new ArrayList<>(numRecords);
		
		ByteBuffer response = ByteBuffer.allocate(numRecords*(2+makeAdRecord(0).serialize().length));
		for(int i = 0; i < numRecords; i++) {
			DHTRecord newRecord = makeAdRecord(i);
			records.add(newRecord);
			response.putShort((short) newRecord.serialize().length);
			response.put(newRecord.serialize());
		}
		
		peer.getRecords(id, (results, isFinal)->{
			assert(results.size() == numRecords);
			receivedCallback.setTrue();
			for(DHTRecord result : results) {
				assertTrue(records.contains(result));
			}
		});
		
		client.msg.callback.responseReceived(makeResponse(response.array()));
		assertTrue(receivedCallback.booleanValue());
	}
	
	@Test
	public void testGetRecordsHandlerToleratesUnreadableRecords() throws ProtocolViolationException {
		int numRecords = 4;

		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		ArrayList<DHTRecord> records = new ArrayList<>(numRecords);
		
		ByteBuffer response = ByteBuffer.allocate(numRecords*(2+makeAdRecord(0).serialize().length));
		for(int i = 0; i < numRecords; i++) {
			DummyRecord newRecord = makeAdRecord(i);
			if(i == 2) newRecord.corrupt();
			records.add(newRecord);
			response.putShort((short) newRecord.serialize().length);
			response.put(newRecord.serialize());
		}
		
		peer.getRecords(id, (results, isFinal)->{
			assertEquals(numRecords-1, results.size());
		});
		
		client.msg.callback.responseReceived(makeResponse(response.array()));
	}
	
	@Test
	public void testGetRecordsHandlerDoesNotSetFinalFlagIfMessageNotMarkedFinal() throws ProtocolViolationException {
		MutableBoolean callbackReceived = new MutableBoolean();
		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		peer.getRecords(id, (results, isFinal)->{
			assertFalse(isFinal);
			callbackReceived.setTrue();
		});
		
		assertFalse(callbackReceived.booleanValue());
		client.msg.callback.responseReceived(makeResponse(new byte[0], false));
		assertTrue(callbackReceived.booleanValue());
	}

	@Test
	public void testGetRecordsHandlerSetsFinalFlagIfMessageMarkedFinal() throws ProtocolViolationException {
		MutableBoolean callbackReceived = new MutableBoolean();
		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		peer.getRecords(id, (results, isFinal)->{
			assertTrue(isFinal);
			callbackReceived.setTrue();
		});
		
		assertFalse(callbackReceived.booleanValue());
		client.msg.callback.responseReceived(makeResponse(new byte[0], true));
		assertTrue(callbackReceived.booleanValue());
	}
	
	@Test
	public void testAddRecordSendsAddRecordRequestToPeer() {
		DHTPeer peer = makeTestPeer();
		DHTID id = makeId();
		DummyRecord record = makeAdRecord(0);
		peer.remoteAuthTag = crypto.rng(crypto.hashLength());
		
		peer.addRecord(id, record);
		assertNotNull(client.msg);
		assertEquals(peer, client.reqPeer);
		assertEquals(DHTMessage.CMD_ADD_RECORD, client.msg.cmd);
		assertEquals(id, client.reqId);
		assertEquals(record, client.reqRecord);
		assertTrue(client.msg.sent);
	}
	
	
	@Test
	public void testEqualsReturnsTrueIfArgumentIsPeerWithMatchingID() {
		assertEquals(makeTestPeer(0), makeTestPeer(0));
	}
	
	@Test
	public void testEqualsReturnsFalseIfArgumentIsPeerWithNonmatchingID() {
		assertNotEquals(makeTestPeer(0), makeTestPeer(1));
	}
	
	@Test
	public void testEqualsReturnsTrueIfArgumentIsMatchingID() {
		assertEquals(makeTestPeer(0), makeTestPeer(0).id);
	}
	
	@Test
	public void testEqualsReturnsFalseIfArgumentIsNonmatchingID() {
		assertNotEquals(makeTestPeer(0), makeTestPeer(1).id);
	}
	
	@Test
	public void testLocalAuthTagIsConstantForMatchingAddressPortAndKey() {
		byte[] pubKey = crypto.makePrivateDHKey().publicKey().getBytes(); 
		DHTPeer peer0 = new DHTPeer(client, "10.0.0.0", 1000, pubKey);
		DHTPeer peer1 = new DHTPeer(client, "10.0.0.0", 1000, pubKey);
		
		assertTrue(Arrays.equals(peer0.localAuthTag(), peer1.localAuthTag()));
	}
	
	@Test
	public void testLocalAuthTagIsVariableWithAddress() {
		byte[] pubKey = crypto.makePrivateDHKey().publicKey().getBytes(); 
		DHTPeer peer0 = new DHTPeer(client, "10.0.0.0", 1000, pubKey);
		DHTPeer peer1 = new DHTPeer(client, "10.0.0.1", 1000, pubKey);
		
		assertFalse(Arrays.equals(peer0.localAuthTag(), peer1.localAuthTag()));
	}
	
	@Test
	public void testLocalAuthTagIsVariableWithPort() {
		byte[] pubKey = crypto.makePrivateDHKey().publicKey().getBytes(); 
		DHTPeer peer0 = new DHTPeer(client, "10.0.0.0", 1000, pubKey);
		DHTPeer peer1 = new DHTPeer(client, "10.0.0.0", 1001, pubKey);
		
		assertFalse(Arrays.equals(peer0.localAuthTag(), peer1.localAuthTag()));
	}
	
	@Test
	public void testLocalAuthTagIsVariableWithKey() {
		DHTPeer peer0 = new DHTPeer(client, "10.0.0.0", 1000, crypto.makePrivateDHKey().publicKey().getBytes());
		DHTPeer peer1 = new DHTPeer(client, "10.0.0.0", 1001, crypto.makePrivateDHKey().publicKey().getBytes());
		
		assertFalse(Arrays.equals(peer0.localAuthTag(), peer1.localAuthTag()));
	}
}
