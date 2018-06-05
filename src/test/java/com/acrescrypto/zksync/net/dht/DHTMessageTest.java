package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageCallback;
import com.acrescrypto.zksync.utility.Util;

public class DHTMessageTest {
	class DummyClient extends DHTClient {
		ArrayList<DatagramPacket> packets = new ArrayList<>();
		DHTMessage watch;
		
		protected DummyClient() {
			this.crypto = new CryptoSupport();
			this.key = this.crypto.makePrivateDHKey();
			this.tagKey = new Key(crypto);
			this.routingTable = new DummyRoutingTable();
			routingTable.client = this;
		}
		
		@Override
		protected void watchForResponse(DHTMessage message, DatagramPacket packet) {
			watch = message;
		}
		
		@Override
		protected void sendDatagram(DatagramPacket packet) {
			packets.add(packet);
		}
	}
	
	class DummyRoutingTable extends DHTRoutingTable {
		@Override public DHTPeer peerForMessage(String address, int port, PublicDHKey pubKey) {
			return new DHTPeer(client, address, port, pubKey.getBytes());
		}
	}
	
	class DummyRecord extends DHTRecord {
		byte[] contents;
		boolean reachable = true, valid = true;
		
		public DummyRecord(int i) {
			ByteBuffer buf = ByteBuffer.allocate(32);
			buf.putInt(i);
			buf.put(client.crypto.prng(ByteBuffer.allocate(4).putInt(i).array()).getBytes(buf.remaining()));
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

		@Override public boolean isValid() { return valid; }
		@Override public boolean isReachable() { return reachable; }
		public boolean equals(Object o) { return Arrays.equals(contents, ((DummyRecord) o).contents); }
		public int hashCode() { return ByteBuffer.wrap(contents).getInt(); }
	}
	
	CryptoSupport crypto;
	DummyClient client;
	DHTPeer peer;
	HashMap<Integer,PrivateDHKey> testKeys = new HashMap<>();
	
	public DHTID makeId() {
		return new DHTID(crypto.rng(crypto.hashLength()));
	}
	
	public PrivateDHKey privateKeyForPeer(int i) {
		testKeys.putIfAbsent(i, crypto.makePrivateDHKey());
		return testKeys.get(i);
	}
	
	public DHTPeer makeTestPeer(int i) {
		return new DHTPeer(client, "10.0.0."+i, 1000+i, privateKeyForPeer(i).publicKey().getBytes());
	}
	
	public byte[] decodePacket(PrivateDHKey recvKey, DHTMessage msg, DatagramPacket packet) {
		ByteBuffer sent = ByteBuffer.wrap(packet.getData());
		
		assertEquals(msg.peer.port, packet.getPort());
		assertEquals(msg.peer.address, packet.getAddress().getHostAddress());
		
		assertArrayEquals(msg.peer.key.getBytes(), recvKey.publicKey().getBytes());
		byte[] ephPubKeyRaw = new byte[crypto.asymPublicDHKeySize()];
		sent.get(ephPubKeyRaw);		
		PublicDHKey ephPubKey = crypto.makePublicDHKey(ephPubKeyRaw);
		ByteBuffer keyMaterial = ByteBuffer.allocate(8+crypto.asymDHSecretSize());
		keyMaterial.putLong(0);
		keyMaterial.put(recvKey.sharedSecret(ephPubKey));
		byte[] symKey = peer.client.crypto.makeSymmetricKey(keyMaterial.array());
		Key key = new Key(crypto, symKey);
		
		byte[] plaintext = key.decrypt(new byte[crypto.symIvLength()], sent.array(), sent.position(), sent.remaining());
		ByteBuffer ptBuf = ByteBuffer.wrap(plaintext);
		
		byte[] senderKey = new byte[crypto.asymPublicDHKeySize()];
		byte[] authTag = new byte[DHTClient.AUTH_TAG_SIZE];
		
		ptBuf.get(senderKey);
		ptBuf.get(authTag);
		
		assertArrayEquals(client.key.publicKey().getBytes(), senderKey);
		assertArrayEquals(msg.authTag, authTag);
		assertEquals(msg.msgId, ptBuf.getInt());
		assertEquals(msg.cmd, ptBuf.get());
		assertEquals(msg.flags, ptBuf.get());
		assertEquals(client.packets.size(), ptBuf.get()); // handcuffs test style a bit, but how else to validate this?
		
		byte[] payload = new byte[ptBuf.remaining()];
		ptBuf.get(payload);
		return payload;
	}

	@Before
	public void beforeEach() {
		crypto = new CryptoSupport();
		client = new DummyClient();
		peer = makeTestPeer(0);
	}
	
	@After
	public void afterEach() {
		Util.setCurrentTimeNanos(-1);
	}
	
	@Test
	public void testConstructorWithPayloadArray() {
		byte[] payload = crypto.rng(32);
		byte cmd = DHTMessage.CMD_FIND_NODE;
		DHTMessageCallback callback = (resp)->{};
		
		DHTMessage msg = new DHTMessage(peer, cmd, payload, callback);
		assertEquals(peer, msg.peer);
		assertEquals(cmd, msg.cmd);
		assertEquals(0, msg.flags);
		assertEquals(callback, msg.callback);
		assertNotEquals(payload, msg.payload);
		assertArrayEquals(payload, msg.payload);
		assertNotEquals(0, msg.msgId);
	}
	
	@Test
	public void testConstructorWithPayloadBuffer() {
		byte[] actualPayload = crypto.rng(32);
		ByteBuffer payload = ByteBuffer.allocate(64);
		payload.position(16);
		payload.put(actualPayload);
		payload.limit(payload.position());
		payload.position(16);
		
		byte cmd = DHTMessage.CMD_FIND_NODE;
		DHTMessageCallback callback = (resp)->{};
		
		DHTMessage msg = new DHTMessage(peer, cmd, payload, callback);
		assertEquals(peer, msg.peer);
		assertEquals(cmd, msg.cmd);
		assertEquals(0, msg.flags);
		assertEquals(callback, msg.callback);
		assertArrayEquals(actualPayload, msg.payload);
		assertNotEquals(0, msg.msgId);
	}
	
	@Test
	public void testConstructorWithItemList() {
		int numItems = 4;
		ArrayList<DHTRecord> items = new ArrayList<>(numItems);
		for(int i = 0; i < numItems; i++) {
			items.add(new DummyRecord(i));
		}

		byte cmd = DHTMessage.CMD_FIND_NODE;
		int msgId = 1234;

		DHTMessage msg = new DHTMessage(peer, cmd, msgId, items);
		assertEquals(peer, msg.peer);
		assertEquals(cmd, msg.cmd);
		assertEquals(msgId, msg.msgId);
		assertEquals(DHTMessage.FLAG_RESPONSE, msg.flags);
		assertFalse(items == msg.items);
		assertEquals(items.size(), msg.items.size());
		assertTrue(items.containsAll(msg.items));
	}
	
	@Test
	public void testConstructorWithArrayAssignsMsgId() {
		DHTMessage msg0 = new DHTMessage(peer, DHTMessage.CMD_PING, new byte[0], (resp)->{});
		DHTMessage msg1 = new DHTMessage(peer, DHTMessage.CMD_PING, new byte[0], (resp)->{});
		assertNotEquals(msg0.msgId, msg1.msgId);
	}
	
	@Test
	public void testConstructorWithBufferAssignsMsgId() {
		DHTMessage msg0 = new DHTMessage(peer, DHTMessage.CMD_PING, ByteBuffer.allocate(0), (resp)->{});
		DHTMessage msg1 = new DHTMessage(peer, DHTMessage.CMD_PING, ByteBuffer.allocate(0), (resp)->{});
		assertNotEquals(msg0.msgId, msg1.msgId);
	}
	
	@Test
	public void testMakeResponseCreatesMessageOfSameCmdAndIdToSender() {
		ArrayList<DHTPeer> peerList = new ArrayList<DHTPeer>();
		for(int i = 0; i < 4; i++) {
			peerList.add(makeTestPeer(i+1));
		}
		
		DHTMessage req = new DHTMessage(peer, DHTMessage.CMD_FIND_NODE, new byte[0], (resp)->{});
		DHTMessage resp = req.makeResponse(peerList);
		
		assertEquals(req.msgId, resp.msgId);
		assertEquals(req.cmd, resp.cmd);
		assertEquals(peer, resp.peer);
		assertEquals(DHTMessage.FLAG_RESPONSE, resp.flags);
		assertFalse(peerList == resp.items);
		assertEquals(peerList, resp.items);
	}
	
	@Test
	public void testSendNotifiesClientToWatchForResponse() {
		DHTMessage req = new DHTMessage(peer, DHTMessage.CMD_FIND_NODE, new byte[0], (resp)->{});
		req.send();
		assertEquals(req, client.watch);
	}
	
	@Test
	public void testSendTransmitsEmptyPayloadIfItemsIsNull() {
		DHTMessage req = new DHTMessage(peer, DHTMessage.CMD_FIND_NODE, new byte[0], (resp)->{});
		req = req.makeResponse(null);
		req.send();
		
		byte[] payload = decodePacket(privateKeyForPeer(0), req, client.packets.get(0));
		assertEquals(0, payload.length);
	}
	
	@Test
	public void testSerializesItemsIntoDatagrams() throws UnsupportedProtocolException {
		int numRecords = 64;
		ArrayList<DHTRecord> items = new ArrayList<>();
		for(int i = 0; i < numRecords; i++) {
			items.add(new DummyRecord(i));
		}
		
		HashSet<DummyRecord> set = new HashSet<>();
		DHTMessage req = new DHTMessage(peer, DHTMessage.CMD_GET_RECORDS, 1234, items);
		req.send();
		
		for(DatagramPacket packet : client.packets) {
			ByteBuffer buf = ByteBuffer.wrap(decodePacket(privateKeyForPeer(0), req, packet));
			while(buf.hasRemaining()) {
				buf.getShort();
				DummyRecord record = new DummyRecord(buf);
				assertFalse(set.contains(record));
				set.add(record);
			}
		}
		
		assertEquals(numRecords, set.size());
	}
	
	@Test
	public void testSerializesPayloadIntoDatagram() {
		byte[] payload = crypto.rng(32);
		DHTMessage req = new DHTMessage(peer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		assertArrayEquals(payload, decodePacket(privateKeyForPeer(0), req, client.packets.get(0)));
	}
	
	@Test
	public void testDeserializesMessagesToClient() throws ProtocolViolationException {
		byte[] payload = crypto.rng(32);
		DHTPeer localPeer = new DHTPeer(client, "localhost", 12345, client.key.publicKey().getBytes());
		DHTMessage req = new DHTMessage(localPeer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		
		byte[] serialized = client.packets.get(0).getData();
		DHTMessage deserialized = new DHTMessage(client, "127.0.0.1", 54321, ByteBuffer.wrap(serialized));
		assertArrayEquals(req.authTag, deserialized.authTag);
		assertEquals(req.msgId, deserialized.msgId);
		assertEquals(req.cmd, deserialized.cmd);
		assertEquals(req.flags, deserialized.flags);
		assertEquals(client.packets.size(), deserialized.numExpected);
		assertArrayEquals(payload, deserialized.payload);
		
		assertEquals("127.0.0.1", deserialized.peer.address);
		assertEquals(54321, deserialized.peer.port);
		assertArrayEquals(client.key.publicKey().getBytes(), req.peer.key.getBytes());
	}
	
	@Test
	public void testDeserializeThrowsProtocolViolationExceptionIfTampered() {
		byte[] payload = crypto.rng(32);
		DHTPeer localPeer = new DHTPeer(client, "localhost", 12345, client.key.publicKey().getBytes());
		DHTMessage req = new DHTMessage(localPeer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		
		byte[] serialized = client.packets.get(0).getData();
		for(int i = 0; i < 8*serialized.length; i++) {
			// Curve25519 keys are actually 255 bits, so the little endian MSB can be modified with no effect
			if(i == 8*crypto.asymPublicDHKeySize()-1) continue;
			
			serialized[i/8] ^= (1 << (i%8));
			try {
				new DHTMessage(client, "127.0.0.1", 54321, ByteBuffer.wrap(serialized));
				fail();
			} catch(ProtocolViolationException exc) {}
			serialized[i/8] ^= (1 << (i%8));
		}
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testDeserializeThrowsProtocolViolationExceptionIfTruncatedCiphertext() throws ProtocolViolationException {
		byte[] payload = crypto.rng(32);
		DHTPeer localPeer = new DHTPeer(client, "localhost", 12345, client.key.publicKey().getBytes());
		DHTMessage req = new DHTMessage(localPeer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		
		ByteBuffer buf = ByteBuffer.wrap(client.packets.get(0).getData());
		buf.limit(buf.limit()-1);
		new DHTMessage(client, "127.0.0.1", 54321, buf);
	}

	@Test(expected=ProtocolViolationException.class)
	public void testDeserializeThrowsProtocolViolationExceptionIfTruncatedKey() throws ProtocolViolationException {
		byte[] payload = crypto.rng(32);
		DHTPeer localPeer = new DHTPeer(client, "localhost", 12345, client.key.publicKey().getBytes());
		DHTMessage req = new DHTMessage(localPeer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		
		ByteBuffer buf = ByteBuffer.wrap(client.packets.get(0).getData());
		buf.limit(crypto.asymPublicDHKeySize()-1);
		new DHTMessage(client, "127.0.0.1", 54321, buf);
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testDeserializeThrowsProtocolViolationExceptionIfTruncatedPlaintext() throws ProtocolViolationException {
		DHTMessage req = new DHTMessage(peer, DHTMessage.CMD_ADD_RECORD, new byte[0], (resp)->{});
		PrivateDHKey ephKey = peer.client.crypto.makePrivateDHKey();
		byte[] symKeyRaw = peer.client.crypto.makeSymmetricKey(ephKey.sharedSecret(client.key.publicKey()));
		Key symKey = new Key(peer.client.crypto, symKeyRaw);
		
		ByteBuffer plaintext = ByteBuffer.allocate(req.headerSize()-1);
		plaintext.put(peer.client.key.publicKey().getBytes());
		plaintext.putInt(1234);
		
		byte[] ciphertext = symKey.encrypt(new byte[peer.client.crypto.symIvLength()], plaintext.array(), 0);
		
		ByteBuffer serialized = ByteBuffer.allocate(ephKey.getBytes().length + ciphertext.length);
		serialized.put(ephKey.publicKey().getBytes());
		serialized.put(ciphertext);
		serialized.rewind();
		
		new DHTMessage(client, "127.0.0.1", 54321, serialized);
	}
	
	@Test
	public void testSendsRemoteAuthTagIfRequest() {
		peer.remoteAuthTag = crypto.rng(DHTClient.AUTH_TAG_SIZE);
		DHTMessage req = new DHTMessage(peer, DHTMessage.CMD_ADD_RECORD, new byte[0], (resp)->{});
		assertArrayEquals(req.authTag, peer.remoteAuthTag);
	}
	
	@Test
	public void testSendsLocalAuthTagIfResponse() {
		peer.remoteAuthTag = crypto.rng(DHTClient.AUTH_TAG_SIZE);
		DHTMessage resp = new DHTMessage(peer, DHTMessage.CMD_ADD_RECORD, 0, new ArrayList<DHTRecord>(0));
		assertArrayEquals(resp.authTag, peer.localAuthTag());
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testAssertValidAuthTagThrowsProtocolViolationExceptionIfAuthTagNotValid() throws ProtocolViolationException {
		peer.remoteAuthTag = crypto.rng(DHTClient.AUTH_TAG_SIZE);
		DHTMessage req = new DHTMessage(peer, DHTMessage.CMD_ADD_RECORD, new byte[0], (resp)->{});
		req.assertValidAuthTag();
	}

	@Test
	public void testAssertValidAuthTagReturnsCleanIfAuthTagValid() throws ProtocolViolationException {
		peer.remoteAuthTag = peer.localAuthTag();
		DHTMessage req = new DHTMessage(peer, DHTMessage.CMD_ADD_RECORD, new byte[0], (resp)->{});
		req.assertValidAuthTag();
	}
}
