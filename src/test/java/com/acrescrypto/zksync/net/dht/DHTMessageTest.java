package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigDefaults;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageCallback;
import com.acrescrypto.zksync.utility.Util;

public class DHTMessageTest {
	class DummyMaster extends ZKMaster {
		public DummyMaster() {
			this.storage      = new RAMFS();
			try {
				this.globalConfig = new ConfigFile(storage, "config.json");
			} catch (IOException e) {
				fail();
			}
			
			globalConfig.apply(ConfigDefaults.getActiveDefaults());
		}
	}
	
	class DummyClient extends DHTClient {
		ArrayList<DatagramPacket> packets = new ArrayList<>();
		DHTMessage watch;
		
		protected DummyClient() {
			this.master          = new DummyMaster();
			this.crypto          = CryptoSupport.defaultCrypto();
			this.privateKey      = this.crypto.makePrivateDHKey();
			this.tagKey          = new  Key(crypto);
			this.networkId       = new byte[crypto.hashLength()];
			
			this.routingTable    = new DummyRoutingTable   (this);
			this.socketManager   = new DummySocketManager  (this);
			this.protocolManager = new DummyProtocolManager(this);
		}
	}
	
	class DummyProtocolManager extends DHTProtocolManager {
		DummyClient client;
		
		public DummyProtocolManager(DummyClient client) {
			super.client = this.client = client;
		}

		@Override
		protected void watchForResponse(DHTMessage message) {
			client.watch = message;
		}
	}
	
	class DummySocketManager extends DHTSocketManager {
		DummyClient client;
		
		public DummySocketManager(DummyClient client) {
			super.client = this.client = client;
		}

		@Override
		protected void sendDatagram(DatagramPacket packet) {
			client.packets.add(packet);
		}
	}
	
	class DummyRoutingTable extends DHTRoutingTable {
		public DummyRoutingTable(DummyClient client) {
			this.client = client;
		}
		
		@Override public DHTPeer peerForMessage(String address, int port, PublicDHKey pubKey) {
			try {
				return new DHTPeer(client, address, port, pubKey.getBytes());
			} catch(UnknownHostException exc) {
				fail();
				return null;
			}
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
		@Override public String routingInfo() { return ""; }
	}
	
	CryptoSupport crypto;
	DummyClient client;
	DHTPeer peer;
	HashMap<Integer,PrivateDHKey> testKeys = new HashMap<>();
	
	public DHTID makeId() {
		return DHTID.withBytes(crypto.rng(crypto.hashLength()));
	}
	
	public PrivateDHKey privateKeyForPeer(int i) {
		testKeys.putIfAbsent(i, crypto.makePrivateDHKey());
		return testKeys.get(i);
	}
	
	public DHTPeer makeTestPeer(int i) {
		try {
			return new DHTPeer(client, "10.0.0."+i, 1000+i, privateKeyForPeer(i).publicKey().getBytes());
		} catch(UnknownHostException exc) {
			fail();
			return null;
		}
	}
	
	public byte[] decodePacket(PrivateDHKey recvKey, DHTMessage msg, DatagramPacket packet) {
		ByteBuffer serialized = ByteBuffer.wrap(packet.getData());
		
		assertEquals     (msg.peer.port,           packet.getPort());
		assertEquals     (msg.peer.address,        packet.getAddress().getHostAddress());
		assertArrayEquals(msg.peer.key.getBytes(), recvKey.publicKey().getBytes());
		
		CryptoSupport crypto             = msg.peer.client.crypto;
		PrivateDHKey  localStaticPrivkey = privateKeyForPeer(peer.getPort()-1000);
		PublicDHKey   localStaticPubkey  = localStaticPrivkey.publicKey();
		
		byte[]        networkId          = peer.client.getNetworkId(),
				      rnd                = new byte[8],
				      blankIv            = new byte[crypto.symIvLength()],
				      obfuscatedEphKey   = new byte[crypto.asymPublicDHKeySize()],
				      encryptedStaticKey = new byte[crypto.asymPublicDHKeySize()];
		
		serialized.get(rnd);
		serialized.get(obfuscatedEphKey);
		serialized.get(encryptedStaticKey);
		
		Key[]         keys               = new Key[3];
		              keys[0]            = new Key(crypto, crypto.expand(
											Util.concat(
													rnd,
													networkId),
											crypto.symKeyLength(),
											localStaticPubkey.getBytes(),
											new byte[0]));
		byte[]        ephPubkeyRaw       = keys[0].decryptUnauthenticated(blankIv, obfuscatedEphKey);
		PublicDHKey   ephPubkey          = crypto.makePublicDHKey(ephPubkeyRaw);
		byte[]        ephSharedSecret    = localStaticPrivkey.sharedSecret(ephPubkey);
		
		              keys[1]            = new Key(crypto, crypto.expand(
				                            Util.concat(
				                            		keys[0].getRaw(),
				                            		rnd,
				                            		obfuscatedEphKey,
				                            		ephSharedSecret),
				                            crypto.symKeyLength(),
				                            localStaticPubkey.getBytes(),
				                            new byte[0]));
		byte[]        staticPubkeyRaw    = keys[1].decryptUnauthenticated(blankIv, encryptedStaticKey);
		PublicDHKey   remoteStaticPubkey = crypto.makePublicDHKey(staticPubkeyRaw);
		byte[]        staticSharedSecret = localStaticPrivkey.sharedSecret(remoteStaticPubkey);
		
		              keys[2]            = new Key(crypto, crypto.expand(
                                            Util.concat(
                                            		keys[1].getRaw(),
                                            		rnd,
                                            		obfuscatedEphKey,
                                            		encryptedStaticKey,
                                            		staticSharedSecret),
                                            crypto.symKeyLength(),
                                            localStaticPubkey.getBytes(),
                                            new byte[0]));
		
		byte[]        plaintextRaw       = keys[2].decrypt(
		                                     blankIv,
		                                     serialized.array(),
		                                     serialized.position(),
		                                     serialized.remaining());
		ByteBuffer ptBuf      = ByteBuffer.wrap(plaintextRaw);
		
		byte[] authTag        = new byte[DHTClient.AUTH_TAG_SIZE];
		
		ptBuf.get(authTag);
		
		assertArrayEquals(msg.authTag,                      authTag);
		assertEquals     (msg.msgId,                        ptBuf.getInt());
		assertEquals     (msg.timestamp,                    ptBuf.getLong());
		assertEquals     (msg.cmd,                          ptBuf.get());
		assertEquals     (msg.flags,                        ptBuf.get());
		assertEquals     (client.packets.size(),            ptBuf.get()); // handcuffs test style a bit, but how else to validate this?
		
		byte[] payload        = new byte[ptBuf.remaining()];
		ptBuf.get(payload);
		
		return payload;
	}

	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() {
		crypto = CryptoSupport.defaultCrypto();
		client = new DummyClient();
		peer = makeTestPeer(0);
	}
	
	@After
	public void afterEach() {
		Util.setCurrentTimeNanos(-1);
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
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
		assertFalse(items == msg.itemLists.get(0));
		assertEquals(items.size(), msg.itemLists.get(0).size());
		assertTrue(items.containsAll(msg.itemLists.get(0)));
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
		assertFalse(peerList == resp.itemLists.get(0));
		assertEquals(peerList, resp.itemLists.get(0));
	}
	
	@Test
	public void testSupplementaryListsAreAddedToSerialization() {
		int numItems = 4;
		@SuppressWarnings("unchecked")
		ArrayList<DHTRecord>[] itemsLists = new ArrayList[2];
		for(int i = 0; i < itemsLists.length; i++) {
			itemsLists[i] = new ArrayList<>(numItems);
			for(int j = 0; j < numItems + i; j++) {
				itemsLists[i].add(new DummyRecord(j + numItems*i));
			}
		}

		byte cmd = DHTMessage.CMD_FIND_NODE;
		int msgId = 1234;

		DHTMessage msg = new DHTMessage(peer, cmd, msgId, itemsLists[0]);
		msg.addItemList(itemsLists[1]);
		assertEquals(peer, msg.peer);
		assertEquals(cmd, msg.cmd);
		assertEquals(msgId, msg.msgId);
		assertEquals(DHTMessage.FLAG_RESPONSE, msg.flags);
		assertEquals(itemsLists.length, msg.itemLists.size());
		for(int i = 0; i < itemsLists.length; i++) {
			assertFalse(itemsLists[i] == msg.itemLists.get(i));
			assertEquals(itemsLists[i].size(), msg.itemLists.get(i).size());
			assertTrue(itemsLists[i].containsAll(msg.itemLists.get(i)));
		}
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
		DHTMessage req = new DHTMessage(peer, DHTMessage.CMD_FIND_NODE, 1234, items);
		req.send();
		
		for(DatagramPacket packet : client.packets) {
			ByteBuffer buf = ByteBuffer.wrap(decodePacket(privateKeyForPeer(0), req, packet));
			while(buf.hasRemaining()) {
				buf.get();
				buf.getShort();
				DummyRecord record = new DummyRecord(buf);
				assertFalse(set.contains(record));
				set.add(record);
			}
		}
		
		assertEquals(numRecords, set.size());
	}
	
	@Test
	public void testSerializesMultipleItemLists() throws UnsupportedProtocolException {
		int numRecords = 64;
		@SuppressWarnings("unchecked")
		ArrayList<DHTRecord>[] itemLists = new ArrayList[2];
		for(int i = 0; i < itemLists.length; i++) {
			itemLists[i] = new ArrayList<DHTRecord>(numRecords);
			for(int j = 0; j < numRecords; j++) {
				itemLists[i].add(new DummyRecord(j));
			}
		}
		
		DHTMessage req = new DHTMessage(peer, DHTMessage.CMD_FIND_NODE, 1234, itemLists[0]);
		req.addItemList(itemLists[1]);
		req.send();
		
		for(DatagramPacket packet : client.packets) {
			ByteBuffer buf = ByteBuffer.wrap(decodePacket(privateKeyForPeer(0), req, packet));
			while(buf.hasRemaining()) {
				int listIndex = buf.get();
				buf.getShort();
				DummyRecord record = new DummyRecord(buf);
				assertTrue(itemLists[listIndex].contains(record));
				itemLists[listIndex].remove(record);
			}
		}
		
		for(ArrayList<DHTRecord> itemList : itemLists) {
			assertEquals(0, itemList.size());
		}
	}
	
	@Test
	public void testSerializesPayloadIntoDatagram() {
		byte[] payload = crypto.rng(32);
		DHTMessage req = new DHTMessage(peer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		assertArrayEquals(payload, decodePacket(privateKeyForPeer(0), req, client.packets.get(0)));
	}
	
	@Test
	public void testDeserializesMessagesToClient() throws ProtocolViolationException, UnknownHostException {
		byte[]     payload      = crypto.rng(32);
		DHTPeer    localPeer    = new DHTPeer(client, "localhost", 12345, client.getPublicKey().getBytes());
		DHTMessage req          = new DHTMessage(localPeer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		
		byte[]     serialized   = client.packets.get(0).getData();
		DHTMessage deserialized = new DHTMessage(client, "127.0.0.1", 54321, ByteBuffer.wrap(serialized));
		
		assertArrayEquals(req.authTag,                      deserialized.authTag);
		assertEquals     (req.msgId,                        deserialized.msgId);
		assertEquals     (req.cmd,                          deserialized.cmd);
		assertEquals     (req.flags,                        deserialized.flags);
		assertEquals     (client.packets.size(),            deserialized.numExpected);
		assertArrayEquals(payload,                          deserialized.payload);
		
		assertEquals     ("127.0.0.1",                      deserialized.peer.address);
		assertEquals     (54321,                            deserialized.peer.port);
		assertArrayEquals(client.getPublicKey().getBytes(), req.peer.key.getBytes());
	}
	
	@Test
	public void testDeserializeThrowsProtocolViolationExceptionIfTampered() throws UnknownHostException {
		byte[]     payload    = crypto.rng(32);
		DHTPeer    localPeer  = new DHTPeer(client, "localhost", 12345, client.getPublicKey().getBytes());
		DHTMessage req        = new DHTMessage(localPeer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		
		// if we flip any bit in the message, it should throw an exception during deserialization
		byte[] serialized = client.packets.get(0).getData();
		for(int i = 0; i < 8*serialized.length; i++) {
			serialized[i/8] ^= (1 << (i%8));
			try {
				new DHTMessage(client, "127.0.0.1", 54321, ByteBuffer.wrap(serialized)).hashCode();
				fail();
			} catch(ProtocolViolationException exc) {}
			serialized[i/8] ^= (1 << (i%8));
		}
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testDeserializeThrowsProtocolViolationExceptionIfTruncatedCiphertext() throws ProtocolViolationException, UnknownHostException {
		byte[]     payload    = crypto.rng(32);
		DHTPeer    localPeer  = new DHTPeer(client, "localhost", 12345, client.getPublicKey().getBytes());
		DHTMessage req        = new DHTMessage(localPeer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		
		ByteBuffer buf = ByteBuffer.wrap(client.packets.get(0).getData());
		buf.limit(buf.limit()-1);
		new DHTMessage(client, "127.0.0.1", 54321, buf).hashCode();
	}

	@Test(expected=ProtocolViolationException.class)
	public void testDeserializeThrowsProtocolViolationExceptionIfTruncatedKey() throws ProtocolViolationException, UnknownHostException {
		byte[]     payload    = crypto.rng(32);
		DHTPeer    localPeer  = new DHTPeer(client, "localhost", 12345, client.getPublicKey().getBytes());
		DHTMessage req        = new DHTMessage(localPeer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		
		ByteBuffer buf = ByteBuffer.wrap(client.packets.get(0).getData());
		buf.limit(crypto.asymPublicDHKeySize()-1);
		new DHTMessage(client, "127.0.0.1", 54321, buf).hashCode();
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testDeserializeThrowsProtocolViolationExceptionIfTimestampIsTooOld() throws ProtocolViolationException, UnknownHostException {
		long startTs = System.currentTimeMillis();
		long sendTs  = startTs;		
		long maxAge  = client.getMaster().getGlobalConfig().getLong("net.dht.maxTimestampDelta");
		Util.setCurrentTimeMillis(sendTs);

		byte[]     payload    = crypto.rng(32);
		DHTPeer    localPeer  = new DHTPeer(client, "localhost", 12345, client.getPublicKey().getBytes());
		DHTMessage req        = new DHTMessage(localPeer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		Util.setCurrentTimeMillis(startTs + maxAge);
		
		ByteBuffer buf        = ByteBuffer.wrap(client.packets.get(0).getData());
		new DHTMessage(client, "127.0.0.1", 54321, buf).hashCode();
	}

	@Test
	public void testDeserializeDoesntThrowProtocolViolationExceptionIfTimestampIsJustShyOfOld() throws ProtocolViolationException, UnknownHostException {
		long startTs = System.currentTimeMillis();
		long sendTs  = startTs + 1;		
		long maxAge  = client.getMaster().getGlobalConfig().getLong("net.dht.maxTimestampDelta");
		Util.setCurrentTimeMillis(sendTs);

		byte[]     payload    = crypto.rng(32);
		DHTPeer    localPeer  = new DHTPeer(client, "localhost", 12345, client.getPublicKey().getBytes());
		DHTMessage req        = new DHTMessage(localPeer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		Util.setCurrentTimeMillis(startTs + maxAge);
		
		ByteBuffer buf = ByteBuffer.wrap(client.packets.get(0).getData());
		new DHTMessage(client, "127.0.0.1", 54321, buf).hashCode();
	}
	
	@Test(expected=ProtocolViolationException.class)
	public void testDeserializeThrowsProtocolViolationExceptionIfTimestampIsTooFuturistic() throws ProtocolViolationException, UnknownHostException {
		long startTs = System.currentTimeMillis();
		long minAge  = client.getMaster().getGlobalConfig().getLong("net.dht.minTimestampDelta");
		long sendTs  = startTs - minAge;
		Util.setCurrentTimeMillis(sendTs);

		byte[]     payload    = crypto.rng(32);
		DHTPeer    localPeer  = new DHTPeer(client, "localhost", 12345, client.getPublicKey().getBytes());
		DHTMessage req        = new DHTMessage(localPeer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		Util.setCurrentTimeMillis(startTs);
		
		ByteBuffer buf = ByteBuffer.wrap(client.packets.get(0).getData());
		new DHTMessage(client, "127.0.0.1", 54321, buf).hashCode();
	}
	
	@Test
	public void testDeserializeDoesntThrowProtocolViolationExceptionIfTimestampIsJustShyOfTooFuturistic() throws ProtocolViolationException, UnknownHostException {
		long startTs = System.currentTimeMillis();
		long minAge  = client.getMaster().getGlobalConfig().getLong("net.dht.minTimestampDelta");
		long sendTs  = startTs - minAge - 1;		
		Util.setCurrentTimeMillis(sendTs);

		byte[]     payload    = crypto.rng(32);
		DHTPeer    localPeer  = new DHTPeer(client, "localhost", 12345, client.getPublicKey().getBytes());
		DHTMessage req        = new DHTMessage(localPeer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		Util.setCurrentTimeMillis(startTs);
		
		ByteBuffer buf = ByteBuffer.wrap(client.packets.get(0).getData());
		new DHTMessage(client, "127.0.0.1", 54321, buf).hashCode();
	}
	
	@Test
	public void testDeserializeThrowsProtocolViolationExceptionIfReplay() throws ProtocolViolationException, UnknownHostException {
		byte[] payload    = crypto.rng(32);
		DHTPeer localPeer = new DHTPeer(client, "localhost", 12345, client.getPublicKey().getBytes());
		DHTMessage req    = new DHTMessage(localPeer, DHTMessage.CMD_ADD_RECORD, payload, (resp)->{});
		req.send();
		
		ByteBuffer buf = ByteBuffer.wrap(client.packets.get(0).getData());
		new DHTMessage(client, "127.0.0.1", 54321, buf).hashCode();
		buf.position(0);
		
		try {
			new DHTMessage(client, "127.0.0.1", 54321, buf).hashCode();
			fail();
		} catch(BenignProtocolViolationException exc) {
		}
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
