package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PRNG;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.net.TCPPeerAdvertisement;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageCallback;
import com.acrescrypto.zksync.utility.Shuffler;
import com.acrescrypto.zksync.utility.Util;

public class DHTClientTest {
	final static int MAX_TEST_TIME_MS = 2000;
	final static int MAX_MSG_WAIT_TIME_MS = 500;
	
	class DummyMaster extends ZKMaster {
		public DummyMaster()
				throws IOException, InvalidBlacklistException {
			super();
			this.crypto = CryptoSupport.defaultCrypto();
			this.threadGroup = Thread.currentThread().getThreadGroup();
			this.storage = new RAMFS();
			this.blacklist = new Blacklist(storage, "blacklist", new Key(crypto));
		}
		
		@Override
		public void close() {}
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
	
	class RemotePeer implements AutoCloseable {
		DHTClient listenClient;
		DHTPeer peer;
		DatagramSocket socket;
		PrivateDHKey dhKey;
		Queue<DHTMessage> incoming = new LinkedList<>();
		boolean strict = true;
		
		public RemotePeer() throws SocketException, UnknownHostException {
			socket = new DatagramSocket(0, InetAddress.getByName("localhost"));
			socket.setReuseAddress(true);
			assertTrue(socket.isBound());
			dhKey = crypto.makePrivateDHKey();
			initListenClient();
			peer = new DHTPeer(client, socket.getLocalAddress().getHostAddress(), socket.getLocalPort(), dhKey.publicKey().getBytes());
			new Thread(()->listenThread()).start();
		}
		
		public void initListenClient() {
			try {
				ZKMaster master = new DummyMaster();
				listenClient = new DHTClient(new Key(crypto), master);
			} catch(Exception exc) {
				exc.printStackTrace();
				fail();
			}

			/* this is grotesque.
			 * we're basically murdering a DHTClient, hollowing out its skull and working its limbs like a puppet
			 * to woo a still-living DHTClient. But it's 3 lines and a comment versus a DummyClient subclass...
			 */
			listenClient.key = dhKey;
			listenClient.socket = socket;
		}
		
		public void addSubsetOfPeers(ArrayList<RemotePeer> remotes, int numToAdd) {
			assertTrue(numToAdd <= remotes.size());
			Shuffler shuffler = new Shuffler(remotes.size());
			for(int i = 0; i < numToAdd; i++) {
				listenClient.addPeer(remotes.get(shuffler.next()).peer);
			}
		}
		
		public void addAllPeers(ArrayList<RemotePeer> remotes) {
			for(RemotePeer remote : remotes) {
				listenClient.addPeer(remote.peer);
			}
		}
		
		void listenThread() {
			Util.setThreadName("DHTClientTest RemotePeer listenThread");
			byte[] receiveData = new byte[65536];
			DatagramPacket packet = new DatagramPacket(receiveData, receiveData.length);

			try {
				while(true) {
					socket.receive(packet);
					assertTrue(packet.getLength() <= DHTClient.MAX_DATAGRAM_SIZE);
					DHTMessage msg;
					try {
						msg = new DHTMessage(listenClient, packet.getAddress().getHostAddress(), packet.getPort(), ByteBuffer.wrap(packet.getData(), 0, packet.getLength()));
						synchronized(incoming) {
							incoming.add(msg);
							incoming.notifyAll();
						}
					} catch (ProtocolViolationException exc) {
						exc.printStackTrace();
					}
				}
			} catch(IOException exc) {}
		}

		DHTMessage receivePacket() throws IOException, ProtocolViolationException {
			return receivePacket((byte) -1);
		}
		
		DHTMessage receivePacket(byte cmd) throws ProtocolViolationException {
			synchronized(incoming) {
				if(incoming.isEmpty()) {
					try {
						incoming.wait(MAX_MSG_WAIT_TIME_MS);
					} catch (InterruptedException exc) {
						exc.printStackTrace();
					}
				}
				
				if(strict) assertFalse(incoming.isEmpty());
				else if(incoming.isEmpty()) return null;
				
				DHTMessage msg = incoming.remove();
				if(cmd >= 0) assertEquals(cmd, msg.cmd);
				
				return msg;
			}
		}
		
		public void close() {
			listenClient.close();
			listenClient.master.close();
			socket.close();
		}
	}
	
	interface RemotePeerHandler {
		void handle(RemotePeer remote) throws ProtocolViolationException, IOException;
	}

	class TestNetwork implements AutoCloseable {
		ArrayList<RemotePeer> remotes = new ArrayList<>();
		HashMap<RemotePeer,RemotePeerHandler> handlers = new HashMap<>();
		boolean closed;
		
		public TestNetwork(int size) throws SocketException, UnknownHostException {
			for(int i = 0; i < size; i++) {
				remotes.add(new RemotePeer());
				handlers.put(remotes.get(i), (remote)->defaultHandler(remote));
			}
			
			for(RemotePeer remote : remotes) {
				remote.addAllPeers(remotes);
			}
		}
		
		public void run() {
			ArrayList<Thread> threads = new ArrayList<>(remotes.size());
			MutableBoolean failed = new MutableBoolean();
			for(RemotePeer remote : remotes) {
				Thread t = new Thread(()-> {
					Util.setThreadName("Remote peer handler " + remote.peer.port);
					try {
						handlers.get(remote).handle(remote);
					} catch(Exception exc) {
						if(closed) return;
						exc.printStackTrace();
						failed.setTrue();
					}
				});
				threads.add(t);
				t.start();
			}
			
			for(Thread t : threads) {
				try {
					t.join(MAX_TEST_TIME_MS);
				} catch(InterruptedException exc) {
				}
			}
			
			close();
			assertFalse(failed.booleanValue());
		}
		
		public void setHandlerForClosest(DHTID id, int numResults, RemotePeerHandler handler) {
			for(RemotePeer remote : closestRemotePeers(id, numResults)) {
				handlers.put(remote, handler);
			}
		}
		
		public ArrayList<RemotePeer> closestRemotePeers(DHTID id, int numResults) {
			ArrayList<RemotePeer> sorted = new ArrayList<RemotePeer>(remotes);
			sorted.sort((a, b)->a.peer.id.xor(id).compareTo(b.peer.id.xor(id)));
			
			ArrayList<RemotePeer> results = new ArrayList<RemotePeer>(numResults);
			for(int i = 0; i < numResults; i++) {
				results.add(sorted.get(i));
			}
			
			return results;
		}
		
		public ArrayList<DHTPeer> closestDHTPeers(DHTID id, int numResults) {
			ArrayList<RemotePeer> remotes = closestRemotePeers(id, numResults);
			ArrayList<DHTPeer> peers = new ArrayList<>();
			for(RemotePeer remote : remotes) {
				peers.add(remote.peer);
			}
			
			return peers;
		}
		
		public void defaultHandler(RemotePeer remote) {
			try {
				remote.strict = false;
				DHTMessage msg = remote.receivePacket();
				if(msg == null) return;
				
				if(msg.cmd == DHTMessage.CMD_FIND_NODE) {
					byte[] id = new byte[client.idLength()];
					ByteBuffer.wrap(msg.payload).get(id);
					Collection<DHTPeer> results = remote.listenClient.routingTable.closestPeers(new DHTID(id), DHTBucket.MAX_BUCKET_CAPACITY);
					msg.makeResponse(results).send();
				} else if(msg.cmd == DHTMessage.CMD_PING) {
					msg.makeResponse(new ArrayList<>()).send();
				} else {
					fail();
				}
			} catch(ProtocolViolationException | IOException exc) {
				if(closed) return;
				exc.printStackTrace();
				fail();
			}
		}
		
		public void close() {
			closed = true;
			for(RemotePeer remote : remotes) {
				remote.close();
			}
		}
	}
	
	DHTAdvertisementRecord makeBogusAd(int i) {
		PRNG prng = crypto.prng(ByteBuffer.allocate(4).putInt(i).array());
		PublicDHKey pubKey = crypto.makePublicDHKey(prng.getBytes(crypto.asymPublicDHKeySize()));
		byte[] encryptedArchiveId = prng.getBytes(crypto.hashLength());
		String addr = "10." + (i/(256*256)) + "." + ((i/256) % 256) + "." + (i%256);
		int port = prng.getInt() & 0xffff;
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(pubKey, addr, port, encryptedArchiveId);
		return new DHTAdvertisementRecord(crypto, ad);
	}

	CryptoSupport crypto;
	Key storageKey;
	DummyMaster master;
	DHTClient client;
	DHTPeer clientPeer;
	
	RemotePeer remote;
	
	void assertAlive() throws ProtocolViolationException {
		remote.listenClient.pingMessage(clientPeer, null).send();
		assertNotNull(remote.receivePacket(DHTMessage.CMD_PING));
	}
	
	@BeforeClass
	public static void beforeAll() {
		TCPPeerAdvertisement.disableReachabilityTest = true;
	}
	
	@Before
	public void beforeEach() throws IOException, InvalidBlacklistException {
		DHTClient.messageExpirationTimeMs = 125;
		DHTClient.lookupResultMaxWaitTimeMs = 150;
		DHTClient.messageRetryTimeMs = 50;
		DHTClient.socketCycleDelayMs = 10;
		DHTClient.socketOpenFailCycleDelayMs = 20;
		
		crypto = CryptoSupport.defaultCrypto();
		
		master = new DummyMaster();
		storageKey = new Key(crypto);
		client = new DHTClient(storageKey, master);
		remote = new RemotePeer();
		
		client.addPeer(remote.peer);
		client.listen("127.0.0.1", 0);
		
		clientPeer = new DHTPeer(remote.listenClient, "localhost", client.socket.getLocalPort(), client.key.publicKey());
	}
	
	@After
	public void afterEach() {
		DHTSearchOperation.searchQueryTimeoutMs = DHTSearchOperation.DEFAULT_SEARCH_QUERY_TIMEOUT_MS;
		
		master.close();
		remote.close();
		client.close();
		Util.setCurrentTimeNanos(-1);
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TCPPeerAdvertisement.disableReachabilityTest = false;
		DHTClient.lookupResultMaxWaitTimeMs = DHTClient.DEFAULT_LOOKUP_RESULT_MAX_WAIT_TIME_MS;
		DHTClient.messageExpirationTimeMs = DHTClient.DEFAULT_MESSAGE_EXPIRATION_TIME_MS;
		DHTClient.messageRetryTimeMs = DHTClient.DEFAULT_MESSAGE_RETRY_TIME_MS;
		DHTClient.socketCycleDelayMs = DHTClient.DEFAULT_SOCKET_CYCLE_DELAY_MS;
		DHTClient.socketOpenFailCycleDelayMs = DHTClient.DEFAULT_SOCKET_OPEN_FAIL_CYCLE_DELAY_MS;
		DHTClient.autoFindPeersIntervalMs = DHTClient.DEFAULT_AUTO_FIND_PEERS_INTERVAL_MS;
	}
	
	@Test
	public void testConstructorWithoutExistingData() {
		assertEquals(master.getBlacklist(), client.blacklist);
		assertEquals(storageKey, client.storageKey);
		assertEquals(master.getBlacklist().getFS(), client.storage);
		assertEquals(crypto, client.crypto);
		assertNotNull(client.routingTable);
		assertNotNull(client.store);
	}
	
	@Test
	public void testConstructorWithExistingData() {
		DHTClient client1 = new DHTClient(storageKey, master);

		assertEquals(master.getBlacklist(), client1.blacklist);
		assertEquals(storageKey, client1.storageKey);
		assertEquals(master.getBlacklist().getFS(), client1.storage);
		assertEquals(crypto, client1.crypto);
		assertNotNull(client1.routingTable);
		assertNotNull(client1.store);
		
		assertArrayEquals(client.key.getBytes(), client1.key.getBytes());
		assertArrayEquals(client.key.publicKey().getBytes(), client1.key.publicKey().getBytes());
		assertArrayEquals(client.tagKey.getRaw(), client1.tagKey.getRaw());
		assertEquals(client.id, client1.id);
		
		client1.close();
	}
	
	@Test
	public void testListenBindsToAddressAndPort() throws SocketException {
		client.listen("127.0.0.1", 41851); // randomly picked
		assertEquals(41851, client.socket.getLocalPort());
		assertEquals("127.0.0.1", client.socket.getLocalAddress().getHostAddress());
	}
	
	@Test
	public void testListenOnNullBindsToAllAddresses() throws SocketException {
		client.listen(null, 0);
		assertEquals("0.0.0.0", client.socket.getLocalAddress().getHostAddress());
	}
	
	@Test(expected=SocketException.class)
	public void testListenThrowsExceptionIfPortInUse() throws SocketException {
		client.listen("127.0.0.1", remote.socket.getLocalPort());
	}
	
	@Test
	public void testFindPeersTriggersSearchOperationForOwnId() throws IOException, ProtocolViolationException {
		client.findPeers();
		DHTMessage msg = remote.receivePacket();
		byte[] rxId = new byte[client.id.rawId.length];
		System.arraycopy(msg.payload, 0, rxId, 0, rxId.length);
		assertArrayEquals(client.id.rawId, rxId);
	}
	
	@Test
	public void testLookupTriggersSearchOperationForRequestedId() throws IOException, ProtocolViolationException {
		DHTID searchId = new DHTID(crypto.rng(client.idLength()));
		Key lookupKey = new Key(crypto);
		byte[] token = lookupKey.authenticate(Util.concat(searchId.rawId, remote.peer.key.getBytes()));
		client.lookup(searchId, lookupKey, (resp)->{});
		DHTMessage msg = remote.receivePacket(DHTMessage.CMD_FIND_NODE);
		ByteBuffer rxBuf = ByteBuffer.wrap(msg.payload);
		byte[] rxId = new byte[searchId.rawId.length], rxToken = new byte[token.length];
		rxBuf.get(rxId);
		rxBuf.get(rxToken);
		assertArrayEquals(searchId.rawId, rxId);
		assertArrayEquals(token, rxToken);
	}

	@Test
	public void testLookupInvokesCallbackWithEachReceivedRecord() throws IOException, ProtocolViolationException {
		MutableBoolean seenNull = new MutableBoolean();
		ArrayList<DHTRecord> records = new ArrayList<>();
		Key lookupKey = new Key(crypto);
		DHTID searchId = new DHTID(crypto.rng(client.idLength()));

		client.lookup(searchId, lookupKey, (record)->{
			assertFalse(seenNull.booleanValue());
			if(record == null) {
				seenNull.setTrue();
				return;
			}

			assertTrue(records.contains(record));
			records.remove(record);
		});

		DHTMessage findNodeReq = remote.receivePacket(DHTMessage.CMD_FIND_NODE);
		ArrayList<DHTPeer> peers = new ArrayList<>(1);
		peers.add(remote.peer);
		for(int i = 0; i < DHTRecordStore.MAX_RECORDS_PER_ID; i++) records.add(makeBogusAd(i));
		findNodeReq.makeResponse(peers).addItemList(records).send();

		assertTrue(Util.waitUntil(MAX_TEST_TIME_MS, ()->seenNull.booleanValue()));
		assertEquals(0, records.size());
	}
	
	@Test
	public void testLookupInvokesCallbackWithNullIfNoResponseReceivedInTime() throws IOException, ProtocolViolationException {
		MutableBoolean seenNull = new MutableBoolean();
		ArrayList<DHTRecord> records = new ArrayList<>();
		DHTSearchOperation.searchQueryTimeoutMs = 50;

		DHTID searchId = new DHTID(crypto.rng(client.idLength()));
		client.lookup(searchId, new Key(crypto), (record)->{
			assertFalse(seenNull.booleanValue());
			assertNull(record);
			seenNull.setTrue();
		});

		remote.receivePacket(DHTMessage.CMD_FIND_NODE);
		assertTrue(Util.waitUntil(DHTSearchOperation.searchQueryTimeoutMs+10, ()->seenNull.booleanValue()));
		assertEquals(0, records.size());
	}
	
	@Test
	public void testLookupInvokesCallbackWithNullIfPartialResponseReceivedInTime() throws IOException, ProtocolViolationException {
		MutableBoolean seenNull = new MutableBoolean();
		MutableInt numSeen = new MutableInt();
		ArrayList<DHTRecord> records = new ArrayList<>();
		DHTClient.lookupResultMaxWaitTimeMs = 50;
		DHTSearchOperation.searchQueryTimeoutMs = 50;

		DHTID searchId = new DHTID(crypto.rng(client.idLength()));
		client.lookup(searchId, new Key(crypto), (resp)->{
			assertFalse(seenNull.booleanValue());
			if(resp == null) {
				seenNull.setTrue();
			} else {
				numSeen.increment();
			}
		});

		DHTMessage findNodeReq = remote.receivePacket(DHTMessage.CMD_FIND_NODE);
		ArrayList<DHTPeer> peers = new ArrayList<>(1);
		peers.add(remote.peer);

		for(int i = 0; i < DHTRecordStore.MAX_RECORDS_PER_ID; i++) records.add(makeBogusAd(i));

		class RiggedMessage extends DHTMessage {
			public RiggedMessage(DHTMessage msg) { super(msg.peer, msg.cmd, msg.msgId, null); this.itemLists = msg.itemLists; }                

			@Override protected int numPacketsNeeded() { return super.numPacketsNeeded() + 1; }
		};

		new RiggedMessage(findNodeReq.makeResponse(records).addItemList(records)).send();
		assertTrue(Util.waitUntil(MAX_TEST_TIME_MS, ()->seenNull.booleanValue()));
	}
	
	@Test
	public void testLookupObtainsRecordsFromMultiplePeers() throws IOException, ProtocolViolationException, InvalidBlacklistException {
		int recordsPerPeer = 4;

		MutableInt numSeen = new MutableInt();
		MutableBoolean seenNull = new MutableBoolean();
		ArrayList<RemotePeer> remotes = new ArrayList<>();
		ArrayList<DHTPeer> peers = new ArrayList<>();

		remotes.add(remote);
		for(int i = 0; i < DHTSearchOperation.maxResults-1; i++) {
			remotes.add(new RemotePeer());
		}

		for(RemotePeer r : remotes) peers.add(r.peer);

		DHTID searchId = new DHTID(crypto.rng(client.idLength()));
		client.lookup(searchId, new Key(crypto), (resp)->{
			assertFalse(seenNull.booleanValue());
			if(resp == null) {
				seenNull.setTrue();
				return;
			}

			numSeen.increment();
		});

		int i = 0;
		for(RemotePeer r : remotes) {
			ArrayList<DHTRecord> peerRecords = new ArrayList<>();
			for(int j = 0; j < recordsPerPeer; j++) {
				peerRecords.add(makeBogusAd(recordsPerPeer*i+j));
			}

			DHTMessage findNodeReq = r.receivePacket(DHTMessage.CMD_FIND_NODE);
			findNodeReq.makeResponse(peers).addItemList(peerRecords).send(); 
		}               

		assertTrue(Util.waitUntil(MAX_TEST_TIME_MS, ()->seenNull.booleanValue()));
		assertEquals(recordsPerPeer*remotes.size(), numSeen.intValue());

		for(RemotePeer r : remotes) r.close();
	}
	
	@Test
	public void testAddRecordCallsAddRecordOnClosestPeersForID() throws IOException, InvalidBlacklistException {
		DHTID searchId = client.id.flip(); // ensure that the client is NOT one of the closest results
		try(TestNetwork network = new TestNetwork(16)) {
			MutableInt numFindNode = new MutableInt();
			MutableInt numReceived = new MutableInt();
			Key lookupKey = new Key(crypto);
			
			network.setHandlerForClosest(searchId, DHTSearchOperation.maxResults, (remote)->{
				byte[] token = lookupKey.authenticate(Util.concat(searchId.rawId, remote.peer.key.getBytes()));
				DHTMessage findNodeMsg = remote.receivePacket(DHTMessage.CMD_FIND_NODE);
				assertArrayEquals(Util.concat(searchId.rawId, token), findNodeMsg.payload);
				synchronized(numFindNode) { numFindNode.increment();  }
				findNodeMsg.makeResponse(remote.listenClient.routingTable.closestPeers(searchId, DHTSearchOperation.maxResults)).send();
				
				DHTMessage addRecordMsg;
				do {
					// TODO Someday: (bug) Figure out why we mysteriously get extra CMD_FIND_NODEs if we run a full test suite.
					// For now, we'll just discard them and move on.
					addRecordMsg = remote.receivePacket();
				} while(addRecordMsg.cmd != DHTMessage.CMD_ADD_RECORD);
				
				synchronized(numReceived) { numReceived.increment(); }
				addRecordMsg.assertValidAuthTag();
				
				ByteBuffer payload = ByteBuffer.wrap(addRecordMsg.payload);
				byte[] id = new byte[client.idLength()];
				byte[] rxToken = new byte[crypto.hashLength()];
				payload.get(id);
				payload.get(rxToken);
				assertArrayEquals(searchId.rawId, id);
				assertArrayEquals(token, rxToken);
				
				byte[] recordBytes = new byte[payload.remaining()];
				payload.get(recordBytes);
				assertArrayEquals(makeBogusAd(0).serialize(), recordBytes);
			});
			
			client.routingTable.reset();
			for(RemotePeer remote : network.remotes) {
				client.addPeer(remote.peer);
			}
			
			client.addRecord(searchId, lookupKey, makeBogusAd(0));
			network.run();
			
			assertEquals(DHTSearchOperation.maxResults, numReceived.intValue());
		}
	}
	
	@Test
	public void testAddsRecordToSelfWhenOwnIdIsInResultSet() throws IOException, ProtocolViolationException {
		ArrayList<DHTPeer> list = new ArrayList<>(1);
		list.add(clientPeer);
		Key lookupKey = new Key(crypto);
		byte[] token = lookupKey.authenticate(Util.concat(client.id.rawId, client.getLocalPeer().key.getBytes()));
		
		client.addRecord(client.id, lookupKey, makeBogusAd(0));
		remote.receivePacket().makeResponse(list).send();
		assertTrue(Util.waitUntil(MAX_TEST_TIME_MS, ()->client.store.recordsForId(client.id, token).size() > 0));
	}

	@Test
	public void testIdLengthIsHashLength() {
		assertEquals(crypto.hashLength(), client.idLength());
	}
	
	@Test
	public void testIsInitializedReturnsFalseIfFindPeersNotCalled() {
		assertFalse(client.initialized);
	}
	
	@Test
	public void testIsInitializedReturnsTrueIfFindPeersComplete() throws IOException, ProtocolViolationException {
		client.findPeers();
		remote.receivePacket().makeResponse(new ArrayList<>()).send();
		assertTrue(Util.waitUntil(100, ()->client.isInitialized()));
	}
	
	@Test
	public void testGetPortReturnsBoundPortNumber() {
		assertEquals(client.socket.getLocalPort(), client.getPort());
	}
	
	@Test
	public void testToleratesIndecipherableMessages() throws UnknownHostException {
		/* send an corrupted one, and then a valid one and make sure we get a reply.
		 * (in other words, did we blow up the listen thread or get blacklisted?)
		 */
		DHTMessage ping = remote.listenClient.pingMessage(clientPeer, (resp)->fail());
		byte[] pingBytes = ping.serialize(1, ByteBuffer.allocate(0));
		pingBytes[crypto.asymPublicDHKeySize()] ^= 0x01;
		
		InetAddress address = InetAddress.getByName(clientPeer.address);
		DatagramPacket datagram = new DatagramPacket(pingBytes, pingBytes.length, address, clientPeer.port);
		clientPeer.client.sendDatagram(datagram);
		
		Util.sleep(10);
		
		MutableBoolean received = new MutableBoolean();
		clientPeer.findNode(remote.listenClient.id, new Key(crypto), (peers, isFinal)->{ received.setTrue(); }, (record)->{});
		assertFalse(Util.waitUntil(MAX_MSG_WAIT_TIME_MS, ()->received.booleanValue()));
	}
	
	@Test
	public void testToleratesUnsupportedMessages() throws UnknownHostException {
		// same idea as toleratesIndecipherableMessages, except with unsupported message types
		DHTMessage ping = remote.listenClient.pingMessage(clientPeer, (resp)->fail());
		ping.cmd = -1;
		ping.send();
		
		Util.sleep(10);
		
		MutableBoolean received = new MutableBoolean();
		clientPeer.findNode(remote.listenClient.id, new Key(crypto), (peers, isFinal)->{ received.setTrue(); }, (record)->{});
		assertFalse(Util.waitUntil(MAX_MSG_WAIT_TIME_MS, ()->received.booleanValue()));
	}
	
	@Test
	public void testMessageSendersAutomaticallyAddedToRoutingTable() {
		client.routingTable.reset();
		assertFalse(client.routingTable.allPeers().contains(remote.peer));
		remote.listenClient.pingMessage(clientPeer, (resp)->fail()).send();		
		Util.sleep(10);
		assertTrue(client.routingTable.allPeers().contains(remote.peer));
	}
	
	@Test
	public void testMessageRespondersMarkedAsRefreshed() throws ProtocolViolationException {
		DHTPeer peerFromTable = null;
		for(DHTPeer peer : client.routingTable.allPeers()) {
			peerFromTable = peer;
			break;
		}
		
		peerFromTable.missedMessages = 1;
		peerFromTable.ping();
		DHTMessage req = remote.receivePacket(DHTMessage.CMD_PING);
		req.makeResponse(new ArrayList<>(0)).send();
		Util.sleep(10);
		assertEquals(0, peerFromTable.missedMessages);
	}
	
	@Test
	public void testMessageResponderBucketsMarkedAsRefreshed() throws ProtocolViolationException {
		DHTBucket bucket = null;
		for(DHTBucket candidateBucket : client.routingTable.buckets) {
			if(candidateBucket.includes(remote.peer.id)) {
				bucket = candidateBucket;
				break;
			}
		}
		
		Util.setCurrentTimeMillis(Util.currentTimeMillis() + DHTBucket.BUCKET_FRESHEN_INTERVAL_MS);
		assertTrue(bucket.needsFreshening());
		bucket.peers.get(0).ping();
		DHTMessage req = remote.receivePacket(DHTMessage.CMD_PING);
		req.makeResponse(new ArrayList<>(0)).send();
		Util.sleep(10);
		assertFalse(bucket.needsFreshening());
	}
	
	@Test
	public void testRespondsToPing() throws ProtocolViolationException {
		DHTMessage req = remote.listenClient.pingMessage(clientPeer, null);
		req.send();
		
		DHTMessage resp = remote.receivePacket(DHTMessage.CMD_PING);
		assertEquals(req.msgId, resp.msgId);
	}
	
	@Test
	public void testIgnoresMessagesFromAlternateNetworkIds() throws ProtocolViolationException {
		remote.listenClient.networkId = crypto.rng(crypto.hashLength());
		DHTMessage req = remote.listenClient.pingMessage(clientPeer, null);
		req.send();
		
		remote.strict = false;
		assertNull(remote.receivePacket(DHTMessage.CMD_PING));
	}
	
	@Test
	public void testFindNodeIgnoresTruncatedID() throws IOException, ProtocolViolationException {
		DHTMessage req = remote.listenClient.findNodeMessage(clientPeer, clientPeer.id, new Key(crypto), null);
		req.payload = crypto.rng(req.payload.length-1);
		req.send();
		
		Util.sleep(10);
		remote.strict = false;
		assertNull(remote.receivePacket());
		assertAlive();
	}
	
	@Test
	public void testFindNodeIgnoresOverlyLongID() throws IOException, ProtocolViolationException {
		DHTMessage req = remote.listenClient.findNodeMessage(clientPeer, clientPeer.id, new Key(crypto), null);
		req.payload = crypto.rng(req.payload.length+1);
		req.send();
		
		Util.sleep(10);
		remote.strict = false;
		assertNull(remote.receivePacket());
		assertAlive();
	}
	
	@Test
	public void testRespondsToFindNodeWithClosestPeers() throws IOException, ProtocolViolationException {
		for(int i = 0; i < 4*DHTSearchOperation.maxResults; i++) {
			DHTPeer peer = new DHTPeer(client, "localhost", i+10000, crypto.makePrivateDHKey().publicKey());
			client.addPeer(peer);
		}
		
		DHTID searchId = remote.peer.id;
		ArrayList<DHTPeer> results = new ArrayList<>(DHTSearchOperation.maxResults);
		int numReceived = 0;
		clientPeer.findNode(searchId, new Key(crypto), (resp, isFinal)->{}, (record)->{});
		
		while(true) {
			DHTMessage resp = remote.receivePacket();
			numReceived++;
			ByteBuffer buf = ByteBuffer.wrap(resp.payload);
			
			while(buf.hasRemaining()) {
				buf.get(); // list id
				buf.getShort(); // item length
				results.add(new DHTPeer(remote.listenClient, buf));
			}
			
			if(resp.numExpected == numReceived) break;
		}
		
		assertEquals(DHTSearchOperation.maxResults, results.size());
		
		DHTID mostDistant = null;
		for(DHTPeer result : results) {
			if(mostDistant == null || result.id.xor(searchId).compareTo(mostDistant) > 0) {
				mostDistant = result.id.xor(searchId);
			}
		}
		
		for(DHTPeer existing : client.routingTable.allPeers()) {
			int r = existing.id.xor(searchId).compareTo(mostDistant);
			if(r <= 0) {
				assertTrue(results.contains(existing));
			} else {
				assertFalse(results.contains(existing));
			}
		}
	}
	
	@Test
	public void testRespondsToFindNodeWithTruncatedListIfNotEnoughPeers() throws IOException, ProtocolViolationException {
		/* if the routing table doesn't have enough peers to fill out a result set, we should just get a list of
		 * the peers the client does have.
		 */
		
		for(int i = 0; i < DHTSearchOperation.maxResults-2; i++) {
			DHTPeer peer = new DHTPeer(client, "localhost", i+10000, crypto.makePrivateDHKey().publicKey());
			client.addPeer(peer);
		}
		
		DHTID searchId = remote.peer.id;
		ArrayList<DHTPeer> results = new ArrayList<>(DHTSearchOperation.maxResults);
		int numReceived = 0;
		clientPeer.findNode(searchId, new Key(crypto), (resp, isFinal)->{}, (record)->{});
		
		while(true) {
			DHTMessage resp = remote.receivePacket();
			numReceived++;
			ByteBuffer buf = ByteBuffer.wrap(resp.payload);
			
			while(buf.hasRemaining()) {
				buf.get(); // list id
				buf.getShort(); // item length
				results.add(new DHTPeer(remote.listenClient, buf));
			}
			
			if(resp.numExpected == numReceived) break;
		}
		
		assertEquals(client.routingTable.allPeers().size(), results.size());
		assertTrue(results.containsAll(client.routingTable.allPeers()));
	}
	
	@Test
	public void testRespondsToFindNodeWithListOfRecordsIfTokenCorrect() throws IOException, ProtocolViolationException, UnsupportedProtocolException {
		DHTID searchId = remote.peer.id;
		int numRecords = 16;
		ArrayList<DHTRecord> records = new ArrayList<>(numRecords);
		Key lookupKey = new Key(crypto);
		byte[] token = lookupKey.authenticate(Util.concat(searchId.rawId, client.getLocalPeer().key.getBytes()));
		
		for(int i = 0; i < numRecords; i++) {
			DHTRecord record = new DummyRecord(i);
			records.add(record);
			client.store.addRecordForId(searchId, token, record);
		}
		
		int numReceived = 0;
		clientPeer.findNode(searchId, lookupKey, (resp, isFinal)->{}, (record)->{});
		
		while(true) {
			DHTMessage resp = remote.receivePacket();
			numReceived++;
			ByteBuffer buf = ByteBuffer.wrap(resp.payload);
			
			while(buf.hasRemaining()) {
				int listIndex = buf.get();
				int itemSize = buf.getShort(); // item length
				if(listIndex == 1) {
					DHTRecord record = new DummyRecord(buf);
					assertTrue(records.contains(record));
					records.remove(record);
				} else {
					buf.position(buf.position() + itemSize);
				}
			}
			
			if(resp.numExpected == numReceived) break;
		}
		
		assertEquals(0, records.size());
	}
	
	@Test
	public void testRespondsToFindNodeWithoutRecordsIfTokenIncorrect() throws IOException, ProtocolViolationException, UnsupportedProtocolException {
		DHTID searchId = remote.peer.id;
		int numRecords = 16;
		Key lookupKey = new Key(crypto);
		byte[] token = lookupKey.authenticate(Util.concat(searchId.rawId, client.getLocalPeer().key.getBytes()));
		
		for(int i = 0; i < numRecords; i++) {
			DHTRecord record = new DummyRecord(i);
			client.store.addRecordForId(searchId, token, record);
		}
		
		int numReceived = 0;
		clientPeer.findNode(searchId, new Key(crypto), (resp, isFinal)->{}, (record)->{});
		
		while(true) {
			DHTMessage resp = remote.receivePacket();
			numReceived++;
			ByteBuffer buf = ByteBuffer.wrap(resp.payload);
			
			while(buf.hasRemaining()) {
				int listIndex = buf.get();
				int itemSize = buf.getShort(); // item length
				if(listIndex == 1) {
					fail();
				} else {
					buf.position(buf.position() + itemSize);
				}
			}
			
			if(resp.numExpected == numReceived) break;
		}
	}

	@Test
	public void testFindNodeResponseIncludesNoRecordsIfNoRecordsForId() throws IOException, ProtocolViolationException, UnsupportedProtocolException {
		for(int i = 0; i < 4*DHTSearchOperation.maxResults; i++) {
			DHTPeer peer = new DHTPeer(client, "localhost", i+10000, crypto.makePrivateDHKey().publicKey());
			client.addPeer(peer);
		}
		
		DHTID searchId = remote.peer.id;
		Key lookupKey = new Key(crypto);
		clientPeer.findNode(searchId, lookupKey, (resp, isFinal)->{}, (record)->{});
		int numReceived = 0;
		
		while(true) {
			DHTMessage resp = remote.receivePacket();
			numReceived++;
			ByteBuffer buf = ByteBuffer.wrap(resp.payload);
			
			while(buf.hasRemaining()) {
				int listIndex = buf.get();
				int itemLength = buf.getShort(); // item length
				
				assertNotEquals(1, listIndex); // list id
				buf.position(buf.position() + itemLength);
			}
			
			if(resp.numExpected == numReceived) break;
		}
	}
	
	@Test
	public void testAddRecordIgnoresRequestsWithInvalidAuthTag() throws ProtocolViolationException {
		DHTID id = new DHTID(crypto.rng(client.idLength()));
		Key lookupKey = new Key(crypto);
		byte[] token = lookupKey.authenticate(Util.concat(id.rawId, client.getLocalPeer().key.getBytes()));
		clientPeer.addRecord(id, new Key(crypto), makeBogusAd(0));
		assertFalse(Util.waitUntil(100, ()->client.store.recordsForId(id, token).size() > 0));
		assertAlive();
	}
	
	@Test
	public void testAddRecordIgnoresRequestsWithTruncatedPayload() throws ProtocolViolationException {
		DHTID id = new DHTID(crypto.rng(client.idLength()));
		Key lookupKey = new Key(crypto);
		byte[] token = lookupKey.authenticate(Util.concat(id.rawId, client.getLocalPeer().key.getBytes()));

		DHTMessage msg = remote.listenClient.addRecordMessage(clientPeer, id, lookupKey, makeBogusAd(0), null);
		byte[] fakePayload = new byte[msg.payload.length - 1];
		System.arraycopy(msg.payload, 0, fakePayload, 0, fakePayload.length);
		msg.authTag = remote.peer.localAuthTag();
		msg.payload = fakePayload;
		msg.send();
		
		assertFalse(Util.waitUntil(100, ()->client.store.recordsForId(id, token).size() > 0));
		assertAlive();
	}
	
	@Test
	public void testAddRecordIgnoresUnsupportedRecordTypes() throws ProtocolViolationException {
		DHTID id = new DHTID(crypto.rng(client.idLength()));
		Key lookupKey = new Key(crypto);
		byte[] token = lookupKey.authenticate(Util.concat(id.rawId, client.getLocalPeer().key.getBytes()));

		DHTMessage msg = remote.listenClient.addRecordMessage(clientPeer, id, new Key(crypto), makeBogusAd(0), null);
		msg.authTag = remote.peer.localAuthTag();
		msg.payload[id.rawId.length + crypto.hashLength()] = Byte.MIN_VALUE; // first byte after token is record type field
		msg.send();
		
		assertFalse(Util.waitUntil(100, ()->client.store.recordsForId(id, token).size() > 0));
		
		assertAlive();
	}
	
	@Test @Ignore
	public void testAddRecordIgnoresInvalidRecords() {
		// no good way to test this 6/4/18 since only record right now is advertisement, which always returns valid
	}
	
	@Test
	public void testAddRecordAddsValidRecordsToStore() throws ProtocolViolationException {
		DHTID id = new DHTID(crypto.rng(client.idLength()));
		Key lookupKey = new Key(crypto);
		byte[] token = lookupKey.authenticate(Util.concat(id.rawId, client.getLocalPeer().key.getBytes()));

		DHTMessage msg = remote.listenClient.addRecordMessage(clientPeer, id, lookupKey, makeBogusAd(0), null);
		msg.authTag = remote.peer.localAuthTag();
		msg.send();
		assertTrue(Util.waitUntil(MAX_TEST_TIME_MS, ()->client.store.recordsForId(id, token).size() > 0));
		remote.receivePacket(DHTMessage.CMD_ADD_RECORD);
	}
	
	@Test
	public void testPingMessageConstructsAppropriatePingMessages() {
		DHTMessageCallback callback = (resp)->{};
		DHTMessage ping = client.pingMessage(remote.peer, callback);
		assertNotEquals(0, ping.msgId); // admittedly there's a 1 in 2**32 chance of this failing randomly
		assertEquals(callback, ping.callback);
		assertEquals(DHTMessage.CMD_PING, ping.cmd);
		assertEquals(remote.peer, ping.peer);
		assertEquals(0, ping.payload.length);
		assertEquals(0, ping.flags);
	}
	
	@Test
	public void testFindNodeMessageConstructsAppropriateFindNodeMessages() {
		DHTMessageCallback callback = (resp)->{};
		DHTID id = new DHTID(crypto.rng(client.idLength()));
		Key lookupKey = new Key(crypto);
		byte[] token = lookupKey.authenticate(Util.concat(id.rawId, remote.peer.key.getBytes()));
		DHTMessage findNode = client.findNodeMessage(remote.peer, id, lookupKey, callback);
		assertNotEquals(0, findNode.msgId); // admittedly there's a 1 in 2**32 chance of this failing randomly
		assertEquals(callback, findNode.callback);
		assertEquals(DHTMessage.CMD_FIND_NODE, findNode.cmd);
		assertEquals(remote.peer, findNode.peer);
		assertArrayEquals(Util.concat(id.rawId, token), findNode.payload);
		assertEquals(0, findNode.flags);
	}
	
	@Test
	public void testAddRecordMessageConstructsAppropriateAddRecordMessages() {
		DHTMessageCallback callback = (resp)->{};
		DHTID id = new DHTID(crypto.rng(client.idLength()));
		Key lookupKey = new Key(crypto);
		byte[] token = lookupKey.authenticate(Util.concat(id.rawId, remote.peer.key.getBytes()));
		DHTRecord record = makeBogusAd(0);
		DHTMessage addRecord = client.addRecordMessage(remote.peer, id, lookupKey, record, callback);
		
		byte[] expected = Util.concat(
				id.rawId,
				token,
				record.serialize());
		assertNotEquals(0, addRecord.msgId); // admittedly there's a 1 in 2**32 chance of this failing randomly
		assertEquals(callback, addRecord.callback);
		assertEquals(DHTMessage.CMD_ADD_RECORD, addRecord.cmd);
		assertEquals(remote.peer, addRecord.peer);
		assertArrayEquals(expected, addRecord.payload);
		assertEquals(0, addRecord.flags);
	}
	
	@Test
	public void testDeserializeRecordDeserializesIntoAppropriateRecordObject() throws UnsupportedProtocolException {
		DHTRecord record = makeBogusAd(0);
		assertEquals(record, client.deserializeRecord(ByteBuffer.wrap(record.serialize())));
	}
	
	@Test
	public void testStatusCallbackQuestionableIfFindPeersReturnsNoResults() {
		MutableInt receivedStatus = new MutableInt(-1);
		client.lastStatus = -1; // we're already in a questionable state, so clear this
		client.setStatusCallback((status)->receivedStatus.setValue(status));
		client.routingTable.reset();
		client.findPeers();
		
		assertTrue(Util.waitUntil(100, ()->DHTClient.STATUS_QUESTIONABLE == receivedStatus.intValue()));
	}
	
	@Test
	public void testStatusCallbackQuestionableIfLookupReturnsNoResults() {
		MutableInt receivedStatus = new MutableInt(-1);
		client.lastStatus = -1; // we're already in a questionable state, so clear this
		client.setStatusCallback((status)->receivedStatus.setValue(status));
		client.routingTable.reset();
		client.lookup(clientPeer.id, new Key(crypto), (record)->{});
		
		assertTrue(Util.waitUntil(100, ()->DHTClient.STATUS_QUESTIONABLE == receivedStatus.intValue()));
	}
	
	@Test
	public void testStatusCallbackQuestionableIfAddRecordsCantFindNodes() {
		MutableInt receivedStatus = new MutableInt(-1);
		client.lastStatus = -1; // we're already in a questionable state, so clear this
		client.setStatusCallback((status)->receivedStatus.setValue(status));
		client.routingTable.reset();
		client.addRecord(clientPeer.id, new Key(crypto), makeBogusAd(0));
		
		assertTrue(Util.waitUntil(100, ()->DHTClient.STATUS_QUESTIONABLE == receivedStatus.intValue()));
	}
	
	@Test
	public void testStatusCallbackEstablishingWhenSocketBinding() throws SocketException {
		MutableBoolean seen = new MutableBoolean();
		client.lastStatus = -1; // we're already in a questionable state, so clear this
		client.setStatusCallback((status)->{ if(status == DHTClient.STATUS_ESTABLISHING) seen.setTrue(); });
		client.openSocket();
		assertTrue(Util.waitUntil(100, ()->seen.booleanValue()));
	}
	
	@Test
	public void testStatusCallbackQuestionableWhenSocketBound() throws SocketException {
		MutableInt receivedStatus = new MutableInt(-1);
		client.lastStatus = -1; // we're already in a questionable state, so clear this
		client.setStatusCallback((status)->receivedStatus.setValue(status));
		client.openSocket();
		assertTrue(Util.waitUntil(100, ()->DHTClient.STATUS_QUESTIONABLE == receivedStatus.intValue()));
	}
	
	@Test
	public void testStatusCallbackOfflineWhenSocketFailsToBind() throws SocketException {
		MutableInt receivedStatus = new MutableInt(-1);
		client.bindPort = remote.peer.port;
		client.setStatusCallback((status)->receivedStatus.setValue(status));
		try {
			client.openSocket();
		} catch(SocketException exc) {}
		
		assertTrue(Util.waitUntil(100, ()->DHTClient.STATUS_OFFLINE == receivedStatus.intValue()));
	}
	
	@Test
	public void testStatusCallbackCanSendWhenResponseReceivedAndLastStatusNotGood() throws ProtocolViolationException {
		MutableInt receivedStatus = new MutableInt(-1);
		client.bindPort = remote.peer.port;
		client.setStatusCallback((status)->receivedStatus.setValue(status));
		client.pingMessage(remote.peer, null).send();
		remote.receivePacket(DHTMessage.CMD_PING).makeResponse(new ArrayList<>()).send();
		assertTrue(Util.waitUntil(100, ()->DHTClient.STATUS_CAN_SEND == receivedStatus.intValue()));
	}
	
	@Test
	public void testStatusCallbackDoesNotGetCanSendWhenLastStatusWasGood() throws ProtocolViolationException {
		MutableInt receivedStatus = new MutableInt(-1);
		client.bindPort = remote.peer.port;
		client.setStatusCallback((status)->receivedStatus.setValue(status));
		client.pingMessage(remote.peer, null).send();
		client.lastStatus = DHTClient.STATUS_GOOD;
		remote.receivePacket(DHTMessage.CMD_PING).makeResponse(new ArrayList<>()).send();
		assertFalse(Util.waitUntil(100, ()->-1 != receivedStatus.intValue()));
	}
	
	@Test
	public void testStatusCallbackDoesNotGetCanSendWhenLastStatusWasCanSend() throws ProtocolViolationException {
		MutableInt receivedStatus = new MutableInt(-1);
		client.bindPort = remote.peer.port;
		client.setStatusCallback((status)->receivedStatus.setValue(status));
		client.pingMessage(remote.peer, null).send();
		client.lastStatus = DHTClient.STATUS_CAN_SEND;
		remote.receivePacket(DHTMessage.CMD_PING).makeResponse(new ArrayList<>()).send();
		assertFalse(Util.waitUntil(100, ()->-1 != receivedStatus.intValue()));
	}
	
	@Test
	public void testStatusCallbackGoodWhenRequestReceived() throws ProtocolViolationException {
		MutableInt receivedStatus = new MutableInt(-1);
		client.bindPort = remote.peer.port;
		client.setStatusCallback((status)->receivedStatus.setValue(status));
		clientPeer.ping();
		assertTrue(Util.waitUntil(100, ()->DHTClient.STATUS_GOOD == receivedStatus.intValue()));
	}
	
	@Test
	public void testStatusCallbackDoesNotGetGoodWhenLastStatusWasGood() throws ProtocolViolationException {
		MutableInt receivedStatus = new MutableInt(-1);
		client.bindPort = remote.peer.port;
		client.setStatusCallback((status)->receivedStatus.setValue(status));
		client.lastStatus = DHTClient.STATUS_GOOD;
		clientPeer.ping();
		assertFalse(Util.waitUntil(100, ()->-1 != receivedStatus.intValue()));
	}
	
	@Test
	public void testAutoFindPeersCallsFindPeersOnInterval() throws IOException, ProtocolViolationException {
		DHTClient.autoFindPeersIntervalMs = 50;
		client.autoFindPeers();
		long timeStart = Util.currentTimeMillis();
		for(int i = 0; i < 8; i++) {
			remote.receivePacket().makeResponse(new ArrayList<>()).send();
			assertTrue(Util.currentTimeMillis() - timeStart >= i*DHTClient.autoFindPeersIntervalMs);
		}
	}
}
