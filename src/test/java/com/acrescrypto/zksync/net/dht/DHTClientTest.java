package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import java.util.Collection;
import java.util.HashMap;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PRNG;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.net.TCPPeerAdvertisement;
import com.acrescrypto.zksync.utility.Shuffler;
import com.acrescrypto.zksync.utility.Util;

public class DHTClientTest {
	class RemotePeer implements AutoCloseable {
		DHTClient listenClient;
		DHTPeer peer;
		DatagramSocket socket;
		PrivateDHKey dhKey;
		boolean strict = true;
		
		public RemotePeer() throws SocketException, UnknownHostException {
			socket = new DatagramSocket(0, InetAddress.getByName("localhost"));
			socket.setReuseAddress(true);
			assertTrue(socket.isBound());
			dhKey = crypto.makePrivateDHKey();
			initListenClient();
			peer = new DHTPeer(client, socket.getLocalAddress().getHostAddress(), socket.getLocalPort(), dhKey.publicKey().getBytes());
		}
		
		public void initListenClient() {
			try {
				listenClient = new DHTClient(new Key(crypto), new Blacklist(new RAMFS(), "blacklist", new Key(crypto)));
			} catch(Exception exc) {
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

		DHTMessage receivePacket() throws IOException, ProtocolViolationException {
			return receivePacket((byte) -1);
		}
		
		DHTMessage receivePacket(byte cmd) throws ProtocolViolationException {
			class Holder { DatagramPacket packet; }
			byte[] receiveData = new byte[65536];
			Holder holder = new Holder();
			
			new Thread(()->{
				DatagramPacket packet = new DatagramPacket(receiveData, receiveData.length);
				try {
					socket.receive(packet);
					holder.packet = packet;
				} catch(IOException exc) {
				}
			}).start();
			
			Util.waitUntil(500, ()->holder.packet != null);
			if(strict) assertTrue(holder.packet != null);
			else if(holder.packet == null) return null;
			
			DatagramPacket packet = holder.packet;

			assertTrue(packet.getLength() <= DHTClient.MAX_DATAGRAM_SIZE);
			DHTMessage msg = new DHTMessage(listenClient, packet.getAddress().getHostAddress(), packet.getPort(), ByteBuffer.wrap(packet.getData(), 0, packet.getLength()));
			if(cmd >= 0) assertEquals(cmd, msg.cmd);
			
			return msg;
		}
		
		public void close() {
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
				remote.addSubsetOfPeers(remotes, Math.max(Math.min(64, size/4), 1));
			}
		}
		
		public void run() {
			ArrayList<Thread> threads = new ArrayList<>(remotes.size());
			MutableBoolean failed = new MutableBoolean();
			for(RemotePeer remote : remotes) {
				Thread t = new Thread(()-> {
					Thread.currentThread().setName("Remote peer handler " + remote.peer.port);
					try {
						handlers.get(remote).handle(remote);
					} catch(Exception exc) {
						if(closed) return;
						failed.setTrue();
					}
				});
				threads.add(t);
				t.start();
			}
			
			for(Thread t : threads) {
				try {
					t.join(500);
					// assertFalse(t.isAlive());
				} catch(InterruptedException exc) {}
			}
			
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
					Collection<DHTPeer> results = remote.listenClient.routingTable.closestPeers(new DHTID(msg.payload), DHTBucket.MAX_BUCKET_CAPACITY);
					msg.makeResponse(results).send();
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
	Blacklist blacklist;
	Key storageKey;
	DHTClient client;
	
	RemotePeer remote;
	
	@Before
	public void beforeEach() throws IOException, InvalidBlacklistException {
		crypto = new CryptoSupport();

		blacklist = new Blacklist(new RAMFS(), "blacklist", new Key(crypto));
		storageKey = new Key(crypto);
		client = new DHTClient(storageKey, blacklist);
		remote = new RemotePeer();
		
		client.addPeer(remote.peer);
		client.listen("127.0.0.1", 0);
	}
	
	@After
	public void afterEach() {
		remote.close();
		client.close();
		Util.setCurrentTimeNanos(-1);
		
		DHTClient.LOOKUP_RESULT_MAX_WAIT_TIME_MS = DHTClient.DEFAULT_LOOKUP_RESULT_MAX_WAIT_TIME_MS;
		DHTClient.MESSAGE_EXPIRATION_TIME_MS = DHTClient.DEFAULT_MESSAGE_EXPIRATION_TIME_MS;
	}
	
	@Test
	public void testConstructorWithoutExistingData() {
		assertEquals(blacklist, client.blacklist);
		assertEquals(storageKey, client.storageKey);
		assertEquals(blacklist.getFS(), client.storage);
		assertEquals(crypto, client.crypto);
		assertNotNull(client.routingTable);
		assertNotNull(client.store);
	}
	
	@Test
	public void testConstructorWithExistingData() {
		DHTClient client1 = new DHTClient(storageKey, blacklist);

		assertEquals(blacklist, client1.blacklist);
		assertEquals(storageKey, client1.storageKey);
		assertEquals(blacklist.getFS(), client1.storage);
		assertEquals(crypto, client1.crypto);
		assertNotNull(client1.routingTable);
		assertNotNull(client1.store);

		assertArrayEquals(client.key.getBytes(), client1.key.getBytes());
		assertArrayEquals(client.key.publicKey().getBytes(), client1.key.publicKey().getBytes());
		assertArrayEquals(client.tagKey.getRaw(), client1.tagKey.getRaw());
		assertEquals(client.id, client1.id);
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
		assertArrayEquals(client.id.rawId, msg.payload);
	}
	
	@Test
	public void testLookupTriggersSearchOperationForRequestedId() throws IOException, ProtocolViolationException {
		DHTID searchId = new DHTID(crypto.rng(client.idLength()));
		client.lookup(searchId, (resp)->{});
		DHTMessage msg = remote.receivePacket(DHTMessage.CMD_FIND_NODE);
		assertArrayEquals(searchId.rawId, msg.payload);
	}
	
	@Test
	public void testLookupInvokesCallbackWithEachReceivedRecord() throws IOException, ProtocolViolationException {
		MutableInt numSeen = new MutableInt();
		MutableBoolean seenNull = new MutableBoolean();
		ArrayList<DHTRecord> records = new ArrayList<>();
		
		DHTID searchId = new DHTID(crypto.rng(client.idLength()));
		client.lookup(searchId, (resp)->{
			assertFalse(seenNull.booleanValue());
			if(resp == null) {
				seenNull.setTrue();
				return;
			}
			
			assertTrue(records.contains(resp));
			numSeen.increment();
		});
		
		DHTMessage findNodeReq = remote.receivePacket(DHTMessage.CMD_FIND_NODE);
		ArrayList<DHTPeer> peers = new ArrayList<>(1);
		peers.add(remote.peer);
		findNodeReq.makeResponse(peers).send();
		
		DHTMessage getRecordsReq = remote.receivePacket(DHTMessage.CMD_GET_RECORDS);
		for(int i = 0; i < DHTRecordStore.MAX_RECORDS_PER_ID; i++) records.add(makeBogusAd(i));
		getRecordsReq.makeResponse(records).send();
		
		assertTrue(Util.waitUntil(100, ()->seenNull.booleanValue()));
		assertEquals(records.size(), numSeen.intValue());
	}
	
	@Test
	public void testLookupInvokesCallbackWithNullIfNoResponseReceivedInTime() throws IOException, ProtocolViolationException {
		MutableBoolean seenNull = new MutableBoolean();
		DHTClient.LOOKUP_RESULT_MAX_WAIT_TIME_MS = 10;
		
		DHTID searchId = new DHTID(crypto.rng(client.idLength()));
		client.lookup(searchId, (resp)->{
			assertFalse(seenNull.booleanValue());
			assertNull(resp);
			seenNull.setTrue();
		});
		
		DHTMessage findNodeReq = remote.receivePacket(DHTMessage.CMD_FIND_NODE);
		ArrayList<DHTPeer> peers = new ArrayList<>(1);
		peers.add(remote.peer);
		findNodeReq.makeResponse(peers).send();
		
		remote.receivePacket(DHTMessage.CMD_GET_RECORDS);		
		assertTrue(Util.waitUntil(100, ()->seenNull.booleanValue()));
	}

	@Test
	public void testLookupInvokesCallbackWithNullIfPartialResponseReceivedInTime() throws IOException, ProtocolViolationException {
		MutableBoolean seenNull = new MutableBoolean();
		MutableInt numSeen = new MutableInt();
		ArrayList<DHTRecord> records = new ArrayList<>();
		DHTClient.LOOKUP_RESULT_MAX_WAIT_TIME_MS = 50;
		
		DHTID searchId = new DHTID(crypto.rng(client.idLength()));
		client.lookup(searchId, (resp)->{
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
		findNodeReq.makeResponse(peers).send();
		
		DHTMessage getRecordsReq = remote.receivePacket(DHTMessage.CMD_GET_RECORDS);
		for(int i = 0; i < DHTRecordStore.MAX_RECORDS_PER_ID; i++) records.add(makeBogusAd(i));
		
		class RiggedMessage extends DHTMessage {
			public RiggedMessage(DHTMessage msg) { super(msg.peer, msg.cmd, msg.msgId, msg.items); }			
			@Override protected int numPacketsNeeded() { return super.numPacketsNeeded() + 1; }
		};
		
		new RiggedMessage(getRecordsReq.makeResponse(records)).send();
		assertTrue(Util.waitUntil(100, ()->seenNull.booleanValue()));
	}
	
	@Test
	public void testLookupObtainsRecordsFromMultiplePeers() throws IOException, ProtocolViolationException {
		int recordsPerPeer = 4;
		
		MutableInt numSeen = new MutableInt();
		MutableBoolean seenNull = new MutableBoolean();
		ArrayList<RemotePeer> remotes = new ArrayList<>();
		ArrayList<DHTPeer> peers = new ArrayList<>();
		
		remotes.add(remote);
		for(int i = 0; i < DHTSearchOperation.MAX_RESULTS-1; i++) {
			remotes.add(new RemotePeer());
		}

		for(RemotePeer r : remotes) peers.add(r.peer);

		DHTID searchId = new DHTID(crypto.rng(client.idLength()));
		client.lookup(searchId, (resp)->{
			assertFalse(seenNull.booleanValue());
			if(resp == null) {
				seenNull.setTrue();
				return;
			}
			
			numSeen.increment();
		});
		
		int i = 0;
		for(RemotePeer r : remotes) {
			DHTMessage findNodeReq = r.receivePacket(DHTMessage.CMD_FIND_NODE);
			findNodeReq.makeResponse(peers).send(); 
		}		
		
		for(RemotePeer r : remotes) {
			ArrayList<DHTRecord> peerRecords = new ArrayList<>();
			for(int j = 0; j < recordsPerPeer; j++) {
				peerRecords.add(makeBogusAd(recordsPerPeer*i+j));
			}

			DHTMessage getRecordsReq = r.receivePacket(DHTMessage.CMD_GET_RECORDS);
			getRecordsReq.makeResponse(peerRecords).send();
		}
		
		assertTrue(Util.waitUntil(100, ()->seenNull.booleanValue()));
		assertEquals(recordsPerPeer*remotes.size(), numSeen.intValue());
	}
	
	@Test
	public void testAddRecordCallsAddRecordOnClosestPeersForID() throws SocketException, UnknownHostException {
		DHTID searchId = new DHTID(crypto.rng(client.idLength()));
		try(TestNetwork network = new TestNetwork(16)) {
			MutableInt numReceived = new MutableInt();
			
			network.setHandlerForClosest(searchId, DHTSearchOperation.MAX_RESULTS, (remote)->{
				DHTMessage findNodeMsg = remote.receivePacket(DHTMessage.CMD_FIND_NODE);
				assertArrayEquals(searchId.rawId, findNodeMsg.payload);
				findNodeMsg.makeResponse(remote.listenClient.routingTable.closestPeers(searchId, DHTSearchOperation.MAX_RESULTS)).send();
				
				DHTMessage addRecordMsg = remote.receivePacket(DHTMessage.CMD_ADD_RECORD);
				synchronized(this) { numReceived.increment(); }
				// TODO DHT: (implement) validate auth tag
			});
			
			client.routingTable.reset();
			client.addPeer(network.remotes.get(0).peer);
			client.addRecord(searchId, makeBogusAd(0));
			network.run();
			assertEquals(DHTSearchOperation.MAX_RESULTS, numReceived.intValue());
		}
	}
	// addRecord calls addRecord on closest peers for ID
	
	// authTagLength is hash length
	// idLength is hash length
	
	// processMessage tolerates indecipherable messages
	// processMessage blacklists peers when handler triggers ProtocolViolationException
	// processMessage adds peers to routing table

	// processResponse calls acknowledgedMessage on sending peer
	// processResponse updates routing table as fresh if response matches outstanding request
	// processResponse invokes request callback when response matches outstanding request
	
	// processRequest responds to pings
	
	// processRequest triggers violation if request is findNode and payload is not an ID
	// processRequest replies to findNode requests with closest peers
	// processRequest replies to findNode requests with truncated list if not enough peers are available
	// processRequest replies to findNode requests with partial list if more peers are available
	
	// processRequest triggers violation if request is getRecords and payload is not an ID
	// processRequest responds to getRecords requests with list of all records for ID
	// processRequest responds to getRecords requests with empty list if ID not present
	
	// processRequest triggers violation if request does not include auth tag
	// processRequest triggers violation if request auth tag is not valid
	// processRequest triggers violation if request does not include id
	// processRequest triggers violation if request does not include record payload
	// processRequest tolerates unsupported record types
	// processRequest triggers violation if request record is not valid
	// processRequest adds valid record to store
	
	// pingMessage constructs appropriate ping message
	// findNodeMessage constructs appropriate findNode message
	// getRecordsMessage constructs appropriate getRecords message
	// addRecordMessage constructs appropriate addRecord message
	
	// deserializeRecord deserializes into approrpiate record object
	
	// datagrams never exceed max

	// status callback gets questionable state if findPeers returns no results
	// status callback gets questionable state if lookup returns no results
	// status callback gets questionable state if addRecord returns no results
	// status callback gets establishing state when socket binding
	// status callback gets questionable state when socket bound
	// status callback gets offline state when socket fails to bind
	// status callback gets CAN_SEND state when not marked GOOD or CAN_SEND and response received
	// status callback does not get CAN_SEND state when marked CAN_SEND and response received
	// status callback does not get CAN_SEND state when marked GOOD and response received
	// status callback gets GOOD state when request received
}
