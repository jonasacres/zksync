package com.acrescrypto.zksyncweb.resources.dht;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.glassfish.grizzly.http.server.HttpServer;
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
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.dht.DHTBootstrapper;
import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.net.dht.DHTID;
import com.acrescrypto.zksync.net.dht.DHTMessage;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageCallback;
import com.acrescrypto.zksync.net.dht.DHTPeer;
import com.acrescrypto.zksync.net.dht.DHTProtocolManager;
import com.acrescrypto.zksync.net.dht.DHTRecord;
import com.acrescrypto.zksync.net.dht.DHTRecordStore;
import com.acrescrypto.zksync.net.dht.DHTRoutingTable;
import com.acrescrypto.zksync.net.dht.DHTSocketManager;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.acrescrypto.zksyncweb.data.XDHTPeerInfo;
import com.fasterxml.jackson.databind.JsonNode;

public class DHTResourceTest {
	private HttpServer server;
	private WebTarget target;
	private String basepath;
	private DummyDHTClient client;

	class DummyDHTClient extends DHTClient {
		boolean closed, purged;
		DHTPeer addedPeer;

		public DummyDHTClient() throws IOException {
			byte[] netKey        = State.sharedState().getMaster().getGlobalConfig().getString("net.dht.network").getBytes();
			
			this.master          = State.sharedState().getMaster();
			this.crypto          = CryptoSupport.defaultCrypto();
			
			this.socketManager   = new DummyDHTSocketManager  (this);
			this.protocolManager = new DummyDHTProtocolManager(this);
			this.store           = new DummyDHTRecordStore    (this);
			this.routingTable    = new DummyDHTRoutingTable   (this);
			this.bootstrapper    = new DummyDHTBootstrapper   (this);

			this.privateKey      = new PrivateDHKey(State.sharedCrypto());
			this.storageKey      = Key.blank(State.sharedCrypto());
			this.tagKey          = Key.blank(State.sharedCrypto());
			this.id              = DHTID.withKey(privateKey.publicKey());
			this.networkId       = crypto.hash(netKey);
			
			this.socketManager.setBindPort(1234);
			this.socketManager.setBindAddress("0.0.0.0");
			this.socketManager.setMonitorRx(new DummyMonitor(1000));
			this.socketManager.setMonitorTx(new DummyMonitor(2000));
			
			subscriptions.add(master.getGlobalConfig().subscribe("net.dht.network").asString((network)->{
				byte[] newNetworkId = crypto.hash(network.getBytes());
				this.networkId = newNetworkId;
			}));
		}
		
		@Override public DummyDHTProtocolManager getProtocolManager() { return (DummyDHTProtocolManager) protocolManager; }
		@Override public DummyDHTBootstrapper bootstrapper() { return (DummyDHTBootstrapper) bootstrapper; }
		@Override public void    close() { closed = true; closeSubscriptions(); }
		@Override public void    purge() throws IOException { purged = true; super.purge(); }
		@Override public int     numPendingRequests() { return 777; }
		@Override public void    addPeer(DHTPeer peer) { this.addedPeer = peer; }
		@Override public boolean isEnabled() { return true; }
		@Override public FS getStorage() {
			try {
				return State.sharedState().getMaster().getStorage();
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}
	
	class DummyDHTBootstrapper extends DHTBootstrapper {
		boolean calledBootstrapWithoutBody, calledBootstrapWithBody;
		String peerfileString;
		
		public DummyDHTBootstrapper(DummyDHTClient client) {
			super(client);
		}
		
		@Override
		public void bootstrap() {
			calledBootstrapWithoutBody = true;
		}
		
		@Override
		public void bootstrapFromPeerFileString(String peerfile) {
			calledBootstrapWithBody = true;
			peerfileString             = peerfile;
		}
	}
	
	class DummyDHTSocketManager extends DHTSocketManager {
		DummyDHTClient client;
		
		public DummyDHTSocketManager(DummyDHTClient client) {
			super.client = this.client = client;
		}
		
		@Override
		public void sendDatagram(DatagramPacket packet) {}
	}
	
	class DummyDHTProtocolManager extends DHTProtocolManager {
		DummyDHTClient client;
		HashSet<DHTPeer> pinged = new HashSet<>();
		boolean calledFindPeers;
		
		public DummyDHTProtocolManager(DummyDHTClient client) {
			super.client = this.client = client;
		}
		
		@Override
		public void findPeers() {
			calledFindPeers = true;
		}
		
		@Override
		public DHTMessage pingMessage(DHTPeer peer, DHTMessageCallback callback) {
			pinged.add(peer);
			return super.pingMessage(peer, callback);
		}
	}

	class DummyDHTRecordStore extends DHTRecordStore {
		boolean reset;
		public DummyDHTRecordStore(DummyDHTClient client) {
			this.client = client;
		}
		
		@Override public void reset() { reset = true; }
		@Override public int numIds() { return reset ? 0 : 12; }
		@Override public int numRecords() { return reset ? 0 : 321; }
		@Override public Map<DHTID, Collection<StoreEntry>> records() {
			if(reset) return new HashMap<DHTID, Collection<StoreEntry>>();
			CryptoSupport crypto = CryptoSupport.defaultCrypto();
			HashMap<DHTID, Collection<StoreEntry>> map = new HashMap<>();
			int recordsPerId = 5, numIds = 3;
			
			for(int i = 0; i < numIds; i++) {
				DHTID id                    = DHTID.withBytes(crypto.hash(Util.serializeInt(i)));
				LinkedList<StoreEntry> list = new LinkedList<>();
				map.put(id, list);
				
				for(int j = 0; j < recordsPerId; j++) {
					int         x      = i*recordsPerId + j;
					DummyRecord record = new DummyRecord(x);
					StoreEntry entry   = new StoreEntry(record, Util.serializeInt(i));
					
					list.add(entry);
				}
			}
			
			return map;
		}
	}
	
	class DummyRecord extends DHTRecord {
		byte[] contents;
		boolean reachable = true, valid = true;
		int index;
		
		public DummyRecord(int i) {
			CryptoSupport crypto = CryptoSupport.defaultCrypto();
			index = i;
			ByteBuffer buf = ByteBuffer.allocate(32);
			buf.putInt(i);
			buf.put(crypto.prng(Util.serializeInt(i)).getBytes(buf.remaining()));
			contents = buf.array();
			sender = makePeer(client, i);
		}
		
		@Override
		public byte[] serialize() {
			ByteBuffer serialized = ByteBuffer.allocate(2+contents.length);
			serialized.putShort((short) contents.length);
			serialized.put(contents);
			return serialized.array();
		}

		@Override public boolean isReachable() { return true; }
		@Override public String routingInfo() { return "dummy-" + index; }
		@Override public void deserialize(ByteBuffer serialized) {}
		@Override public boolean isValid() { return true; }
	}


	class DummyDHTRoutingTable extends DHTRoutingTable {
		DummyDHTClient client;

		public DummyDHTRoutingTable(DummyDHTClient client) {
			super.client = this.client = client;
			this.allPeers = new ArrayList<>();
			for(int i = 0; i < 64; i++) {
				allPeers.add(makePeer(client, i));
			}
		}
	}

	class DummyMonitor extends BandwidthMonitor {
		long bytesPerSecond;

		public DummyMonitor(long bytesPerSecond) {
			this.bytesPerSecond = bytesPerSecond;
		}

		@Override public long getBytesPerSecond() { return bytesPerSecond; }
		@Override public long getLifetimeBytes() { return bytesPerSecond + 1; }
	}

	public DHTPeer makePeer(DHTClient client, int i) {
		try {
			CryptoSupport crypto = CryptoSupport.defaultCrypto();
			DHTPeer peer = new DHTPeer(client,
					"127.0.0." + i,
					1000+i,
					crypto.hash(Util.serializeInt(i)));
	
			Util.setCurrentTimeMillis(i*100000);
			peer.acknowledgedMessage();
			
			for(int j = 0; j < i; j++) peer.missedMessage();
	
			return peer;
		} catch(UnknownHostException exc) {
			fail();
			return null;
		}
	}

	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
		WebTestUtils.squelchGrizzlyLogs();
	}

	@SuppressWarnings("deprecation")
	@Before
	public void beforeEach() throws Exception {
		State.setTestState();
		server = Main.startServer(TestUtils.testHttpPort());
		Client c = ClientBuilder.newClient();
		target = c.target(TestUtils.testHttpUrl());
		Util.setCurrentTimeMillis(0);

		client = new DummyDHTClient();
		State.sharedState().getMaster().getDHTClient().close();
		State.sharedState().getMaster().setDHTClient(client);

		basepath = "/dht/";
	}

	@After
	public void afterEach() throws Exception {
		server.shutdownNow();
		State.clearState();
		Util.setCurrentTimeMillis(-1);
	}

	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}

	void validateDHTClientInfo(JsonNode info, DHTClient client) throws IOException {
		int expectedGood = 0, expectedBad = 0, expectedQuestionable = 0;

		for(DHTPeer peer : client.getRoutingTable().allPeers()) {
			if(peer.isBad()) {
				expectedBad++;
			} else if(peer.isQuestionable()) {
				expectedQuestionable++;
			} else {
				expectedGood++;
			}
		}

		assertArrayEquals(client.getId().serialize(), info.get("peerId").binaryValue());
		assertArrayEquals(client.getPublicKey().getBytes(), info.get("pubKey").binaryValue());
		assertArrayEquals(client.getNetworkId(), info.get("networkId").binaryValue());

		assertEquals(client.getBindAddress(), info.get("bindAddress").textValue());
		assertEquals(client.getPort(), info.get("udpPort").intValue());
		assertEquals(client.getStatus(), info.get("status").intValue());

		assertEquals(client.isClosed(), info.get("closed").booleanValue());
		assertEquals(client.isInitialized(), info.get("initialized").booleanValue());
		assertEquals(client.getRoutingTable().allPeers().size(), info.get("numPeers").intValue());
		assertEquals(expectedGood, info.get("numGoodPeers").intValue());
		assertEquals(expectedQuestionable, info.get("numQuestionablePeers").intValue());
		assertEquals(expectedBad, info.get("numBadPeers").intValue());

		assertEquals(client.numPendingRequests(), info.get("numPendingRequests").intValue());
		assertEquals(client.getMonitorRx().getBytesPerSecond(), info.get("bytesPerSecondRx").doubleValue(), 1e-5);
		assertEquals(client.getMonitorTx().getBytesPerSecond(), info.get("bytesPerSecondTx").doubleValue(), 1e-5);
		assertEquals(client.getMonitorRx().getLifetimeBytes(), info.get("lifetimeBytesRx").doubleValue(), 1e-5);
		assertEquals(client.getMonitorTx().getLifetimeBytes(), info.get("lifetimeBytesTx").doubleValue(), 1e-5);

		assertEquals(client.getRecordStore().numIds(), info.get("numRecordIds").intValue());
		assertEquals(client.getRecordStore().numRecords(), info.get("numRecords").intValue());
	}

	void validateDHTPeerInfo(JsonNode info, DHTPeer peer) throws IOException {
		assertArrayEquals(peer.getId().serialize(), info.get("id").binaryValue());
		assertArrayEquals(peer.getKey().getBytes(), info.get("pubKey").binaryValue());
		assertEquals(peer.getPort(), info.get("port").intValue());
		assertEquals(peer.getAddress(), info.get("address").textValue());
		assertEquals(peer.getMissedMessages(), info.get("missedMessages").intValue());
		assertEquals(peer.getLastSeen(), info.get("lastSeen").longValue());
	}

	@Test
	public void getDhtReturnsDhtClientInfo() throws IOException {
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		validateDHTClientInfo(resp, client);
	}


	@Test
	public void getDhtPeersReturnsDhtClientInfo() throws IOException {
		JsonNode resp = WebTestUtils.requestGet(target, basepath + "peers");
		assertEquals(client.getRoutingTable().allPeers().size(), resp.get("peers").size());

		for(int i = 0 ; i < resp.get("peers").size(); i++) {
			JsonNode info = resp.get("peers").get(i);
			DHTPeer peer = ((ArrayList<DHTPeer>) client.getRoutingTable().allPeers()).get(i);
			validateDHTPeerInfo(info, peer);
		}
	}

	@Test
	public void testPutDhtPeersAddsRequestedPeer() {
		CryptoSupport crypto = CryptoSupport.defaultCrypto();
		XDHTPeerInfo info = new XDHTPeerInfo();
		info.setAddress("10.1.2.3");
		info.setPort(1234);
		info.setPubKey(crypto.hash(Util.serializeInt(0)));

		WebTestUtils.requestPut(target, basepath + "peers", info);
		assertEquals(client.addedPeer.getAddress(), info.getAddress());
		assertEquals(client.addedPeer.getPort(), info.getPort().intValue());
		assertArrayEquals(client.addedPeer.getKey().getBytes(), info.getPubKey());
	}

	@Test
	public void testRegenerateCausesKeyChange() throws IOException {
		WebTestUtils.requestPost(target, basepath + "regenerate", null);
		DHTClient newClient = State.sharedState().getMaster().getDHTClient();
		assertFalse(Arrays.equals(client.getPublicKey().getBytes(),
				newClient.getPublicKey().getBytes()));
	}

	@Test
	public void testRegenerateClearsRecordStore() throws IOException {
		WebTestUtils.requestPost(target, basepath + "regenerate", null);
		DHTClient newClient = State.sharedState().getMaster().getDHTClient();
		assertEquals(0, newClient.getRecordStore().numRecords());
	}

	@Test
	public void testRegenerateClearsRoutingTable() throws IOException {
		ZKMaster master = State.sharedState().getMaster();
		master.getGlobalConfig().set("net.dht.bootstrap.enabled", false);
		
		WebTestUtils.requestPost(target, basepath + "regenerate", null);
		DHTClient newClient = master.getDHTClient();
		assertEquals(0, newClient.getRoutingTable().allPeers().size());
	}

	@Test
	public void testRegenerateUsesPreviousNetworkId() throws IOException {
		State.sharedState().getMaster().getGlobalConfig().set("net.dht.network", "test");
		WebTestUtils.requestPost(target, basepath + "regenerate", null);
		DHTClient newClient = State.sharedState().getMaster().getDHTClient();
		assertArrayEquals(client.getNetworkId(), newClient.getNetworkId());
	}
	
	@Test
	public void testPeerFileIncludesNetworkId() throws IOException {
		byte[] networkId = WebTestUtils
			.requestGet(target, basepath + "peerfile")
			.get("networkId")
			.binaryValue();
		assertArrayEquals(client.getNetworkId(), networkId);
	}
	
	@Test
	public void testPeerFileIncludesSelf() throws IOException {
		MutableBoolean found = new MutableBoolean();
		
		WebTestUtils
			.requestGet(target, basepath + "peerfile")
			.get("peers")
			.elements()
			.forEachRemaining((peer)-> {
				try {
					byte[] pubKey          = peer.get("pubKey") .binaryValue();
					int    port            = peer.get("port")   .asInt();
					String addr            = peer.get("address").asText();
					
					String expectedAddress = client.getProtocolManager().getLocalPeer().getAddress();
				
					if(!Arrays.equals(pubKey, client.getPublicKey().getBytes())) return;
					assertEquals     (client.getPort(),           port);
					assertEquals     (expectedAddress,            addr);
					
					found.setTrue();
				} catch(IOException exc) {
					fail();
				}
			});
		
		assertTrue(found.booleanValue());
	}
	
	@Test
	public void testRecordsListsAllRecordsInStore() {
		JsonNode        records      = WebTestUtils
			                           .requestGet(target, basepath + "records")
			                           .get("records");
		HashSet<String>  seenIds     = new HashSet<>();
		HashSet<Integer> seenRecords = new HashSet<>();
		CryptoSupport    crypto      = CryptoSupport.defaultCrypto();
		
		int numIds       = 3,
			recordsPerId = 5; // match values in records() in DummyRecordStore
		
		records.fieldNames().forEachRemaining((id)->{
			seenIds.add(id);
			JsonNode recordsForId = records.get(id);
			int i;
			for(i = 0; i < numIds; i++) {
				String ss = DHTID.withBytes(crypto.hash(Util.serializeInt(i))).toFullString();
				if(ss.equals(id)) break;
			}
			
			final int idIndex = i;
			assertTrue(i < numIds);
			
			recordsForId.forEach((record)->{
				try {
					byte[] contents = record.get("data").binaryValue();
					int xx = ByteBuffer.wrap(contents).position(2).getInt();
					int ii = xx / recordsPerId,
						jj = xx % recordsPerId;
					assertEquals(idIndex, ii);
					assertFalse(seenRecords.contains(xx));
					assertTrue(0 <= jj && jj < recordsPerId);
					seenRecords.add(xx);
					
					DHTPeer expectedPeer = makePeer(client, xx);
					JsonNode peer = record.get("sender");
					
					assertEquals     ("dummy-" + xx,
							          record.get("routingInfo").asText());
					assertEquals     (expectedPeer.getPort(),
							          peer.get("port").asInt());
					assertEquals     (expectedPeer.getAddress(),
					                  peer.get("address").asText());
					assertArrayEquals(expectedPeer.getId().serialize(),
							          peer.get("id").binaryValue());
					assertArrayEquals(expectedPeer.getKey().getBytes(),
					                  peer.get("pubKey").binaryValue());
				} catch (IOException exc) {
					exc.printStackTrace();
					fail();
				}
			});
		});
		
		assertEquals(numIds,              seenIds    .size());
		assertEquals(numIds*recordsPerId, seenRecords.size());
	}
	
	// TODO: testPeerFileIncludesGoodPeers
	// TODO: testPeerFileDoesNotIncludeBadPeers
	// TODO: testPeerFileDoesNotIncludeQuestionablePeers
	
	@Test
	public void testDeleteDhtPeersPurgesRoutingTable() {
		WebTestUtils.requestDelete(target, basepath + "peers");
		assertEquals(0, client.getRoutingTable().allPeers().size());
	}
	
	@Test
	public void testDeleteDhtPeersDoesNotPurgeRecords() {
		WebTestUtils.requestDelete(target, basepath + "peers");
		assertNotEquals(0, client.getRecordStore().numRecords());
	}
	
	@Test
	public void testDeleteDhtPeersDoesNotAlterPublicKey() {
		PublicDHKey key = client.getPublicKey();
		WebTestUtils.requestDelete(target, basepath + "peers");
		assertEquals(key, client.getPublicKey());
	}
	
	@Test
	public void testDeleteDhtRecordsPurgesRecordStore() {
		WebTestUtils.requestDelete(target, basepath + "records");
		assertEquals(0, client.getRecordStore().numRecords());
	}
	
	@Test
	public void testDeleteDhtRecordsDoesNotPurgePeers() {
		WebTestUtils.requestDelete(target, basepath + "records");
		assertNotEquals(0, client.getRoutingTable().allPeers().size());
	}
	
	@Test
	public void testDeleteDhtRecordsDoesNotAlterPublicKey() {
		PublicDHKey key = client.getPublicKey();
		WebTestUtils.requestDelete(target, basepath + "records");
		assertEquals(key, client.getPublicKey());
	}
	
	@Test
	public void testRefreshPingsAllPeers() {
		WebTestUtils.requestPost(target, basepath + "refresh", null);
		for(DHTPeer peer : client.getRoutingTable().allPeers()) {
			assertTrue(client.getProtocolManager().pinged.contains(peer));
		}
	}
	
	@Test
	public void testRefreshFindsNewPeers() {
		WebTestUtils.requestPost(target, basepath + "refresh", null);
		assertTrue(client.getProtocolManager().calledFindPeers);
	}
	
	@Test
	public void testBootstrapWithoutBodyInvokesConfiguredBootstrap() {
		WebTestUtils.requestPost(target, basepath + "bootstrap", null);
		assertTrue(client.bootstrapper().calledBootstrapWithoutBody);
	}
	
	@Test
	public void testBootstrapWithoutBodyIsSafeIfConfiguredBootstrapIsBlank() throws IOException {
		State.sharedState().getMaster().getGlobalConfig().set("net.dht.bootstrap.peerfile", "");
		WebTestUtils.requestPost(target, basepath + "bootstrap", null);
		assertTrue(client.bootstrapper().calledBootstrapWithoutBody);
	}
	
	@Test
	public void testBootstrapWithoutBodyCausesBootstrapEvenIfBootstrapDisabled() throws IOException {
		State.sharedState().getMaster().getGlobalConfig().set("net.dht.bootstrap.enabled", false);
		WebTestUtils.requestPost(target, basepath + "bootstrap", null);
		assertTrue(client.bootstrapper().calledBootstrapWithoutBody);
	}
	
	@Test
	public void testBootstrapWithBodyInvokesWithPeerFileString() {	
		String body = "hello there!";
		WebTestUtils.requestBinaryPost(target, basepath + "bootstrap", body.getBytes());
		assertTrue(client.bootstrapper().calledBootstrapWithBody);
		assertEquals(body, client.bootstrapper().peerfileString);
	}
}
