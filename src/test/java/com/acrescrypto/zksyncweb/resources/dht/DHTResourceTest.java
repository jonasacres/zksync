package com.acrescrypto.zksyncweb.resources.dht;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.net.dht.DHTID;
import com.acrescrypto.zksync.net.dht.DHTPeer;
import com.acrescrypto.zksync.net.dht.DHTRecordStore;
import com.acrescrypto.zksync.net.dht.DHTRoutingTable;
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
			this.crypto = CryptoSupport.defaultCrypto();
			this.monitorRx = new DummyMonitor(1000);
			this.monitorTx = new DummyMonitor(2000);
			this.store = new DummyDHTRecordStore();
			this.routingTable = new DummyDHTRoutingTable(this);

			this.key = new PrivateDHKey(State.sharedCrypto());
			this.id = new DHTID(key.publicKey());
			this.pendingRequests = new ArrayList<>();
			this.networkId = crypto.hash(State.sharedState().getMaster().getGlobalConfig().getString("net.dht.network").getBytes());

			this.bindAddress = "0.0.0.0";
			this.bindPort = 1234;
			
			subscriptions.add(State.sharedState().getMaster().getGlobalConfig().subscribe("net.dht.network").asString((network)->{
				byte[] newNetworkId = crypto.hash(network.getBytes());
				this.networkId = newNetworkId;
			}));
		}

		@Override public void close() { closed = true; closeSubscriptions(); }
		@Override public void purge() { purged = true; }
		@Override public int numPendingRequests() { return 777; }
		@Override public void addPeer(DHTPeer peer) { this.addedPeer = peer; }
	}

	class DummyDHTRecordStore extends DHTRecordStore {
		public DummyDHTRecordStore() {
		}

		@Override public int numIds() { return 12; }
		@Override public int numRecords() { return 321; }
	}

	class DummyDHTRoutingTable extends DHTRoutingTable {
		DummyDHTClient client;

		public DummyDHTRoutingTable(DummyDHTClient client) {
			this.client = client;
			this.allPeers = new ArrayList<>();
			for(int i = 0; i < 64; i++) {
				allPeers.add(makePeer(i));
			}
		}

		public DHTPeer makePeer(int i) {
			CryptoSupport crypto = CryptoSupport.defaultCrypto();
			DHTPeer peer = new DHTPeer(client,
					"127.0.0." + i,
					1000+i,
					crypto.hash(Util.serializeInt(i)));

			Util.setCurrentTimeMillis(i*100000);
			peer.acknowledgedMessage();
			for(int j = 0; j < i; j++) peer.missedMessage();

			return peer;
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

	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
		WebTestUtils.squelchGrizzlyLogs();
	}

	@SuppressWarnings("deprecation")
	@Before
	public void beforeEach() throws Exception {
		State.setTestState();
		server = Main.startServer();
		Client c = ClientBuilder.newClient();
		target = c.target(Main.BASE_URI);
		Util.setCurrentTimeMillis(0);

		client = new DummyDHTClient();
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
		WebTestUtils.requestPost(target, basepath + "regenerate", null);
		DHTClient newClient = State.sharedState().getMaster().getDHTClient();
		assertEquals(0, newClient.getRoutingTable().allPeers().size());
	}

	@Test
	public void testRegenerateUsesPreviousNetworkId() throws IOException {
		State.sharedState().getMaster().getGlobalConfig().set("net.dht.network", "test");
		WebTestUtils.requestPost(target, basepath + "regenerate", null);
		DHTClient newClient = State.sharedState().getMaster().getDHTClient();
		assertArrayEquals(client.getNetworkId(), newClient.getNetworkId());
	}
}
