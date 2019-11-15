package com.acrescrypto.zksyncweb.resources.archive.net;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

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
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.net.PeerSwarm;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.fasterxml.jackson.databind.JsonNode;

public class ArchiveNetResourceTest {
	class DummyBandwidthMonitor extends BandwidthMonitor {
		long bytesPerSecond;
		public DummyBandwidthMonitor(long bytesPerSecond) {
			this.bytesPerSecond = bytesPerSecond;
		}

		@Override public long getBytesPerSecond() { return bytesPerSecond; }
		@Override public long getLifetimeBytes() { return bytesPerSecond + 2; }
	}

	class DummySwarm extends PeerSwarm {
		public DummySwarm(ZKArchiveConfig config) throws IOException {
			this.config = config;
			this.setBandwidthMonitorRx(new DummyBandwidthMonitor(2000));
			this.setBandwidthMonitorTx(new DummyBandwidthMonitor(1000));
		}

		@Override public int numConnections() { return 4; }
		@Override public int numKnownAds() { return 3; }
		@Override public int numEmbargoedAds() { return 2; }
		@Override public int numConnectedAds() { return 1; }
	}

	private HttpServer server;
	private ZKArchive archive;
	private WebTarget target;
	private String passphrase;
	private String basepath;
	private DummySwarm swarm;

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
		passphrase = "passphrase";
		Util.setCurrentTimeMillis(0);

		archive = State.sharedState().getMaster().createDefaultArchive(passphrase.getBytes());
		swarm = new DummySwarm(archive.getConfig());
		archive.getConfig().setSwarm(swarm);
		State.sharedState().addOpenConfig(archive.getConfig());

		basepath = "/archives/" + WebTestUtils.transformArchiveId(archive) + "/net/";
	}

	@After
	public void afterEach() throws Exception {
		Util.setCurrentTimeMillis(-1);
		archive.close();
		server.shutdownNow();
		State.clearState();
	}

	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}

	@Test
	public void testGetNetShowsNumPeers() {
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		assertEquals(swarm.numConnections(), resp.get("numPeers").asInt());
	}

	@Test
	public void testGetNetShowsNumKnownAds() {
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		assertEquals(swarm.numKnownAds(), resp.get("numKnownAds").asInt());
	}

	@Test
	public void testGetNetShowsNumConnectedAds() {
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		assertEquals(swarm.numConnectedAds(), resp.get("numConnectedAds").asInt());
	}

	@Test
	public void testGetNetShowsNumEmbargoedAds() {
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		assertEquals(swarm.numEmbargoedAds(), resp.get("numEmbargoedAds").asInt());
	}

	@Test
	public void testGetNetShowsStaticPubKey() throws IOException {
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		assertArrayEquals(swarm.getPublicIdentityKey().getBytes(), resp.get("staticPubKey").binaryValue());
	}

	@Test
	public void testGetNetShowsBytesPerSecondRx() throws IOException {
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		assertEquals(swarm.getBandwidthMonitorRx().getBytesPerSecond(), resp.get("bytesPerSecondRx").doubleValue(), 0);
	}

	@Test
	public void testGetNetShowsBytesPerSecondTx() throws IOException {
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		assertEquals(swarm.getBandwidthMonitorTx().getBytesPerSecond(), resp.get("bytesPerSecondTx").doubleValue(), 0);
	}
	
	@Test
	public void testGetNetShowsLifetimeBytesRx() throws IOException {
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		assertEquals(swarm.getBandwidthMonitorRx().getLifetimeBytes(), resp.get("lifetimeBytesRx").doubleValue(), 0);
	}

	@Test
	public void testGetNetShowsLifetimeBytesTx() throws IOException {
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		assertEquals(swarm.getBandwidthMonitorTx().getLifetimeBytes(), resp.get("lifetimeBytesTx").doubleValue(), 0);
	}

	@Test
	public void testGetNetShowsMaxBytesPerSecondRxAsMinusOneWhenUnlimited() {
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		assertEquals(-1, resp.get("maxBytesPerSecondRx").doubleValue(), 0);
	}

	@Test
	public void testGetNetShowsMaxBytesPerSecondTxAsMinusOneWhenUnlimited() {
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		assertEquals(-1, resp.get("maxBytesPerSecondTx").doubleValue(), 0);
	}

	@Test
	public void testGetNetShowsMaxBytesPerSecondRxWhenLimited() {
		archive.getMaster().getBandwidthAllocatorRx().setBytesPerSecond(1234);
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		assertEquals(1234, resp.get("maxBytesPerSecondRx").doubleValue(), 0);
	}

	@Test
	public void testGetNetShowsMaxBytesPerSecondTxWhenLimited() {
		swarm.getConfig().getMaster().getBandwidthAllocatorTx().setBytesPerSecond(1234);
		JsonNode resp = WebTestUtils.requestGet(target, basepath);
		assertEquals(1234, resp.get("maxBytesPerSecondTx").doubleValue(), 0);
	}

	@Test
	public void testGetNetAdShowsHost() throws IOException {
		archive.getMaster().getGlobalConfig().set("net.swarm.enabled", true);
		archive.getConfig().advertise();
		JsonNode resp = WebTestUtils.requestGet(target, basepath + "ad");
		assertNotNull(resp.get("host").textValue());
	}

	@Test
	public void testGetNetAdShowsEncryptedArchiveId() throws IOException {
		archive.getMaster().getGlobalConfig().set("net.swarm.enabled", true);
		archive.getConfig().advertise();
		byte[] encArchiveId = archive.getConfig().getEncryptedArchiveId(swarm.getPublicIdentityKey().getBytes());
		JsonNode resp = WebTestUtils.requestGet(target, basepath + "ad");
		assertArrayEquals(encArchiveId, resp.get("encryptedArchiveId").binaryValue());
	}

	@Test
	public void testGetNetAdShowsListenPort() throws IOException {
		archive.getMaster().getGlobalConfig().set("net.swarm.enabled", true);
		archive.getConfig().advertise();
		JsonNode resp = WebTestUtils.requestGet(target, basepath + "ad");
		assertEquals(archive.getMaster().getTCPListener().getPort(), resp.get("port").intValue());
	}

	@Test
	public void testGetNetAdShowsVersion() throws IOException {
		archive.getMaster().getGlobalConfig().set("net.swarm.enabled", true);
		archive.getConfig().advertise();
		JsonNode resp = WebTestUtils.requestGet(target, basepath + "ad");
		assertEquals(0, resp.get("version").intValue());
	}

	@Test
	public void testGetNetAdReturns404WhenNoAdAvailable() throws IOException {
		WebTestUtils.requestGetWithError(target, 404, basepath + "ad");
	}
}
