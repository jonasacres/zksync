package com.acrescrypto.zksyncweb.resources;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.fasterxml.jackson.databind.JsonNode;

public class GlobalResourceTest {
	private HttpServer server;
	private WebTarget target;
	private ZKMaster master;

	/* These tests were designed when there was a rigid set of config parameters, each explicitly
	 * considered in the GlobalResource handler. This is no longer the case, and these tests don't
	 * really belong here anymore.
	 * 
	 * That said, it is nice to have tests that exercise the basic network management controls
	 * done here. They remain here pending something better.
	 */

	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
		WebTestUtils.squelchGrizzlyLogs();
	}

	@Before
	public void beforeEach() throws IOException, URISyntaxException {
		State.setTestState();
		server = Main.startServer();
		Client c = ClientBuilder.newClient();
		target = c.target(Main.BASE_URI);

		master = State.sharedState().getMaster();
		Util.setCurrentTimeMillis(0);
		WebTestUtils.rigMonitors(new BandwidthMonitor[] { master.getBandwidthMonitorRx(), master.getBandwidthMonitorTx() }, 1000);
	}

	@After
	public void afterEach() {
		master.close();
		Util.setCurrentTimeMillis(-1);
		server.shutdownNow();
	}

	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
	}

	@Test
	public void testGetGlobalReturnsBandwidthInfo() {
		JsonNode resp = WebTestUtils.requestGet(target, "/global");
		assertEquals(1000,
				resp.get("bytesPerSecondTx").longValue());
		assertEquals(1000,
				resp.get("bytesPerSecondRx").longValue());
		assertEquals(master.getBandwidthMonitorTx().getBytesPerSecond(),
				resp.get("bytesPerSecondTx").longValue());
		assertEquals(master.getBandwidthMonitorRx().getBytesPerSecond(),
				resp.get("bytesPerSecondRx").longValue());
		assertEquals(master.getBandwidthMonitorTx().getLifetimeBytes(),
				resp.get("lifetimeBytesTx").longValue());
		assertEquals(master.getBandwidthMonitorRx().getLifetimeBytes(),
				resp.get("lifetimeBytesRx").longValue());
	}

	@Test
	public void testGetGlobalReturnsNumberOfArchivesIfZero() {
		JsonNode resp = WebTestUtils.requestGet(target, "/global");
		assertEquals(0,
				resp.get("numArchives").intValue());
	}

	@Test
	public void testGetGlobalReturnsNumberOfArchivesIfNonzero() throws IOException {
		master.createDefaultArchive("meow".getBytes());
		master.createDefaultArchive("bark".getBytes());
		JsonNode resp = WebTestUtils.requestGet(target, "/global");
		assertEquals(2,
				resp.get("numArchives").intValue());
	}

	@Test
	public void testGetGlobalReturnsIsListeningTrueIfTcpListenerActive() throws IOException {
		master.getGlobalConfig().set("net.swarm.enabled", true);
		assertTrue(Util.waitUntil(100, ()->master.getTCPListener().isListening()));
		JsonNode resp = WebTestUtils.requestGet(target, "/global");
		assertTrue(resp.get("isListening").booleanValue());
	}

	@Test
	public void testGetGlobalReturnsIsListeningFalseIfTcpListenerInactive() throws IOException {
		JsonNode resp = WebTestUtils.requestGet(target, "/global");
		assertFalse(resp.get("isListening").booleanValue());
	}

	@Test
	public void testGetGlobalReturnsSettingsWhenNonEmpty() {
		master.getGlobalConfig().set("net.limits.tx", 4321);
		master.getGlobalConfig().set("net.limits.rx", 1234);

		JsonNode resp = WebTestUtils.requestGet(target, "/global");
		JsonNode settings = resp.get("settings");

		assertEquals(4321,
				settings.get("net.limits.tx").longValue());
		assertEquals(1234,
				settings.get("net.limits.rx").longValue());
	}

	@Test
	public void testGetGlobalSettingsReturnsSettingsWhenNonDefault() {
		master.getGlobalConfig().set("net.limits.tx", 4321);
		master.getGlobalConfig().set("net.limits.rx", 1234);

		JsonNode resp = WebTestUtils.requestGet(target, "/global/settings");

		assertEquals(4321,
				resp.get("net.limits.tx").longValue());
		assertEquals(1234,
				resp.get("net.limits.rx").longValue());
	}

	@Test
	public void testGetGlobalSettingsReturnsAllSettings() {
		JsonNode resp = WebTestUtils.requestGet(target, "/global/settings");
		for(String key : master.getGlobalConfig().keys()) {
			assertTrue("missing " + key, resp.has(key));
		}
	}

	@Test
	public void testGetGlobalSettingsReturnsTcpPortWhenListenerActive() {
		master.getGlobalConfig().set("net.swarm.enabled", true);
		assertTrue(Util.waitUntil(100, ()->master.getTCPListener().getPort() != 0));
		JsonNode resp = WebTestUtils.requestGet(target, "/global/settings");
		assertEquals(master.getTCPListener().getPort(), resp.get("net.swarm.lastport").intValue());
	}

	@Test
	public void testPutSettingsWithOmittedRxLimitDoesNotChangeRxLimit() {
		HashMap<String,Object> settings = new HashMap<>();
		master.getBandwidthAllocatorRx().setBytesPerSecond(1234);
		WebTestUtils.requestPut(target, "/global/settings", settings);
		assertEquals(1234, master.getBandwidthAllocatorRx().getBytesPerSecond(), 0);
	}

	@Test
	public void testPutSettingsWithNullTxLimitDoesNotChangeTxLimit() {
		HashMap<String,Object> settings = new HashMap<>();
		master.getBandwidthAllocatorTx().setBytesPerSecond(1234);
		WebTestUtils.requestPut(target, "/global/settings", settings);
		assertEquals(1234, master.getBandwidthAllocatorTx().getBytesPerSecond(), 0);
	}

	@Test
	public void testPutSettingsWithNegativeRxLimitSetsUnlimitedRx() {
		HashMap<String,Object> settings = new HashMap<>();
		settings.put("net.limits.rx", -1);
		master.getBandwidthAllocatorRx().setBytesPerSecond(1234);

		assertFalse(master.getBandwidthAllocatorRx().isUnlimited());
		WebTestUtils.requestPut(target, "/global/settings", settings);
		assertTrue(master.getBandwidthAllocatorRx().isUnlimited());
	}

	@Test
	public void testPutSettingsWithNegativeTxLimitSetsUnlimitedTx() {
		HashMap<String,Object> settings = new HashMap<>();
		settings.put("net.limits.tx", -1);
		master.getBandwidthAllocatorTx().setBytesPerSecond(1234);

		assertFalse(master.getBandwidthAllocatorTx().isUnlimited());
		WebTestUtils.requestPut(target, "/global/settings", settings);
		assertTrue(master.getBandwidthAllocatorTx().isUnlimited());
	}

	@Test
	public void testPutSettingsWithZeroRxLimitSetsZeroRx() {
		HashMap<String,Object> settings = new HashMap<>();
		settings.put("net.limits.rx", 0);
		master.getBandwidthAllocatorRx().setBytesPerSecond(1234);

		WebTestUtils.requestPut(target, "/global/settings", settings);
		assertEquals(0, master.getBandwidthAllocatorRx().getBytesPerSecond());
	}

	@Test
	public void testPutSettingsWithZeroTxLimitSetsZeroTx() {
		HashMap<String,Object> settings = new HashMap<>();
		settings.put("net.limits.tx", 0);
		master.getBandwidthAllocatorTx().setBytesPerSecond(1234);

		WebTestUtils.requestPut(target, "/global/settings", settings);
		assertEquals(0, master.getBandwidthAllocatorTx().getBytesPerSecond());
	}

	@Test
	public void testPutSettingsWithPositiveRxLimitSetsRxLimit() {
		HashMap<String,Object> settings = new HashMap<>();
		settings.put("net.limits.rx", 4321);

		WebTestUtils.requestPut(target, "/global/settings", settings);
		assertEquals(4321, master.getBandwidthAllocatorRx().getBytesPerSecond());
	}

	@Test
	public void testPutSettingsWithPositiveTxLimitSetsTxLimit() {
		HashMap<String,Object> settings = new HashMap<>();
		settings.put("net.limits.tx", 4321);

		WebTestUtils.requestPut(target, "/global/settings", settings);
		assertEquals(4321, master.getBandwidthAllocatorTx().getBytesPerSecond());
	}

	@Test
	public void testPutSettingsWithNullPortDoesNotStartListener() {
		HashMap<String,Object> settings = new HashMap<>();
		WebTestUtils.requestPut(target, "/global/settings", settings);
		Util.sleep(100);
		assertFalse(master.getTCPListener().isListening());
	}

	@Test
	public void testPutSettingsWithNullPortDoesNotInterfereWithActiveListener() {
		master.getGlobalConfig().set("net.swarm.enabled", true);
		assertTrue(Util.waitUntil(100, ()->master.getTCPListener().getPort() > 0));
		int port = master.getTCPListener().getPort();

		HashMap<String,Object> settings = new HashMap<>();
		WebTestUtils.requestPut(target, "/global/settings", settings);

		Util.sleep(100);
		assertTrue(master.getTCPListener().isListening());
		assertEquals(port, master.getTCPListener().getPort());
	}

	@Test
	public void testPutSettingsWithZeroPortStartsListenerOnRandomPortIfNoPortCached() {
		HashMap<String,Object> settings = new HashMap<>();
		settings.put("net.swarm.port", 0);
		settings.put("net.swarm.enabled", true);
		WebTestUtils.requestPut(target, "/global/settings", settings);
		assertTrue(Util.waitUntil(100, ()->master.getTCPListener().getPort() > 0));
		assertTrue(Util.waitUntil(100, ()->master.getTCPListener().isListening()));
	}

	@Test
	public void testPutSettingsWithPositivePortListensOnRequestedPortIfListenerClosed() {
		HashMap<String,Object> settings = new HashMap<>();
		settings.put("net.swarm.port", 41312);
		settings.put("net.swarm.enabled", true);
		WebTestUtils.requestPut(target, "/global/settings", settings);
		assertTrue(Util.waitUntil(100, ()->master.getTCPListener().isListening()));
		assertEquals(41312, master.getTCPListener().getPort());
	}

	@Test
	public void testPutSettingsWithPositivePortReopensOnRequestedPortIfListenerOpen() throws UnknownHostException, IOException {
		master.getGlobalConfig().set("net.swarm.enabled", true);
		assertTrue(Util.waitUntil(100, ()->master.getTCPListener().getPort() > 0));
		int port = master.getTCPListener().getPort();

		HashMap<String,Object> settings = new HashMap<>();
		settings.put("net.swarm.port", port+1);
		settings.put("net.swarm.enabled", true);
		WebTestUtils.requestPut(target, "/global/settings", settings);

		assertTrue(Util.waitUntil(100, ()->master.getTCPListener().getPort() == port + 1));
	}
	
	@Test
	public void testPutSettingsWithStringFieldSetsConfigValue() {
		// observed issue: setting a config field with a string name caused an exception
		HashMap<String,Object> settings = new HashMap<>();
		settings.put("net.dht.bootstrap.host", "localhost");
		WebTestUtils.requestPut(target, "/global/settings", settings);
		assertTrue(Util.waitUntil(100, ()->master.getGlobalConfig().getString("net.dht.bootstrap.host").equals("localhost")));
	}

	@Test
	public void testGetUptimeReturnsUptime() {
		JsonNode resp = WebTestUtils.requestGet(target, "/global/uptime");
		long expectedUptime = System.currentTimeMillis() - Util.launchTime();
		assertEquals(resp.get("uptime").longValue(), expectedUptime, 10);
	}
}
