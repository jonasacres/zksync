package com.acrescrypto.zksyncweb.resources.blacklist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;

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
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.fasterxml.jackson.databind.JsonNode;

public class BlacklistEntryResourceTest {
	private HttpServer server;
	private WebTarget target;
	private Blacklist blacklist;

	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}

	@Before
	public void beforeEach() throws IOException, URISyntaxException {
		State.setTestState();
		server = Main.startServer();
		Client c = ClientBuilder.newClient();
		target = c.target(Main.BASE_URI);
		blacklist = State.sharedState().getMaster().getBlacklist();
	}

	@After
	public void afterEach() {
		server.shutdownNow();
		State.clearState();
	}

	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}

	@Test
	public void testGetReturnsEntryIfIpIsBlacklisted() throws IOException {
		long expTime = System.currentTimeMillis() + 1234;
		blacklist.addWithAbsoluteTime("1.2.3.4", expTime);
		JsonNode resp = WebTestUtils.requestGet(target, "/blacklist/1.2.3.4");

		assertEquals("1.2.3.4", resp.get("address").textValue());
		assertEquals(expTime, resp.get("expiration").asLong());
	}

	@Test
	public void testGetReturns404IfIpIsNotBlacklisted() {
		WebTestUtils.requestGetWithError(target, 404, "/blacklist/1.2.3.4");
	}

	@Test
	public void testDeleteRemovesIpFromBlacklistIfPresent() throws IOException {
		blacklist.add("1.2.3.4", 1234);
		assertTrue(blacklist.contains("1.2.3.4"));
		WebTestUtils.requestDelete(target, "/blacklist/1.2.3.4");
		assertFalse(blacklist.contains("1.2.3.4"));
	}

	@Test
	public void testDeleteReturns404IfIpIsNotBlacklisted() {
		WebTestUtils.requestDeleteWithError(target, 404, "/blacklist/1.2.3.4");
	}
}
