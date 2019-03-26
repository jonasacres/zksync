package com.acrescrypto.zksyncweb.resources.blacklist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;

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
import com.acrescrypto.zksync.net.BlacklistEntry;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.acrescrypto.zksyncweb.data.XBlacklistEntry;
import com.fasterxml.jackson.databind.JsonNode;

public class BlacklistResourceTest {
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
	public void testGetWithEmptyBlacklistReturnsEmptyList() {
		JsonNode resp = WebTestUtils.requestGet(target, "/blacklist");
		assertTrue(resp.get("entries").isArray());
		assertEquals(0, resp.get("entries").size());
	}

	@Test
	public void testGetWithReturnsBlacklist() throws IOException {
		blacklist.add("1.2.3.4", 1234);
		blacklist.add("4.3.2.1", 4321);
		blacklist.add("127.0.0.1", 80085);
		ArrayList<BlacklistEntry> entries = blacklist.allEntries();

		JsonNode resp = WebTestUtils.requestGet(target, "/blacklist");

		assertTrue(resp.get("entries").isArray());
		assertEquals(3, resp.get("entries").size());

		resp.get("entries").forEach((entry)->{
			assertTrue(entries.removeIf((e)->{
				if(!e.getAddress().equals(entry.get("address").textValue())) return false;
				if(e.getExpiration() != entry.get("expiration").longValue()) return false;
				return true;
			}));
		});
	}

	@Test
	public void testPostCreatesBlacklistEntry() throws IOException {
		XBlacklistEntry entry = new XBlacklistEntry("127.0.0.1", System.currentTimeMillis() + 1234);
		WebTestUtils.requestPost(target, "/blacklist", entry);

		assertEquals(1, blacklist.allEntries().size());
		BlacklistEntry newEntry = blacklist.allEntries().get(0);
		assertEquals(entry.getAddress(), newEntry.getAddress());
		assertEquals(entry.getExpiration(), newEntry.getExpiration());
	}

	@Test
	public void testPostReturns400IfExpirationInPast() throws IOException {
		XBlacklistEntry entry = new XBlacklistEntry("127.0.0.1", System.currentTimeMillis() - 1);
		WebTestUtils.requestPostWithError(target, 400, "/blacklist", entry);

		assertEquals(0, blacklist.allEntries().size());
	}

	@Test
	public void testDeleteClearsBlacklist() throws IOException {
		blacklist.add("1.2.3.4", 1234);
		blacklist.add("4.3.2.1", 4321);
		blacklist.add("127.0.0.1", 80085);

		WebTestUtils.requestDelete(target, "/blacklist");
		assertEquals(0, blacklist.allEntries().size());
	}
}
