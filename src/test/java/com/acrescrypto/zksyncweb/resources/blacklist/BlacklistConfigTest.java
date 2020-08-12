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
import com.acrescrypto.zksyncweb.data.XBlacklistConfig;
import com.fasterxml.jackson.databind.JsonNode;

public class BlacklistConfigTest {
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
		server = Main.startServer(TestUtils.testHttpPort());
		Client c = ClientBuilder.newClient();
		target = c.target(TestUtils.testHttpUrl());
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
	public void testGetReturnsShowsEnabledTrueWhenBlacklistEnabled() throws IOException {
		blacklist.setEnabled(true);
		JsonNode resp = WebTestUtils.requestGet(target, "/blacklist/config");
		assertTrue(resp.get("enabled").isBoolean());
		assertEquals(true, resp.get("enabled").booleanValue());
	}

	@Test
	public void testGetReturnsShowsEnabledFalseWhenBlacklistDisabled() throws IOException {
		blacklist.setEnabled(false);
		JsonNode resp = WebTestUtils.requestGet(target, "/blacklist/config");
		assertTrue(resp.get("enabled").isBoolean());
		assertEquals(false, resp.get("enabled").booleanValue());
	}

	@Test
	public void testPutLeavesEnabledStatusUnchangedIfFieldOmitted() throws IOException {
		XBlacklistConfig blacklistConfig = new XBlacklistConfig();

		WebTestUtils.requestPut(target, "/blacklist/config", blacklistConfig);
		assertTrue(blacklist.isEnabled());

		blacklist.setEnabled(false);
		WebTestUtils.requestPut(target, "/blacklist/config", blacklistConfig);
		assertFalse(blacklist.isEnabled());
	}

	@Test
	public void testSetEnabledFalseDisablesBlacklistChecks() throws IOException {
		XBlacklistConfig blacklistConfig = new XBlacklistConfig();
		blacklistConfig.setEnabled(false);
		blacklist.setEnabled(true);

		WebTestUtils.requestPut(target, "/blacklist/config", blacklistConfig);
		assertFalse(blacklist.isEnabled());
	}

	@Test
	public void testSetEnabledTrueEnablesBlacklistChecks() throws IOException {
		XBlacklistConfig blacklistConfig = new XBlacklistConfig();
		blacklistConfig.setEnabled(true);
		blacklist.setEnabled(false);

		WebTestUtils.requestPut(target, "/blacklist/config", blacklistConfig);
		assertTrue(blacklist.isEnabled());
	}
}
