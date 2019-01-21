package com.acrescrypto.zksyncweb.resources.log;

import static org.junit.Assert.assertEquals;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.utility.MemLogAppender;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.fasterxml.jackson.databind.JsonNode;

import ch.qos.logback.classic.Level;

public class LogResourceTest {
    private HttpServer server;
    private WebTarget target;
    private	Logger logger = LoggerFactory.getLogger(LogResourceTest.class);
    
    @BeforeClass
    public static void beforeAll() {
    	WebTestUtils.squelchGrizzlyLogs();
    	ZKFSTest.cheapenArgon2Costs();
    }
    
    @Before
	public void beforeEach() throws IOException, URISyntaxException {
    	State.setTestState();
        server = Main.startServer();
        Client c = ClientBuilder.newClient();
        target = c.target(Main.BASE_URI);
        MemLogAppender.sharedInstance().purge();
	}
	
	@After
	public void afterEach() {
		server.shutdownNow();
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Test
	public void testGetLogsReturnsMostRecentInfoEventsByDefault() {
		int expectedLength = 1000;
		for(int i = 0; i < expectedLength + 1; i++) {
			logger.info("msg " + i);
		}
		
		JsonNode resp = WebTestUtils.requestGet(target, "/logs");
		assertTrue(resp.get("entries").isArray());
		assertEquals(expectedLength, resp.get("entries").size());
	}
	
	@Test
	public void testGetLogsReturnsAllEventsIfDefaultThresholdExceedsListSize() {
		int expectedLength = 100;
		for(int i = 0; i < expectedLength; i++) {
			logger.info("msg " + i);
		}
		
		JsonNode resp = WebTestUtils.requestGet(target, "/logs");
		assertTrue(resp.get("entries").isArray());
		assertEquals(expectedLength, resp.get("entries").size());
	}
	
	@Test
	public void testGetLogsReturnsInfoAndHigherByDefault() {
		logger.debug("debug");
		logger.info("info");
		logger.warn("warn");
		
		JsonNode resp = WebTestUtils.requestGet(target, "/logs");
		assertTrue(resp.get("entries").isArray());
		assertEquals(2, resp.get("entries").size());
		assertEquals("info", resp.get("entries").get(0).get("msg").asText());
		assertEquals("warn", resp.get("entries").get(1).get("msg").asText());
	}
	
	@Test
	public void testGetLogsFiltersToRequestedThreshold() {
		logger.debug("debug");
		logger.info("info");
		logger.warn("warn");
		
		JsonNode resp = WebTestUtils.requestGet(target, "/logs?level=" + Level.ALL_INT);
		assertTrue(resp.get("entries").isArray());
		assertEquals(3, resp.get("entries").size());
		assertEquals("debug", resp.get("entries").get(0).get("msg").asText());
		assertEquals("info", resp.get("entries").get(1).get("msg").asText());
		assertEquals("warn", resp.get("entries").get(2).get("msg").asText());
	}
	
	@Test
	public void testGetLogsFiltersToRequestedOffset() {
		logger.info("info 0");
		logger.info("info 1");
		logger.info("info 2");
		
		JsonNode resp = WebTestUtils.requestGet(target, "/logs?offset=1");
		assertTrue(resp.get("entries").isArray());
		assertEquals(2, resp.get("entries").size());
		assertEquals("info 1", resp.get("entries").get(0).get("msg").asText());
		assertEquals("info 2", resp.get("entries").get(1).get("msg").asText());
	}

	@Test
	public void testGetLogsFiltersToRequestedNegativeOffset() {
		logger.info("info 0");
		logger.info("info 1");
		logger.info("info 2");
		
		JsonNode resp = WebTestUtils.requestGet(target, "/logs?offset=-2");
		assertTrue(resp.get("entries").isArray());
		assertEquals(2, resp.get("entries").size());
		assertEquals("info 0", resp.get("entries").get(0).get("msg").asText());
		assertEquals("info 1", resp.get("entries").get(1).get("msg").asText());
	}
	
	@Test
	public void testGetLogsFiltersToRequestedLength() {
		logger.info("info 0");
		logger.info("info 1");
		logger.info("info 2");
		
		JsonNode resp = WebTestUtils.requestGet(target, "/logs?length=1");
		assertTrue(resp.get("entries").isArray());
		assertEquals(1, resp.get("entries").size());
		assertEquals("info 2", resp.get("entries").get(0).get("msg").asText());
	}
}
