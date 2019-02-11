package com.acrescrypto.zksyncweb.resources.log;

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
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.utility.MemLogAppender;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.acrescrypto.zksyncweb.data.XLogInjection;
import com.fasterxml.jackson.databind.JsonNode;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;

/* This is ignored unless needed because these tests are a pain to maintain.
 * They're very sensitive to WHICH log events come back... but we can have all sorts of log
 * events happening asynchronously during our test. So these are only used when needed.
 */
public class LogResourceTest {
	private HttpServer server;
	private WebTarget target;
	private	Logger logger = LoggerFactory.getLogger(LogResourceTest.class);

	@BeforeClass
	public static void beforeAll() {
		WebTestUtils.squelchGrizzlyLogs();
		TestUtils.startDebugMode();
	}

	@Before
	public void beforeEach() throws IOException, URISyntaxException {
		State.setTestState();
		server = Main.startServer();
		Client c = ClientBuilder.newClient();
		target = c.target(Main.BASE_URI);
		MemLogAppender.sharedInstance().hardPurge();
	}

	@After
	public void afterEach() {
		server.shutdownNow();
	}

	@AfterClass
	public static void afterAll() {
		TestUtils.stopDebugMode();
	}
	
	public ArrayList<JsonNode> filterEntries(JsonNode entries) {
		assertTrue(entries.isArray());

		ArrayList<JsonNode> list = new ArrayList<>(entries.size());
		for(JsonNode entry : entries) {
			if(entry.get("logger").asText().endsWith("CustomLoggingFilter")) {
				continue;
			}
			
			list.add(entry);
		}
		return list;
	}

	@Test
	public void testGetLogsReturnsMostRecentInfoEventsByDefault() {
		int expectedLength = 1000;
		for(int i = 0; i < expectedLength + 1; i++) {
			logger.info("msg " + i);
		}

		JsonNode resp = WebTestUtils.requestGet(target, "/logs");
		ArrayList<JsonNode> filteredEntries = filterEntries(resp.get("entries"));
		assertEquals(expectedLength, filteredEntries.size());
	}

	@Test
	public void testGetLogsReturnsAllEventsIfDefaultThresholdExceedsListSize() {
		int expectedLength = 100;
		for(int i = 0; i < expectedLength; i++) {
			logger.info("msg " + i);
		}

		JsonNode resp = WebTestUtils.requestGet(target, "/logs");
		ArrayList<JsonNode> filteredEntries = filterEntries(resp.get("entries"));
		assertEquals(expectedLength, filteredEntries.size());
	}

	@Test
	public void testGetLogsReturnsInfoAndHigherByDefault() {
		logger.debug("debug");
		logger.info("info");
		logger.warn("warn");

		JsonNode resp = WebTestUtils.requestGet(target, "/logs");
		ArrayList<JsonNode> filteredEntries = filterEntries(resp.get("entries"));
		assertEquals(2, filteredEntries.size());
		assertEquals("info", filteredEntries.get(0).get("msg").asText());
		assertEquals("warn", filteredEntries.get(1).get("msg").asText());
	}

	@Test
	public void testGetLogsFiltersToRequestedThreshold() {
		logger.debug("debug");
		logger.info("info");
		logger.warn("warn");

		JsonNode resp = WebTestUtils.requestGet(target, "/logs?level=" + Level.ALL_INT);
		ArrayList<JsonNode> filteredEntries = filterEntries(resp.get("entries"));
		assertEquals(3, filteredEntries.size());
		assertEquals("debug", filteredEntries.get(0).get("msg").asText());
		assertEquals("info", filteredEntries.get(1).get("msg").asText());
		assertEquals("warn", filteredEntries.get(2).get("msg").asText());
	}
	
	@Test
	public void testGetLogsFiltersToRequestedBeforeId() {
		logger.debug("debug");
		logger.info("info");
		logger.warn("warn");

		JsonNode resp = WebTestUtils.requestGet(target, "/logs?before=1&level=" + Level.ALL_INT);
		ArrayList<JsonNode> filteredEntries = filterEntries(resp.get("entries"));
		assertEquals(1, filteredEntries.size());
		assertEquals("debug", filteredEntries.get(0).get("msg").asText());
	}

	@Test
	public void testGetLogsFiltersToRequestedAfterId() {
		logger.debug("debug");
		logger.info("info");
		logger.warn("warn");

		JsonNode resp = WebTestUtils.requestGet(target, "/logs?after=1&level=" + Level.ALL_INT);
		ArrayList<JsonNode> filteredEntries = filterEntries(resp.get("entries"));
		assertEquals(1, filteredEntries.size());
		assertEquals("warn", filteredEntries.get(0).get("msg").asText());
	}

	@Test
	public void testGetLogsFiltersToRequestedOffset() {
		logger.info("info 0");
		logger.info("info 1");
		logger.info("info 2");

		JsonNode resp = WebTestUtils.requestGet(target, "/logs?offset=1");
		ArrayList<JsonNode> filteredEntries = filterEntries(resp.get("entries"));
		assertEquals(2, filteredEntries.size());
		assertEquals("info 1", filteredEntries.get(0).get("msg").asText());
		assertEquals("info 2", filteredEntries.get(1).get("msg").asText());
	}

	@Test
	public void testGetLogsFiltersToRequestedNegativeOffset() {
		logger.info("info 0");
		logger.info("info 1");
		logger.info("info 2");

		JsonNode resp = WebTestUtils.requestGet(target, "/logs?offset=-2");
		ArrayList<JsonNode> filteredEntries = filterEntries(resp.get("entries"));
		assertEquals(2, filteredEntries.size());
		assertEquals("info 0", filteredEntries.get(0).get("msg").asText());
		assertEquals("info 1", filteredEntries.get(1).get("msg").asText());
	}

	@Test
	public void testGetLogsFiltersToRequestedLength() {
		logger.info("info 0");
		logger.info("info 1");
		logger.info("info 2");
		
		JsonNode resp = WebTestUtils.requestGet(target, "/logs?length=1");
		ArrayList<JsonNode> filteredEntries = filterEntries(resp.get("entries"));
		assertEquals(1, filteredEntries.size());
		assertEquals("info 2", filteredEntries.get(0).get("msg").asText());
	}
	
	@Test
	public void testGetLogsReturnsLaunchTime() {
		JsonNode resp = WebTestUtils.requestGet(target, "/logs");
		assertEquals(Util.launchTime(), resp.get("launchTime").longValue());
	}
	
	@Test
	public void testGetLogsAppliesFiltersIfLaunchTimeMatches() {
		for(int i = 0; i < 10; i++) {
			logger.info("info " + i);
		}
		
		JsonNode resp = WebTestUtils.requestGet(target,
				  "/logs" 
				+ "?launchTime=" + Util.launchTime()
				+ "&offset=1"
				+ "&after=1"
				+ "&before=9");
		ArrayList<JsonNode> filteredEntries = filterEntries(resp.get("entries"));
		assertEquals(7, filteredEntries.size());
	}
	
	@Test
	public void testGetLogsDoesNotApplyFiltersIfLaunchTimeDoesNotMatch() {
		for(int i = 0; i < 10; i++) {
			logger.info("info " + i);
		}
		
		JsonNode resp = WebTestUtils.requestGet(target,
				  "/logs" 
				+ "?launchTime=" + (Util.launchTime()-1)
				+ "&offset=1"
				+ "&after=1"
				+ "&before=9");
		ArrayList<JsonNode> filteredEntries = filterEntries(resp.get("entries"));
		assertEquals(10, filteredEntries.size());
	}
	
	@Test
	public void testPostLogsCreatesLogEntry() {
		XLogInjection injection = new XLogInjection();
		injection.setText("hello world!");
		WebTestUtils.requestPost(target, "/logs", injection);
		
		ILoggingEvent entry = MemLogAppender.sharedInstance().getEntries(1).get(0).getEntry();
		assertEquals(injection.getText(), entry.getMessage());
		assertEquals("INFO", entry.getLevel().levelStr);
	}
	
	@Test
	public void testPostLogsCreatesLogEntryWithTraceSeverity() {
		XLogInjection injection = new XLogInjection();
		injection.setText("hello world!");
		injection.setSeverity("TRACE");
		WebTestUtils.requestPost(target, "/logs", injection);
		
		ILoggingEvent entry = MemLogAppender.sharedInstance().getEntries(1).get(0).getEntry();
		assertEquals(injection.getText(), entry.getMessage());
		assertEquals(injection.getSeverity().toUpperCase(), entry.getLevel().levelStr);
	}
	
	@Test
	public void testPostLogsCreatesLogEntryWithDebugSeverity() {
		XLogInjection injection = new XLogInjection();
		injection.setText("hello world!");
		injection.setSeverity("Debug");
		WebTestUtils.requestPost(target, "/logs", injection);
		
		ILoggingEvent entry = MemLogAppender.sharedInstance().getEntries(1).get(0).getEntry();
		assertEquals(injection.getText(), entry.getMessage());
		assertEquals(injection.getSeverity().toUpperCase(), entry.getLevel().levelStr);
	}
	
	@Test
	public void testPostLogsCreatesLogEntryWithInfoSeverity() {
		XLogInjection injection = new XLogInjection();
		injection.setText("hello world!");
		injection.setSeverity("info");
		WebTestUtils.requestPost(target, "/logs", injection);
		
		ILoggingEvent entry = MemLogAppender.sharedInstance().getEntries(1).get(0).getEntry();
		assertEquals(injection.getText(), entry.getMessage());
		assertEquals(injection.getSeverity().toUpperCase(), entry.getLevel().levelStr);
	}
	
	@Test
	public void testPostLogsCreatesLogEntryWithWarnSeverity() {
		XLogInjection injection = new XLogInjection();
		injection.setText("hello world!");
		injection.setSeverity("wARN");
		WebTestUtils.requestPost(target, "/logs", injection);
		
		ILoggingEvent entry = MemLogAppender.sharedInstance().getEntries(1).get(0).getEntry();
		assertEquals(injection.getText(), entry.getMessage());
		assertEquals(injection.getSeverity().toUpperCase(), entry.getLevel().levelStr);
	}
	
	@Test @Ignore
	public void testPostLogsCreatesLogEntryWithErrorSeverity() {
		XLogInjection injection = new XLogInjection();
		injection.setText("test of error message injection");
		injection.setSeverity("eRrOr");
		WebTestUtils.requestPost(target, "/logs", injection);
		
		ILoggingEvent entry = MemLogAppender.sharedInstance().getEntries(1).get(0).getEntry();
		assertEquals(injection.getText(), entry.getMessage());
		assertEquals(injection.getSeverity().toUpperCase(), entry.getLevel().levelStr);
	}
}
