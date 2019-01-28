package com.acrescrypto.zksyncweb.resources;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.glassfish.grizzly.http.server.HttpServer;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.fs.zkfs.ZKVersion;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.VersionInfo;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class VersionResourceTest {
	private ObjectMapper mapper = new ObjectMapper();
	private HttpServer server;
	private WebTarget target;


	@BeforeClass
	public static void beforeAll() {
		WebTestUtils.squelchGrizzlyLogs();
	}

	@Before
	public void beforeEach() throws Exception {
		server = Main.startServer();
		Client c = ClientBuilder.newClient();
		target = c.target(Main.BASE_URI);
	}

	@After
	public void afterEach() throws Exception {
		server.shutdownNow();
	}

	@Test
	public void testVersion() throws JsonParseException, JsonMappingException, IOException {
		String responseMsg = target.path("version").request().get(String.class);
		JsonNode json = mapper.readTree(responseMsg);

		assertEquals(200, json.get("status").asInt());
		assertFalse(json.hasNonNull("errmsg"));
		assertNotNull(json.get("resp"));

		JsonNode api = json.get("resp").get("versions").get("api");
		JsonNode fs = json.get("resp").get("versions").get("fs");

		assertEquals(VersionInfo.NAME, api.get("name").asText());
		assertEquals(VersionInfo.MAJOR, api.get("major").asInt());
		assertEquals(VersionInfo.MINOR, api.get("minor").asInt());
		assertEquals(VersionInfo.REVISION, api.get("revision").asInt());

		assertEquals(ZKVersion.NAME, fs.get("name").asText());
		assertEquals(ZKVersion.MAJOR, fs.get("major").asInt());
		assertEquals(ZKVersion.MINOR, fs.get("minor").asInt());
		assertEquals(ZKVersion.REVISION, fs.get("revision").asInt());
	}
}
