package com.acrescrypto.zksyncweb.resources;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;

public class GenericResourceTest {
	private HttpServer server;
	private WebTarget target;


	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
		WebTestUtils.squelchGrizzlyLogs();
	}

	@Before
	public void beforeEach() throws Exception {
		server = Main.startServer(TestUtils.testHttpPort());
		Client c = ClientBuilder.newClient();
		target = c.target(TestUtils.testHttpUrl());
	}

	@After
	public void afterEach() throws Exception {
		server.shutdownNow();
		State.clearState();
	}

	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Test
	public void testInvalidJsonTriggers400() {
		Entity<Object> entity = Entity.entity("{I AM NOT REALLY JSON}", MediaType.APPLICATION_JSON);
		try {
			target
				.path("/archives")
				.request()
				.post(entity, String.class);
			fail();
		} catch(WebApplicationException exc) {
			assertEquals(400, exc.getResponse().getStatus());
		}
	}
	
	@Test
	public void testInvalidMethodTriggers405() {
		Entity<Object> entity = Entity.entity("{I AM NOT REALLY JSON}", MediaType.APPLICATION_JSON);
		try {
			target
				.path("/archives")
				.request()
				.put(entity, String.class);
			fail();
		} catch(WebApplicationException exc) {
			assertEquals(405, exc.getResponse().getStatus());
		}
	}
	
	@Test
	public void testInvalidPathTriggers404() {
		Response resp = target
			.path("/doesntexist")
			.request()
			.get();
		
		assertEquals(404, resp.getStatus());
	}
}
