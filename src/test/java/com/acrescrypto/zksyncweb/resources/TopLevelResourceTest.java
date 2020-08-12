package com.acrescrypto.zksyncweb.resources;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.acrescrypto.zksyncweb.data.XArchiveSpecification;

public class TopLevelResourceTest {
	private HttpServer server;
	private WebTarget target;
	private ZKMaster master;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
		WebTestUtils.squelchGrizzlyLogs();
	}

	@Before
	public void beforeEach() throws IOException, URISyntaxException {
		State.setTestState();
		server = Main.startServer(TestUtils.testHttpPort());
		Client c = ClientBuilder.newClient();
		target = c.target(TestUtils.testHttpUrl());

		master = State.sharedState().getMaster();
		Util.setCurrentTimeMillis(0);
	}

	@After
	public void afterEach() {
		master.close();
		Util.setCurrentTimeMillis(-1);
		server.shutdownNow();
	}

	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}

	@Test
	public void testGetReturnsSuccess() {
		WebTestUtils.requestGet(target, "/");
	}
	
	@Test
	public void testDeletePurgesScratch() throws IOException {
		FS fs = master.scratchStorage();
		fs.write("foo", "bar".getBytes());
		WebTestUtils.requestDelete(target, "/");
		assertFalse(fs.exists("foo"));
	}
	
	@Test
	public void testDeletePurgesStorage() throws IOException {
		FS fs = master.getStorage();
		fs.write("foo", "bar".getBytes());
		WebTestUtils.requestDelete(target, "/");
		assertFalse(fs.exists("foo"));
	}
	
	@Test
	public void testDeleteClearsJoinedArchives() throws IOException {
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadPassphrase("test archive");
		
		WebTestUtils.requestPost(target, "archives", spec);
		assertTrue(Util.waitUntil(1000, ()->{
			try {
				return !State.sharedState().getOpenConfigs().isEmpty();
			} catch (IOException exc) {
				exc.printStackTrace();
				fail();
				return false;
			}
		}));

		WebTestUtils.requestDelete(target, "/");
		
		assertTrue(Util.waitUntil(1000, ()->{
			try {
				return State.sharedState().getOpenConfigs().isEmpty();
			} catch (IOException exc) {
				exc.printStackTrace();
				fail();
				return false;
			}
		}));
	}
}
