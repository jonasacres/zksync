package com.acrescrypto.zksyncweb.resources.archive.revision;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.StoredAccess;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.fasterxml.jackson.databind.JsonNode;

public class ArchiveRevisionFsResourceTest {
	private HttpServer server;
	private WebTarget target;
	private ZKArchive archive;
	private String basePath;
	private RevisionTag revTag;

	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
		WebTestUtils.squelchGrizzlyLogs();
	}

	@Before
	public void beforeEach() throws Exception {
		State.setTestState();
		server = Main.startServer(TestUtils.testHttpPort());
		Client c = ClientBuilder.newClient();
		target = c.target(TestUtils.testHttpUrl());

		archive = State.sharedState().getMaster().createDefaultArchive("passphrase".getBytes());
		archive.getConfig().advertise();
		archive.getMaster().storedAccess().storeArchiveAccess(archive.getConfig(), StoredAccess.ACCESS_LEVEL_READWRITE);
		State.sharedState().addOpenConfig(archive.getConfig());

		try(ZKFS fs = archive.openBlank()) {
			fs.write("a", "i'm a teapot".getBytes());
			fs.write("b", "i exist".getBytes());
			fs.write("dir/d", "some bytes".getBytes());
			revTag = fs.commit();
		}
	
		try(ZKFS fs = archive.openBlank()) {
			fs.write("a", "i'm not a teapot".getBytes());
			fs.write("c", "i'm over here".getBytes());
			fs.commit();
			RevisionTag latest = fs.commit();
			assertEquals(latest, archive.getConfig().getRevisionList().latest());
		}

		basePath = "/archives/" + WebTestUtils.transformArchiveId(archive) + "/revisions/" + WebTestUtils.transformRevTag(revTag) + "/fs/";
	}

	@After
	public void afterEach() throws Exception {
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
	public void testGetFetchesDataFromRequestedRevisionTag() {
		byte[] response = WebTestUtils.requestBinaryGet(target, basePath + "a");
		assertArrayEquals("i'm a teapot".getBytes(), response);

		response = WebTestUtils.requestBinaryGet(target, basePath + "b");
		assertArrayEquals("i exist".getBytes(), response);

		WebTestUtils.requestGetWithError(target, 404, basePath + "c");
	}

	@Test
	public void testGetReadsFromRequestedOffset() {
		byte[] response = WebTestUtils.requestBinaryGet(target, basePath + "a?offset=6");
		assertArrayEquals("teapot".getBytes(), response);
	}

	@Test
	public void testGetReadsToRequestedLength() {
		byte[] response = WebTestUtils.requestBinaryGet(target, basePath + "a?length=3");
		assertArrayEquals("i'm".getBytes(), response);
	}

	@Test
	public void testGetReadsToRequestedLengthWithOffset() {
		byte[] response = WebTestUtils.requestBinaryGet(target, basePath + "a?length=3&offset=6");
		assertArrayEquals("tea".getBytes(), response);
	}

	@Test
	public void testGetReadsToEndOfFileIfLengthGoesPastEOF() {
		byte[] response = WebTestUtils.requestBinaryGet(target, basePath + "a?length=1024");
		assertArrayEquals("i'm a teapot".getBytes(), response);
	}

	@Test
	public void testGetReturnsFileStatIfRequested() throws IOException {
		JsonNode resp = WebTestUtils.requestGet(target, basePath + "a?stat=true");
		try(ZKFS fs = revTag.getFS()) {
			WebTestUtils.validatePathStat(fs, "/", resp);
		}
	}

	@Test
	public void testGetReturnsDirectoryListingIfPathIsDirectory() {
		JsonNode resp = WebTestUtils.requestGet(target, basePath);
		assertTrue(resp.get("entries").isArray());
		assertEquals(4, resp.get("entries").size());
		ArrayList<String> items = new ArrayList<>();
		resp.get("entries").forEach((node)->items.add(node.asText()));

		assertTrue(items.contains("/a"));
		assertTrue(items.contains("/b"));
		assertTrue(items.contains("/dir"));
	}

	@Test
	public void testGetReturnsRecursiveDirectoryListingIfPathIsDirectoryAndRecursionRequested() {
		JsonNode resp = WebTestUtils.requestGet(target, basePath + "?recursive=true");
		assertTrue(resp.get("entries").isArray());
		assertEquals(5, resp.get("entries").size());
		ArrayList<String> items = new ArrayList<>();
		resp.get("entries").forEach((node)->items.add(node.asText()));

		assertTrue(items.contains("/a"));
		assertTrue(items.contains("/b"));
		assertTrue(items.contains("/dir"));
		assertTrue(items.contains("/dir/d"));
	}
}
