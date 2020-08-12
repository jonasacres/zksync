package com.acrescrypto.zksyncweb.resources.archive.revision;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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
import com.acrescrypto.zksyncweb.data.XRevisionInfo;
import com.fasterxml.jackson.databind.JsonNode;

public class ArchiveRevisionsResourceTest {
	private HttpServer server;
	private WebTarget target;
	private ZKArchive archive;
	private String basePath;

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

		basePath = "/archives/" + WebTestUtils.transformArchiveId(archive).substring(0, 8) + "/revisions";
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
	public void testGetReturnsListOfBranchTips() throws IOException {
		ArrayList<RevisionTag> tags = new ArrayList<>();
		for(int i = 0; i < 10; i++) {
			tags.add(archive.openBlank().commitAndClose());
		}

		JsonNode resp = WebTestUtils.requestGet(target, basePath);
		assertEquals(tags.size(), resp.get("branchTips").size());
		resp.get("branchTips").forEach((tip)->{
			assertTrue(tags.removeIf((tag)->{
				try {
					return Arrays.equals(tag.getBytes(), tip.get("revTag").binaryValue());
				} catch (IOException e) {
					return false;
				}
			}));
		});
	}

	@Test
	public void testPostCommitCreatesNewRevisionWithSpecifiedParentsIfWritableAndParentsSupplied() throws IOException {
		XRevisionInfo info = new XRevisionInfo();
		ArrayList<byte[]> tags = new ArrayList<>();
		info.setParents(new XRevisionInfo[10]);
		for(int i = 0; i < info.getParents().length; i++) {
			try(ZKFS fs = archive.openBlank()) {
				info.getParents()[i] = new XRevisionInfo(fs.commit(), 0);
				tags.add(info.getParents()[i].getRevTag());
			}
		}

		JsonNode resp = WebTestUtils.requestPost(target, basePath, info);
		assertNotNull(resp.get("revTag"));
		RevisionTag tag = State.sharedState().activeFs(archive.getConfig()).getBaseRevision();
		assertTrue(Arrays.equals(tag.getBytes(), resp.get("revTag").binaryValue()));
		assertEquals(info.getParents().length, tag.getInfo().getNumParents());

		for(RevisionTag parent : tag.getInfo().getParents()) {
			assertTrue(tags.removeIf((t)->Arrays.equals(t, parent.getBytes())));
		}
	}

	@Test
	public void testPostCommiFailsIfInvalidParentsSupplied() throws IOException {
		XRevisionInfo info = new XRevisionInfo();
		info.setParents(new XRevisionInfo[] { new XRevisionInfo() });
		info.getParents()[0].setRevTag(new byte[1]);
		WebTestUtils.requestPostWithError(target, 400, basePath, info);
	}

	@Test
	public void testPostCommitCreatesNewRevisionIfArchiveWritable() throws IOException {
		JsonNode resp = WebTestUtils.requestPost(target, basePath, new byte[0]);
		assertNotNull(resp.get("revTag"));
		RevisionTag tag = State.sharedState().activeFs(archive.getConfig()).getBaseRevision();
		assertTrue(Arrays.equals(tag.getBytes(), resp.get("revTag").binaryValue()));
		assertEquals(1, tag.getInfo().getNumParents());
	}

	@Test
	public void testPostCommitReturns400IfArchiveReadOnly() throws IOException {
		archive.getConfig().clearWriteRoot();
		WebTestUtils.requestPostWithError(target, 400, basePath, new byte[0]);
	}

	@Test
	public void testPostCommitReturns400IfArchiveSeedOnly() throws IOException {
		archive.getConfig().getAccessor().becomeSeedOnly();
		WebTestUtils.requestPostWithError(target, 400, basePath, new byte[0]);
	}
	
	@Test
	public void testGetAllReturnsToleratesEmptyRevisionList() {
		JsonNode resp = WebTestUtils.requestGet(target, basePath + "?mode=all");
		assertTrue(resp.has("revisions"));
		assertTrue(resp.get("revisions").isArray());
		assertEquals(1, resp.get("revisions").size()); // we list a blank revision if we have nothing else
	}
	
	@Test
	public void testGetAllReturnsListOfAllRevisions() throws IOException {
		ArrayList<RevisionTag> tags = new ArrayList<>();
		int bases = 3, depth = 3, expected = bases*depth+1;
		tags.add(RevisionTag.blank(archive.getConfig()));
		for(int i = 0; i < bases; i++) {
			try(ZKFS fs = archive.openBlank()) {
				for(int j = 0; j < depth; j++) {
					RevisionTag tag = fs.commit();
					tags.add(tag);
				}
			}
		}
		
		JsonNode resp = WebTestUtils.requestGet(target, basePath + "?mode=all");
		assertEquals(expected, resp.get("revisions").size());
		for(JsonNode tagNode : resp.get("revisions")) {
			byte[] tag = tagNode.get("revTag").binaryValue();
			RevisionTag found = null;
			for(RevisionTag requestedTag : tags) {
				if(Arrays.equals(requestedTag.getBytes(), tag)) {
					found = requestedTag;
					break;
				}
			}
			
			assertNotNull(found);
			tags.remove(found);
		}
	}
}
