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

import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.StoredAccess;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.utility.Util;
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
		ZKFSTest.cheapenArgon2Costs();
		WebTestUtils.squelchGrizzlyLogs();
	}

	@Before
	public void beforeEach() throws Exception {
		State.setTestState();
		server = Main.startServer();
		Client c = ClientBuilder.newClient();
		target = c.target(Main.BASE_URI);

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
		ZKFSTest.restoreArgon2Costs();
	}

	@Test
	public void testGetReturnsListOfBranchTips() throws IOException {
		ArrayList<RevisionTag> tags = new ArrayList<>();
		for(int i = 0; i < 10; i++) {
			tags.add(archive.openBlank().commit());
		}

		JsonNode resp = WebTestUtils.requestGet(target, basePath);
		assertEquals(tags.size(), resp.get("branchTips").size());
		resp.get("branchTips").forEach((tip)->{
			assertTrue(tags.removeIf((tag)->{
				try {
					return Arrays.equals(tag.getBytes(), tip.binaryValue());
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
		info.setParents(new byte[10][]);
		for(int i = 0; i < info.getParents().length; i++) {
			info.getParents()[i] = archive.openBlank().commit().getBytes();
			tags.add(info.getParents()[i]);
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
		info.setParents(new byte[1][1]);
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
		assertTrue(resp.get("revisions").size() == 0);
	}
	
	@Test
	public void testGetAllReturnsListOfAllRevisions() throws IOException {
		ArrayList<RevisionTag> tags = new ArrayList<>();
		int bases = 3, depth = 3, expected = bases*depth+1;
		tags.add(RevisionTag.blank(archive.getConfig()));
		for(int i = 0; i < bases; i++) {
			ZKFS fs = archive.openBlank();
			for(int j = 0; j < depth; j++) {
				RevisionTag tag = fs.commit();
				tags.add(tag);
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
