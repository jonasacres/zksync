package com.acrescrypto.zksyncweb.resources.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.StoredAccess;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.PeerSwarm;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.acrescrypto.zksyncweb.data.XArchiveSpecification;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;

public class ArchivesResourceTest {
	private HttpServer server;
	private WebTarget target;

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
	}

	@After
	public void afterEach() throws Exception {
		server.shutdownNow();
		State.clearState();
	}

	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
	}

	ZKArchiveConfig validateSingleArchiveListing(JsonNode resp, XArchiveSpecification spec) throws IOException {
		ZKArchiveConfig storedConfig = State.sharedState().getOpenConfigs().iterator().next();
		long expectedStorageSize = storedConfig.getStorage().storageSize("/");
		long expectedLocalStorageSize = storedConfig.getLocalStorage().storageSize("/");

		boolean speccedWriteKey = spec.getWriteKey() != null || spec.getWritePassphrase() != null;
		boolean expectReadKey = spec.getReadKey() != null || spec.getReadPassphrase() != null;
		boolean expectReady = spec.getArchiveId() == null;
		boolean expectUsesWriteKey = speccedWriteKey || !expectReady;

		if(spec.getPageSize() == null || spec.getPageSize() <= 0) {
			assertEquals(ZKArchive.DEFAULT_PAGE_SIZE, storedConfig.getPageSize());
			assertEquals(ZKArchive.DEFAULT_PAGE_SIZE, resp.get("pageSize").asInt());
		} else {
			assertEquals(spec.getPageSize().intValue(), storedConfig.getPageSize());
			assertEquals(spec.getPageSize().intValue(), resp.get("pageSize").asInt());
		}

		if(expectReady) {
			if(spec.getDescription() == null) {
				assertEquals("", storedConfig.getDescription());
				assertEquals("", resp.get("description").asText());
			} else {
				assertEquals(spec.getDescription(), storedConfig.getDescription());
				assertEquals(spec.getDescription(), resp.get("description").asText());
			}
		}

		assertEquals(speccedWriteKey || !expectUsesWriteKey, resp.get("haveWriteKey").asBoolean());
		assertEquals(expectUsesWriteKey, resp.get("usesWriteKey").asBoolean());
		assertEquals(expectUsesWriteKey, storedConfig.usesWriteKey());
		assertEquals(expectReadKey, resp.get("haveReadKey").asBoolean());
		assertEquals(expectReadKey, !storedConfig.getAccessor().isSeedOnly());
		assertEquals(expectReady, resp.get("ready").asBoolean());
		assertEquals(expectedStorageSize, resp.get("consumedStorage").longValue());
		assertEquals(expectedLocalStorageSize, resp.get("consumedLocalStorage").longValue());
		if(storedConfig.haveConfigLocally()) {
			assertArrayEquals(RevisionTag.blank(storedConfig).getBytes(), resp.get("currentRevTag").binaryValue());
		}
		assertArrayEquals(storedConfig.getArchiveId(), resp.get("archiveId").binaryValue());

		JsonNode xconfig = resp.get("config");
		assertEquals(false, xconfig.get("advertising").asBoolean());
		assertEquals(false, xconfig.get("requestingAll").asBoolean());
		assertEquals(false, xconfig.get("autocommit").asBoolean());
		assertEquals(false, xconfig.get("autofollow").asBoolean());
		assertEquals(PeerSwarm.DEFAULT_MAX_SOCKET_COUNT, xconfig.get("peerLimit").intValue());
		assertEquals(0, xconfig.get("autocommitInterval").asInt());

		return storedConfig;
	}

	@Test
	public void testCreateArchiveCreatesDefaultArchives() throws JsonParseException, JsonMappingException, IOException {
		String pp = "let's test this thing";
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadPassphrase(pp);
		JsonNode resp = WebTestUtils.requestPost(target, "archives", spec);

		ZKArchiveConfig storedConfig = validateSingleArchiveListing(resp, spec);
		ZKArchiveConfig expectedConfig = ZKMaster.openBlankTestVolume().createDefaultArchive(pp.getBytes()).getConfig();

		assertArrayEquals(expectedConfig.getArchiveId(), storedConfig.getArchiveId());
		validateSingleArchiveListing(resp, spec);
	}
	
	@Test
	public void testCreatedArchivesPersistBetweenRuns() throws JsonParseException, JsonMappingException, IOException {
		String pp = "let's test this thing";
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadPassphrase(pp);
		JsonNode postResp = WebTestUtils.requestPost(target, "archives", spec);
		
		State.resetState();
		JsonNode getResp = WebTestUtils.requestGet(target, "archives");
		assertTrue(getResp.get("archives").get(0).get("archiveId").textValue().equals(postResp.get("archiveId").textValue()));
	}

	@Test
	public void testCreateNonDefaultArchiveWithoutWriteKey() throws IOException {
		String pp = "let's test this thing";
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadPassphrase(pp);
		spec.setPageSize(2*ZKArchive.DEFAULT_PAGE_SIZE);
		spec.setDescription("I'm not like the other archives");
		JsonNode resp = WebTestUtils.requestPost(target, "archives", spec);

		validateSingleArchiveListing(resp, spec);
	}

	@Test
	public void testCreateNonDefaultArchiveWithWritePassphrase() throws IOException {
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadPassphrase("read passphrase");
		spec.setWritePassphrase("write passphrase");
		spec.setPageSize(2*ZKArchive.DEFAULT_PAGE_SIZE);
		spec.setDescription("Testing");
		JsonNode resp = WebTestUtils.requestPost(target, "archives", spec);

		validateSingleArchiveListing(resp, spec);
	}

	@Test
	public void testCreateDefaultArchiveWithRawReadKey() throws IOException {
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadKey(State.sharedCrypto().makeSymmetricKey());
		JsonNode resp = WebTestUtils.requestPost(target, "archives", spec);

		ZKArchiveConfig config = validateSingleArchiveListing(resp, spec);

		Key readKey = new Key(State.sharedCrypto(), spec.getReadKey());
		Key derived = readKey.derive("foo");
		assertEquals(derived, config.getAccessor().deriveKey(ArchiveAccessor.KEY_ROOT_PASSPHRASE, "foo"));
	}

	@Test
	public void testCreateNonDefaultArchiveWithRawReadKey() throws IOException {
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadKey(State.sharedCrypto().makeSymmetricKey());
		spec.setPageSize(2*ZKArchive.DEFAULT_PAGE_SIZE);
		spec.setDescription("non-default");
		JsonNode resp = WebTestUtils.requestPost(target, "archives", spec);

		ZKArchiveConfig config = validateSingleArchiveListing(resp, spec);

		Key readKey = new Key(State.sharedCrypto(), spec.getReadKey());
		Key derived = readKey.derive("foo");
		assertEquals(derived, config.getAccessor().deriveKey(ArchiveAccessor.KEY_ROOT_PASSPHRASE, "foo"));
	}

	@Test
	public void testCreateNonDefaultArchiveWithRawWriteKey() throws IOException {
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadKey(State.sharedCrypto().makeSymmetricKey());
		spec.setWriteKey(State.sharedCrypto().makeSymmetricKey());
		spec.setPageSize(2*ZKArchive.DEFAULT_PAGE_SIZE);
		spec.setDescription("non-default");
		JsonNode resp = WebTestUtils.requestPost(target, "archives", spec);

		ZKArchiveConfig config = validateSingleArchiveListing(resp, spec);

		Key writeKey = new Key(State.sharedCrypto(), spec.getWriteKey());
		Key derived = writeKey.derive("foo");
		assertEquals(derived, config.deriveKey(ArchiveAccessor.KEY_ROOT_WRITE, "foo"));
	}

	@Test
	public void testJoinWriteControlledArchiveWithReadWriteKeys() throws IOException {
		ZKMaster master = ZKMaster.openBlankTestVolume();
		Key readRoot = new Key(master.getCrypto()), writeRoot = new Key(master.getCrypto());
		ZKArchive archive = master.createArchiveWithWriteRoot(ZKArchive.DEFAULT_PAGE_SIZE, "", readRoot, writeRoot);

		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadKey(readRoot.getRaw());
		spec.setWriteKey(writeRoot.getRaw());
		spec.setArchiveId(archive.getConfig().getArchiveId());
		spec.setPageSize(archive.getConfig().getPageSize());
		spec.setDescription(archive.getConfig().getDescription());
		JsonNode resp = WebTestUtils.requestPost(target, "archives", spec);

		ZKArchiveConfig config = validateSingleArchiveListing(resp, spec);
		assertArrayEquals(archive.getConfig().getArchiveId(), config.getArchiveId());
	}

	@Test
	public void testJoinWriteControlledArchiveWithReadKeyOnly() throws IOException {
		ZKMaster master = ZKMaster.openBlankTestVolume();
		Key readRoot = new Key(master.getCrypto()), writeRoot = new Key(master.getCrypto());
		ZKArchive archive = master.createArchiveWithWriteRoot(ZKArchive.DEFAULT_PAGE_SIZE, "", readRoot, writeRoot);

		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadKey(readRoot.getRaw());
		spec.setArchiveId(archive.getConfig().getArchiveId());
		spec.setPageSize(archive.getConfig().getPageSize());
		spec.setDescription(archive.getConfig().getDescription());
		JsonNode resp = WebTestUtils.requestPost(target, "archives", spec);

		ZKArchiveConfig config = validateSingleArchiveListing(resp, spec);
		assertArrayEquals(archive.getConfig().getArchiveId(), config.getArchiveId());
	}

	@Test
	public void testJoinWriteControlledArchiveWithSeedKeyOnly() throws IOException {
		ZKMaster master = ZKMaster.openBlankTestVolume();
		Key readRoot = new Key(master.getCrypto()), writeRoot = new Key(master.getCrypto());
		ZKArchive archive = master.createArchiveWithWriteRoot(ZKArchive.DEFAULT_PAGE_SIZE, "", readRoot, writeRoot);

		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setSeedKey(archive.getConfig().getAccessor().getSeedRoot().getRaw());
		spec.setArchiveId(archive.getConfig().getArchiveId());
		spec.setPageSize(archive.getConfig().getPageSize());
		spec.setDescription(archive.getConfig().getDescription());
		JsonNode resp = WebTestUtils.requestPost(target, "archives", spec);

		ZKArchiveConfig config = validateSingleArchiveListing(resp, spec);
		assertArrayEquals(archive.getConfig().getArchiveId(), config.getArchiveId());
	}

	@Test
	public void testJoinNonWriteControlledArchiveWithReadKeyOnly() throws IOException {
		String pp = "passphrase";
		ZKMaster master = ZKMaster.openBlankTestVolume();
		ZKArchive archive = master.createArchiveWithPassphrase(2*ZKArchive.DEFAULT_PAGE_SIZE, "description", pp.getBytes());

		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadPassphrase(pp);
		spec.setArchiveId(archive.getConfig().getArchiveId());
		spec.setPageSize(archive.getConfig().getPageSize());
		spec.setDescription(archive.getConfig().getDescription());
		JsonNode resp = WebTestUtils.requestPost(target, "archives", spec);

		ZKArchiveConfig config = validateSingleArchiveListing(resp, spec);
		assertArrayEquals(archive.getConfig().getArchiveId(), config.getArchiveId());
	}

	@Test
	public void testJoinNonWriteControlledArchiveWithSeedKeyOnly() throws IOException {
		String pp = "passphrase";
		ZKMaster master = ZKMaster.openBlankTestVolume();
		ZKArchive archive = master.createArchiveWithPassphrase(2*ZKArchive.DEFAULT_PAGE_SIZE, "description", pp.getBytes());

		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setSeedKey(archive.getConfig().getAccessor().getSeedRoot().getRaw());
		spec.setArchiveId(archive.getConfig().getArchiveId());
		spec.setPageSize(archive.getConfig().getPageSize());
		spec.setDescription(archive.getConfig().getDescription());
		JsonNode resp = WebTestUtils.requestPost(target, "archives", spec);

		ZKArchiveConfig config = validateSingleArchiveListing(resp, spec);
		assertArrayEquals(archive.getConfig().getArchiveId(), config.getArchiveId());
	}

	@Test
	public void testDuplicatePostWithIdenticalArchiveIdDoesNotOpenAdditionalArchives() throws IOException {
		String pp = "passphrase";
		ZKMaster master = ZKMaster.openBlankTestVolume();
		ZKArchive archive = master.createArchiveWithPassphrase(2*ZKArchive.DEFAULT_PAGE_SIZE, "description", pp.getBytes());

		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadPassphrase(pp);
		spec.setArchiveId(archive.getConfig().getArchiveId());
		spec.setPageSize(archive.getConfig().getPageSize());
		spec.setDescription(archive.getConfig().getDescription());
		WebTestUtils.requestPost(target, "archives", spec);
		ZKArchiveConfig original = State.sharedState().getOpenConfigs().iterator().next();
		WebTestUtils.requestPost(target, "archives", spec);

		assertEquals(1, State.sharedState().getOpenConfigs().size());
		assertTrue(original == State.sharedState().getOpenConfigs().iterator().next());
	}

	@Test
	public void testCreateSavesKeysIfAccessLevelSpecified() throws IOException {
		String pp = "passphrase";

		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadPassphrase(pp);
		spec.setSavedAccessLevel(StoredAccess.ACCESS_LEVEL_READWRITE);
		WebTestUtils.requestPost(target, "archives", spec);

		State state2 = new State(State.defaultPassphrase(), State.sharedState().getMaster().getStorage());
		assertEquals(1, state2.getOpenConfigs().size());
	}

	@Test
	public void testCreateDoesNotSaveKeysIfAccessLevelIsNone() throws IOException {
		String pp = "passphrase";

		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadPassphrase(pp);
		spec.setSavedAccessLevel(StoredAccess.ACCESS_LEVEL_NONE);
		WebTestUtils.requestPost(target, "archives", spec);

		State state2 = new State(State.defaultPassphrase(), State.sharedState().getMaster().getStorage());
		assertEquals(0, state2.getOpenConfigs().size());
	}

	@Test
	public void testCreateDoesSavesKeysIfAccessLevelIsNull() throws IOException {
		String pp = "passphrase";

		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadPassphrase(pp);
		spec.setSavedAccessLevel(null);
		WebTestUtils.requestPost(target, "archives", spec);

		State state2 = new State(State.defaultPassphrase(), State.sharedState().getMaster().getStorage());
		assertEquals(1, state2.getOpenConfigs().size());
	}

	@Test
	public void testDuplicatePostDefaultWithIdenticalPassphraseDoesNotOpenAdditionalArchives() throws IOException {
		String pp = "passphrase";

		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadPassphrase(pp);

		WebTestUtils.requestPost(target, "archives", spec);
		ZKArchiveConfig original = State.sharedState().getOpenConfigs().iterator().next();
		WebTestUtils.requestPost(target, "archives", spec);

		assertEquals(1, State.sharedState().getOpenConfigs().size());
		assertTrue(original == State.sharedState().getOpenConfigs().iterator().next());
	}

	@Test
	public void testListArchivesEmpty() throws IOException {
		JsonNode resp = WebTestUtils.requestGet(target, "archives");
		assertTrue(resp.hasNonNull("archives"));
		assertTrue(resp.get("archives").isArray());
		assertEquals(0, resp.get("archives").size());
	}

	@Test
	public void testListArchivesMultiple() throws IOException {
		int numArchives = 8;
		JsonNode[] ids = new JsonNode[numArchives];

		for(int i = 0; i < numArchives; i++) {
			XArchiveSpecification spec = new XArchiveSpecification();
			spec.setReadPassphrase("test archive " + i);
			JsonNode resp = WebTestUtils.requestPost(target, "archives", spec);
			ids[i] = resp;
		}

		JsonNode listing = WebTestUtils.requestGet(target, "/archives");
		assertTrue(listing.get("archives").isArray());
		assertEquals(numArchives, listing.get("archives").size());
		for(int i = 0; i < numArchives; i++) {
			JsonNode entry = listing.get("archives").get(i);
			boolean found = false;

			for(JsonNode id : ids) {
				if(!Arrays.equals(id.get("archiveId").binaryValue(), entry.get("archiveId").binaryValue())) {
					continue;
				}

				found = true;
				id.fieldNames().forEachRemaining((field)-> {
					assertEquals(id.get(field), entry.get(field));
				});
			}

			assertTrue(found);
		}
	}
	
	@Test
	public void testListArchivesShowsArchiveSettings() throws IOException {
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadPassphrase("test archive");
		WebTestUtils.requestPost(target, "archives", spec);
		JsonNode listing = WebTestUtils.requestGet(target, "/archives");
		
		assertTrue(listing.get("archives").isArray());
		JsonNode config = listing.get("archives").get(0).get("config");
		assertFalse(config.get("autocommit").isNull());
		assertFalse(config.get("automirror").isNull());
		assertFalse(config.get("autofollow").isNull());
	}

}
