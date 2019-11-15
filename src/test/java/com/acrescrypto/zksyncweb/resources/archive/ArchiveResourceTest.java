package com.acrescrypto.zksyncweb.resources.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Base64;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.StoredAccess;
import com.acrescrypto.zksync.fs.zkfs.StoredAccessRecord;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.net.PeerSwarm;
import com.acrescrypto.zksync.net.dht.DHTID;
import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.acrescrypto.zksyncweb.data.XArchiveSettings;
import com.acrescrypto.zksyncweb.data.XArchiveSpecification;
import com.fasterxml.jackson.databind.JsonNode;

public class ArchiveResourceTest {
	public final static String TESTDIR = "/tmp/zksync-test/archiveresourcetest";

	private HttpServer server;
	private ZKArchive archive;
	private WebTarget target;
	private String passphrase;
	private RevisionTag tag;

	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
		WebTestUtils.squelchGrizzlyLogs();
	}

	@Before
	public void beforeEach() throws Exception {
		State.setTestState();
		server = Main.startServer();
		Client c = ClientBuilder.newClient();
		target = c.target(Main.BASE_URI);
		passphrase = "passphrase";
		Util.setCurrentTimeMillis(0);

		archive = State.sharedState().getMaster().createDefaultArchive(passphrase.getBytes());
		WebTestUtils.rigMonitors(new BandwidthMonitor[] { archive.getConfig().getSwarm().getBandwidthMonitorRx(), archive.getConfig().getSwarm().getBandwidthMonitorTx() }, 1000);
		archive.getConfig().advertise();
		archive.getMaster().storedAccess().storeArchiveAccess(archive.getConfig(), StoredAccess.ACCESS_LEVEL_READWRITE);
		try(ZKFS fs = archive.openBlank()) {
			fs.write("foo", "bar".getBytes());
			tag = fs.commit();
			State.sharedState().addOpenConfig(archive.getConfig());
			
			try(LocalFS lfs = new LocalFS("/")) {
				lfs.mkdirp(TESTDIR);
			}
		}
	}

	@After
	public void afterEach() throws Exception {
		try(LocalFS lfs = new LocalFS("/")) {
			lfs.rmrf(TESTDIR);
		}
		archive.close();
		archive.getMaster().close();
		server.shutdownNow();
		State.clearState();
	}

	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
		Util.setCurrentTimeMillis(-1);
	}

	public String encodeArchiveId(ZKArchive archive) {
		String base64 = Base64.getEncoder().encodeToString(archive.getConfig().getArchiveId());
		try {
			return URLEncoder.encode(base64, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			fail();
			return null;
		}
	}

	public String transformArchiveId(ZKArchive archive) {
		String base64 = Base64.getEncoder().encodeToString(archive.getConfig().getArchiveId());
		return Util.toWebSafeBase64(base64);
	}

	public void validateDelete(ZKArchive archive) throws IOException {
		assertNull(State.sharedState().configForArchiveId(archive.getConfig().getArchiveId()));
		assertEquals(0, archive.getConfig().getLocalStorage().storageSize("/", false));
		assertEquals(0, archive.getConfig().getCacheStorage().storageSize("/", false));
		assertTrue(archive.getConfig().isClosed());
		assertTrue(archive.getConfig().getSwarm().isClosed());
		assertTrue(archive.isClosed());
		assertFalse(archive.getMaster().getDHTDiscovery().isAdvertising(archive.getConfig().getAccessor()));
		assertFalse(archive.getMaster().getDHTDiscovery().isDiscovering(archive.getConfig().getAccessor()));
	}

	void validateArchiveListing(JsonNode resp, ZKArchive archive) throws IOException {
		long expectedStorageSize = archive.getConfig().getStorage().storageSize("/", false);
		long expectedLocalStorageSize = archive.getConfig().getLocalStorage().storageSize("/", false);
		double expectedBandwidthRx = archive.getConfig().getSwarm().getBandwidthMonitorRx().getBytesPerSecond();
		double expectedBandwidthTx = archive.getConfig().getSwarm().getBandwidthMonitorTx().getBytesPerSecond();
		long expectedLifetimeTx = archive.getConfig().getSwarm().getBandwidthMonitorTx().getLifetimeBytes();
		long expectedLifetimeRx = archive.getConfig().getSwarm().getBandwidthMonitorRx().getLifetimeBytes();

		assertEquals(ZKArchive.DEFAULT_PAGE_SIZE, resp.get("pageSize").asInt());
		assertEquals("", resp.get("description").asText());

		assertEquals(true, resp.get("haveWriteKey").asBoolean());
		assertEquals(false, resp.get("usesWriteKey").asBoolean());
		assertEquals(true, resp.get("haveReadKey").asBoolean());
		assertEquals(true, resp.get("ready").asBoolean());
		assertEquals(expectedStorageSize, resp.get("consumedStorage").longValue());
		assertEquals(expectedLocalStorageSize, resp.get("consumedLocalStorage").longValue());
		assertEquals(expectedBandwidthRx, resp.get("bytesPerSecondRx").doubleValue(), 1.0);
		assertEquals(expectedBandwidthTx, resp.get("bytesPerSecondTx").doubleValue(), 1.0);
		assertEquals(expectedLifetimeRx, resp.get("lifetimeBytesRx").longValue());
		assertEquals(expectedLifetimeTx, resp.get("lifetimeBytesTx").longValue());
		assertArrayEquals(tag.getBytes(), resp.get("currentRevTag").binaryValue());
		assertArrayEquals(archive.getConfig().getArchiveId(), resp.get("archiveId").binaryValue());

		JsonNode xconfig = resp.get("config");
		assertEquals(true, xconfig.get("advertising").asBoolean());
		assertEquals(false, xconfig.get("requestingAll").asBoolean());
		assertEquals(false, xconfig.get("autocommit").asBoolean());
		assertEquals(false, xconfig.get("autofollow").asBoolean());
		assertEquals(PeerSwarm.DEFAULT_MAX_SOCKET_COUNT, xconfig.get("peerLimit").intValue());
		assertEquals(0, xconfig.get("autocommitInterval").asInt());
	}

	void assertReadable(ZKArchive archive) throws IOException {
		assertFalse(archive.getConfig().getAccessor().isSeedOnly());
		try(ZKFS fs = tag.getFS()) {
			assertArrayEquals("bar".getBytes(), fs.read("foo"));
		}
	}

	void assertWritable(ZKArchive archive) throws IOException {
		assertFalse(archive.getConfig().isReadOnly());
		try(ZKFS fs = archive.openBlank()) {
			fs.commit();
		}
	}

	@Test
	public void testGetArchiveWithEscapedArchiveIdReturnsResult() throws IOException {
		JsonNode result = WebTestUtils.requestGet(target, "archives/" + encodeArchiveId(archive));
		validateArchiveListing(result, archive);
	}

	@Test
	public void testGetArchiveWithTransformedArchiveIdReturnsResult() throws IOException {
		JsonNode result = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive));
		validateArchiveListing(result, archive);
	}

	@Test
	public void testGetArchiveWithPartialEscapedArchiveIdReturnsResult() throws IOException {
		JsonNode result = WebTestUtils.requestGet(target, "archives/" + encodeArchiveId(archive).substring(0, 4));
		validateArchiveListing(result, archive);
	}

	@Test
	public void testGetArchiveWithPartialTransformedArchiveIdReturnsResult() throws IOException {
		JsonNode result = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive).substring(0, 4));
		validateArchiveListing(result, archive);
	}

	@Test
	public void testGetArchiveWithNonexistentArchiveIdReturns404() {
		WebTestUtils.requestGetWithError(target, 404, "archives/jATjHfyINnLr4PTpVCMF4bpPdomQbqpD2iyquAeI5tOAsDqyzPRhPPfKvTQobJj9I9wneei1YWXQb2zMOtoS_g==");
	}

	@Test
	public void testGetArchiveWithMalformedArchiveIdReturns404() {
		WebTestUtils.requestGetWithError(target, 404, "archives/this:isnt:base|64~");
	}


	@Test
	public void testGetArchiveShowsCurrentTitle() throws IOException {
		String title = "here's a new title";
		State.sharedState().activeFs(archive.getConfig()).getInodeTable().setNextTitle(title);
		JsonNode result = WebTestUtils.requestGet(target, "archives/" + encodeArchiveId(archive).substring(0, 4));
		validateArchiveListing(result, archive);
		assertEquals(title, result.get("currentTitle").asText());
	}

	@Test
	public void testGetArchiveSeedReturnsSeedSpecification() throws IOException {
		JsonNode result = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/seed");
		assertArrayEquals(archive.getConfig().getAccessor().getSeedRoot().getRaw(),
				result.get("seedKey").binaryValue());
		assertArrayEquals(archive.getConfig().getArchiveId(),
				result.get("archiveId").binaryValue());
	}

	@Test
	public void testDeleteArchiveWithEscapedArchiveId() throws IOException {
		WebTestUtils.requestDelete(target, "archives/" + encodeArchiveId(archive));
		validateDelete(archive);
	}

	@Test
	public void testDeleteArchiveWithTransformedArchiveId() throws IOException {
		WebTestUtils.requestDelete(target, "archives/" + transformArchiveId(archive));
		validateDelete(archive);
	}

	@Test
	public void testDeleteArchiveWithPartialEscapedArchiveId() throws IOException {
		WebTestUtils.requestDelete(target, "archives/" + encodeArchiveId(archive).substring(0, 8));
		validateDelete(archive);
	}

	@Test
	public void testDeleteArchiveWithPartialTransformedArchiveId() throws IOException {
		WebTestUtils.requestDelete(target, "archives/" + transformArchiveId(archive).substring(0, 8));
		validateDelete(archive);
	}

	@Test
	public void testDeleteArchiveWithNonexistentArchiveIdReturns404() {
		WebTestUtils.requestDeleteWithError(target, 404, "archives/jATjHfyINnLr4PTpVCMF4bpPdomQbqpD2iyquAeI5tOAsDqyzPRhPPfKvTQobJj9I9wneei1YWXQb2zMOtoS_g==");
	}

	@Test
	public void testDeleteArchiveWithMalformedArchiveIdReturns404() {
		WebTestUtils.requestDeleteWithError(target, 404, "archives/this:isnt:base|64~");
	}

	@Test
	public void testDeleteWriteKeyRemovesWriteKey() throws IOException {
		WebTestUtils.requestDelete(target, "archives/" + transformArchiveId(archive).substring(0, 8) + "/keys/write");
		StoredAccessRecord record = State.sharedState().getMaster().storedAccess().recordForArchiveId(archive.getConfig().getArchiveId());
		assertEquals(StoredAccess.ACCESS_LEVEL_READ, record.getAccessLevel());
		assertTrue(archive.getConfig().isReadOnly());
		assertFalse(archive.getConfig().getAccessor().isSeedOnly());

		try(State state2 = new State(State.defaultPassphrase(), State.sharedState().getMaster().getStorage())) {
			StoredAccessRecord record2 = state2.getMaster().storedAccess().recordForArchiveId(archive.getConfig().getArchiveId());
			assertNotNull(record2);
			assertEquals(StoredAccess.ACCESS_LEVEL_READ, record2.getAccessLevel());
			ZKArchiveConfig config2 = state2.getMaster().allConfigs().iterator().next();
			assertTrue(config2.isReadOnly());
			assertFalse(config2.getAccessor().isSeedOnly());
		}
	}

	@Test
	public void testDeleteReadKeyRemovesReadKey() throws IOException {
		WebTestUtils.requestDelete(target, "archives/" + transformArchiveId(archive).substring(0, 8) + "/keys/read");
		StoredAccessRecord record = State.sharedState().getMaster().storedAccess().recordForArchiveId(archive.getConfig().getArchiveId());
		assertEquals(StoredAccess.ACCESS_LEVEL_SEED, record.getAccessLevel());
		assertTrue(archive.getConfig().isReadOnly());
		assertTrue(archive.getConfig().getAccessor().isSeedOnly());

		try(State state2 = new State(State.defaultPassphrase(), State.sharedState().getMaster().getStorage())) {
			StoredAccessRecord record2 = state2.getMaster().storedAccess().recordForArchiveId(archive.getConfig().getArchiveId());
			assertNotNull(record2);
			assertEquals(StoredAccess.ACCESS_LEVEL_SEED, record2.getAccessLevel());
			ZKArchiveConfig config2 = state2.getMaster().allConfigs().iterator().next();
			assertTrue(config2.getAccessor().isSeedOnly());
			assertTrue(config2.isReadOnly());
		}
	}

	@Test
	public void testDeleteKeysRemovesAllAccess() throws IOException {
		long expectedSize = archive.getConfig().getCacheStorage().storageSize("/", false) + archive.getConfig().getLocalStorage().storageSize("/", false);
		WebTestUtils.requestDelete(target, "archives/" + transformArchiveId(archive).substring(0, 8) + "/keys");
		StoredAccessRecord record = State.sharedState().getMaster().storedAccess().recordForArchiveId(archive.getConfig().getArchiveId());
		assertNull(record);

		try(State state2 = new State(State.defaultPassphrase(), State.sharedState().getMaster().getStorage())) {
			StoredAccessRecord record2 = state2.getMaster().storedAccess().recordForArchiveId(archive.getConfig().getArchiveId());
			assertNull(record2);
	
			// make sure we kept the cached pages and local data
			long size = archive.getConfig().getCacheStorage().storageSize("/", false) + archive.getConfig().getLocalStorage().storageSize("/", false);
			assertEquals(expectedSize, size);
		}
	}

	@Test
	public void testDeleteDoesntRemoveOtherArchivesWithSeparateAccessors() throws IOException {
		ZKArchive archive2 = archive.getMaster().createDefaultArchive("another archive".getBytes());
		archive2.getMaster().storedAccess().storeArchiveAccess(archive2.getConfig(), StoredAccess.ACCESS_LEVEL_READWRITE);
		State.sharedState().addOpenConfig(archive2.getConfig());

		long expectedSize = archive2.getConfig().getCacheStorage().storageSize("/", false) + archive2.getConfig().getLocalStorage().storageSize("/", false);
		WebTestUtils.requestDelete(target, "archives/" + transformArchiveId(archive).substring(0, 8));

		// make sure we still have the stored accessor
		try(State state2 = new State(State.defaultPassphrase(), archive.getMaster().getStorage())) {
			StoredAccessRecord record2 = state2.getMaster().storedAccess().recordForArchiveId(archive2.getConfig().getArchiveId());
			assertNotNull(record2);
			
			
			// make sure we kept the cached pages and local data
			long size = archive2.getConfig().getCacheStorage().storageSize("/", false) + archive2.getConfig().getLocalStorage().storageSize("/", false);
			assertEquals(expectedSize, size);
	
			ZKArchiveConfig loadedConfig = state2.getMaster().allConfigs().iterator().next();
			assertEquals(1, state2.getMaster().allConfigs().size());
			assertArrayEquals(archive2.getConfig().getArchiveId(), loadedConfig.getArchiveId());
	
			WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive2));
		}
	}

	@Test
	public void testDeleteDoesntRemoveOtherArchivesWithIdenticalAccessors() throws IOException {
		ZKArchiveConfig config2 = ZKArchiveConfig.create(archive.getConfig().getAccessor(), "custom", 2*archive.getConfig().getPageSize());
		config2.getMaster().storedAccess().storeArchiveAccess(config2, StoredAccess.ACCESS_LEVEL_READWRITE);
		State.sharedState().addOpenConfig(config2);

		long expectedSize = config2.getCacheStorage().storageSize("/", false) + config2.getLocalStorage().storageSize("/", false);
		WebTestUtils.requestDelete(target, "archives/" + transformArchiveId(archive).substring(0, 8));

		// make sure we still have the stored accessor
		try(State state2 = new State(State.defaultPassphrase(), archive.getMaster().getStorage())) {
			StoredAccessRecord record2 = state2.getMaster().storedAccess().recordForArchiveId(config2.getArchiveId());
			assertNotNull(record2);
	
			// make sure we kept the cached pages and local data
			long size = config2.getCacheStorage().storageSize("/", false) + config2.getLocalStorage().storageSize("/", false);
			assertEquals(expectedSize, size);
	
			ZKArchiveConfig loadedConfig = state2.getMaster().allConfigs().iterator().next();
			assertEquals(1, state2.getMaster().allConfigs().size());
			assertArrayEquals(config2.getArchiveId(), loadedConfig.getArchiveId());
	
			WebTestUtils.requestGet(target, "archives/" + transformArchiveId(config2.getArchive()));
		}
	}

	@Test
	public void testDeleteDoesNotRemoveAcccessorFromDHTDiscoveryIfDeletedArchiveIsNotLastAccessorInstance() throws IOException {
		ZKArchiveConfig config2 = ZKArchiveConfig.create(archive.getConfig().getAccessor(), "custom", 2*archive.getConfig().getPageSize());
		config2.getMaster().storedAccess().storeArchiveAccess(config2, StoredAccess.ACCESS_LEVEL_READWRITE);
		config2.advertise();
		State.sharedState().addOpenConfig(config2);

		WebTestUtils.requestDelete(target, "archives/" + transformArchiveId(archive).substring(0, 8));
		assertTrue(State.sharedState().getMaster().getDHTDiscovery().isAdvertising(config2.getAccessor()));
	}

	@Test
	public void testDeleteRemovesAcccessorFromDHTDiscoveryIfDeletedArchiveIsLastAccessorInstance() throws IOException {
		ZKArchiveConfig config2 = ZKArchiveConfig.create(archive.getConfig().getAccessor(), "custom", 2*archive.getConfig().getPageSize());
		config2.getMaster().storedAccess().storeArchiveAccess(config2, StoredAccess.ACCESS_LEVEL_READWRITE);
		config2.advertise();
		State.sharedState().addOpenConfig(config2);

		WebTestUtils.requestDelete(target, "archives/" + transformArchiveId(archive).substring(0, 8));
		WebTestUtils.requestDelete(target, "archives/" + transformArchiveId(config2.getArchive()).substring(0, 8));
		assertFalse(State.sharedState().getMaster().getDHTDiscovery().isAdvertising(config2.getAccessor()));
	}

	@Test
	public void testPostCommitTriggersCommit() throws IOException {
		byte[] oldTag = State.sharedState().activeFs(archive.getConfig()).getBaseRevision().serialize();
		JsonNode resp = WebTestUtils.requestPost(target, "archives/" + transformArchiveId(archive) + "/commit", new byte[0]);

		byte[] revTagBytes = resp.get("revTag").binaryValue();
		assertFalse(Arrays.equals(oldTag, revTagBytes));
		assertArrayEquals(State.sharedState().activeFs(archive.getConfig()).getBaseRevision().serialize(),
				revTagBytes);
	}

	@Test
	public void testPostTitleSetsNextCommitTitle() throws IOException {
		String title = "i'm a title!";
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/title", title.getBytes());
		assertEquals(State.sharedState().activeFs(archive.getConfig()).getInodeTable().getNextTitle(), title);
	}

	@Test
	public void testGetTitleReturnsNextCommitTitle() throws IOException {
		String title = "there's a secret trap in this test 👹";
		State.sharedState().activeFs(archive.getConfig()).getInodeTable().setNextTitle(title);
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/title");
		assertEquals(State.sharedState().activeFs(archive.getConfig()).getInodeTable().getNextTitle(),
				resp.get("title").textValue());
	}

	@Test
	public void testGetSettingsAdvertisingFieldFalseIfNotAdvertising() {
		archive.getConfig().stopAdvertising();
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertFalse(resp.get("advertising").asBoolean());
	}

	@Test
	public void testGetSettingsAdvertisingFieldTrueIfAdvertising() {
		archive.getConfig().advertise();
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertTrue(resp.get("advertising").asBoolean());
	}

	@Test
	public void testGetSettingsRequestingAllFieldFalseIfNotRequestingAllFromSwarm() {
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertFalse(resp.get("requestingAll").asBoolean());
	}

	@Test
	public void testGetSettingsRequestingAllFieldTrueIfRequestingAllFromSwarm() {
		archive.getConfig().getSwarm().requestAll();
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertTrue(resp.get("requestingAll").asBoolean());
	}

	@Test
	public void testGetSettingsPeerLimitFieldReflectsMaxSocketCountFromSwarm() {
		archive.getConfig().getSwarm().setMaxSocketCount(1234);
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertEquals(1234, resp.get("peerLimit").asInt());

		archive.getConfig().getSwarm().setMaxSocketCount(4321);
		resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertEquals(4321, resp.get("peerLimit").asInt());
	}

	@Test
	public void testGetSettingsAutocommitIntervalReflectsManagerSettingForNonseedArchives() throws IOException {
		State.sharedState().activeManager(archive.getConfig()).setAutocommitIntervalMs(1234);
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertEquals(1234, resp.get("autocommitInterval").asInt());

		State.sharedState().activeManager(archive.getConfig()).setAutocommitIntervalMs(4321);
		resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertEquals(4321, resp.get("autocommitInterval").asInt());
	}

	@Test
	public void testGetSettingsAutocommitIntervalIsZeroForSeedArchives() throws IOException {
		archive.getConfig().getAccessor().becomeSeedOnly();
		State.sharedState().activeManager(archive.getConfig()).setAutocommitIntervalMs(4321);
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertEquals(0, resp.get("autocommitInterval").asInt());
	}

	@Test
	public void testGetSettingsAutocommitIsTrueIfSetByManager() throws IOException {
		State.sharedState().activeManager(archive.getConfig()).setAutocommit(true);
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertTrue(resp.get("autocommit").asBoolean());
	}

	@Test
	public void testGetSettingsAutocommitIsFalseIfSetByManager() throws IOException {
		State.sharedState().activeManager(archive.getConfig()).setAutocommit(false);
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertFalse(resp.get("autocommit").asBoolean());
	}

	@Test
	public void testGetSettingsAutocommitIsFalseIfNonseedArchive() throws IOException {
		State.sharedState().activeManager(archive.getConfig()).setAutocommit(true);
		archive.getConfig().getAccessor().becomeSeedOnly();
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertFalse(resp.get("autocommit").asBoolean());
	}
	
	@Test
	public void testGetSettingsAutomergeIsTrueIfSetByManager() throws IOException {
		State.sharedState().activeManager(archive.getConfig()).setAutomerge(true);
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertTrue(resp.get("automerge").asBoolean());
	}

	@Test
	public void testGetSettingsAutomergeIsFalseIfSetByManager() throws IOException {
		State.sharedState().activeManager(archive.getConfig()).setAutomerge(false);
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertFalse(resp.get("automerge").asBoolean());
	}

	@Test
	public void testGetSettingsAutofollowIsTrueIfSetByManager() throws IOException {
		State.sharedState().activeManager(archive.getConfig()).setAutofollow(true);
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertTrue(resp.get("autofollow").asBoolean());
	}

	@Test
	public void testGetSettingsAutofollowIsFalseIfSetByManager() throws IOException {
		State.sharedState().activeManager(archive.getConfig()).setAutofollow(false);
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertFalse(resp.get("autofollow").asBoolean());
	}

	@Test
	public void testGetSettingsgAutofollowIsFalseIfNonseedArchive() throws IOException {
		State.sharedState().activeManager(archive.getConfig()).setAutofollow(true);
		archive.getConfig().getAccessor().becomeSeedOnly();
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertFalse(resp.get("autofollow").asBoolean());
	}

	@Test
	public void testPutSettingsIgnoresAdvertisingFieldUnlessSpecified() {
		archive.getConfig().stopAdvertising();

		XArchiveSettings settings = new XArchiveSettings();
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(archive.getConfig().isAdvertising());

		archive.getConfig().advertise();
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertTrue(archive.getConfig().isAdvertising());
	}

	@Test
	public void testPutSettingsSetsAdvertisingFalseIfRequesting() {
		archive.getConfig().advertise();
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAdvertising(false);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(archive.getConfig().isAdvertising());
	}

	@Test
	public void testPutSettingsSetsAdvertisingTrueIfRequesting() {
		archive.getConfig().stopAdvertising();
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAdvertising(true);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertTrue(archive.getConfig().isAdvertising());
	}

	@Test
	public void testPutSettingsIgnoresRequestingAllFieldUnlessSpecified() {
		archive.getConfig().getSwarm().stopRequestingAll();

		XArchiveSettings settings = new XArchiveSettings();
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(archive.getConfig().getSwarm().isRequestingAll());

		archive.getConfig().getSwarm().requestAll();
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertTrue(archive.getConfig().getSwarm().isRequestingAll());
	}

	@Test
	public void testPutSettingsSetsRequestingAllFalseIfRequesting() {
		archive.getConfig().getSwarm().requestAll();
		XArchiveSettings settings = new XArchiveSettings();
		settings.setRequestingAll(false);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(archive.getConfig().getSwarm().isRequestingAll());
	}

	@Test
	public void testPutSettingsSetsRequestingAllTrueIfRequesting() {
		archive.getConfig().getSwarm().stopRequestingAll();
		XArchiveSettings settings = new XArchiveSettings();
		settings.setRequestingAll(true);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertTrue(archive.getConfig().getSwarm().isRequestingAll());
	}

	@Test
	public void testPutSettingsIgnoresPeerLimitFieldUnlessSpecified() {
		XArchiveSettings settings = new XArchiveSettings();

		archive.getConfig().getSwarm().setMaxSocketCount(1234);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertEquals(1234, archive.getConfig().getSwarm().getMaxSocketCount());

		archive.getConfig().getSwarm().setMaxSocketCount(4321);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertEquals(4321, archive.getConfig().getSwarm().getMaxSocketCount());
	}

	@Test
	public void testPutSettingsSetsPeerLimitIfRequested() {
		archive.getConfig().getSwarm().setMaxSocketCount(1234);
		XArchiveSettings settings = new XArchiveSettings();
		settings.setPeerLimit(4321);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertEquals(4321, archive.getConfig().getSwarm().getMaxSocketCount());
	}

	@Test
	public void testPutSettingsIgnoresAutocommitFieldUnlessSpecified() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();

		State.sharedState().activeManager(archive.getConfig()).setAutocommit(false);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(State.sharedState().activeManager(archive.getConfig()).isAutocommiting());

		State.sharedState().activeManager(archive.getConfig()).setAutocommit(true);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertTrue(State.sharedState().activeManager(archive.getConfig()).isAutocommiting());
	}

	@Test
	public void testPutSettingsIgnoresAutocommitFieldIfSeedOnly() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutocommiting(true);
		State.sharedState().activeManager(archive.getConfig()).setAutocommit(false);
		archive.getConfig().getAccessor().becomeSeedOnly();

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(State.sharedState().activeManager(archive.getConfig()).isAutocommiting());
	}

	@Test
	public void testPutSettingsSetsAutocommitFalseIfRequested() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutocommiting(false);
		State.sharedState().activeManager(archive.getConfig()).setAutocommit(true);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(State.sharedState().activeManager(archive.getConfig()).isAutocommiting());
		
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertFalse(resp.get("autocommit").asBoolean());
	}

	@Test
	public void testPutSettingsSetsAutocommitTrueIfRequested() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutocommiting(true);
		State.sharedState().activeManager(archive.getConfig()).setAutocommit(false);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertTrue(State.sharedState().activeManager(archive.getConfig()).isAutocommiting());
		
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertTrue(resp.get("autocommit").asBoolean());
	}
	
	@Test
	public void testPutSettingsSetsAutomergeFalseIfRequested() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutomerge(false);
		State.sharedState().activeManager(archive.getConfig()).setAutomerge(true);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(State.sharedState().activeManager(archive.getConfig()).isAutomerging());
		
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertFalse(resp.get("automerge").asBoolean());
	}
	
	@Test
	public void testPutSettingsSetsAutomergeTrueIfRequested() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutomerge(true);
		State.sharedState().activeManager(archive.getConfig()).setAutomerge(false);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertTrue(State.sharedState().activeManager(archive.getConfig()).isAutomerging());
		
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertTrue(resp.get("automerge").asBoolean());
	}

	@Test
	public void testPutSettingsIgnoresAutocommitIntervalFieldUnlessSpecified() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();

		State.sharedState().activeManager(archive.getConfig()).setAutocommitIntervalMs(1234);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		State.sharedState().activeManager(archive.getConfig()).getAutocommitIntervalMs();

		State.sharedState().activeManager(archive.getConfig()).setAutocommitIntervalMs(4321);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		State.sharedState().activeManager(archive.getConfig()).getAutocommitIntervalMs();
	}

	@Test
	public void testPutSettingsIgnoresAutocommitIntervalFieldIfSeedOnly() throws IOException {
		State.sharedState().activeManager(archive.getConfig()).setAutocommitIntervalMs(1234);
		archive.getConfig().getAccessor().becomeSeedOnly();
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutocommitInterval(4321);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertEquals(1234, State.sharedState().activeManager(archive.getConfig()).getAutocommitIntervalMs());
	}

	@Test
	public void testPutSettingsSetsAutocommitIntervalIfRequested() throws IOException {
		State.sharedState().activeManager(archive.getConfig()).setAutocommitIntervalMs(1234);
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutocommitInterval(4321);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertEquals(4321, State.sharedState().activeManager(archive.getConfig()).getAutocommitIntervalMs());
	}

	@Test
	public void testPutSettingsIgnoresAutofollowFieldUnlessSpecified() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();

		State.sharedState().activeManager(archive.getConfig()).setAutofollow(false);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(State.sharedState().activeManager(archive.getConfig()).isAutofollowing());

		State.sharedState().activeManager(archive.getConfig()).setAutofollow(true);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertTrue(State.sharedState().activeManager(archive.getConfig()).isAutofollowing());
	}

	@Test
	public void testPutSettingsIgnoresAutofollowFieldIfSeedOnly() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutofollow(true);
		State.sharedState().activeManager(archive.getConfig()).setAutofollow(false);
		archive.getConfig().getAccessor().becomeSeedOnly();

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(State.sharedState().activeManager(archive.getConfig()).isAutofollowing());
	}

	@Test
	public void testPutSettingsSetsAutofollowFalseIfRequested() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutofollow(false);
		State.sharedState().activeManager(archive.getConfig()).setAutofollow(true);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(State.sharedState().activeManager(archive.getConfig()).isAutofollowing());

		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertFalse(resp.get("autofollow").asBoolean());
}

	@Test
	public void testPutSettingsSetsAutofollowTrueIfRequested() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutofollow(true);
		State.sharedState().activeManager(archive.getConfig()).setAutofollow(false);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertTrue(State.sharedState().activeManager(archive.getConfig()).isAutofollowing());

		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertTrue(resp.get("autofollow").asBoolean());
	}

	@Test
	public void testPutSettingsIgnoresAutomirrorFieldUnlessSpecified() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();

		State.sharedState().activeManager(archive.getConfig()).setAutomirror(false);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(State.sharedState().activeManager(archive.getConfig()).isAutomirroring());

		State.sharedState().activeManager(archive.getConfig()).setAutomirrorPath(TESTDIR);
		State.sharedState().activeManager(archive.getConfig()).setAutomirror(true);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertTrue(State.sharedState().activeManager(archive.getConfig()).isAutomirroring());
	}

	@Test
	public void testPutSettingsIgnoresAutomirrorFieldIfSeedOnly() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutomirror(true);
		State.sharedState().activeManager(archive.getConfig()).setAutomirror(false);
		archive.getConfig().getAccessor().becomeSeedOnly();

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(State.sharedState().activeManager(archive.getConfig()).isAutomirroring());
	}

	@Test
	public void testPutSettingsSetsAutomirrorFalseIfRequested() throws IOException {
		State.sharedState().activeManager(archive.getConfig()).setAutomirrorPath(TESTDIR);
		State.sharedState().activeManager(archive.getConfig()).setAutomirror(true);

		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutomirror(false);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(State.sharedState().activeManager(archive.getConfig()).isAutomirroring());
		
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertFalse(resp.get("autofollow").asBoolean());
	}

	@Test
	public void testPutSettingsReturns400IfAutomirrorTrueAndAutomirrorPathNull() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutomirror(true);

		WebTestUtils.requestPutWithError(target, 400, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(State.sharedState().activeManager(archive.getConfig()).isAutomirroring());
	}

	@Test
	public void testPutSettingsReturns409IfAutomirrorTrueAndAutomirrorPathNotFound() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutomirror(true);

		State.sharedState().activeManager(archive.getConfig()).setAutomirrorPath("/path/to/doesntexist");
		WebTestUtils.requestPutWithError(target, 409, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(State.sharedState().activeManager(archive.getConfig()).isAutomirroring());
	}

	@Test
	public void testPutSettingsSetsAutomirrorTrueIfRequested() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutomirror(true);
		State.sharedState().activeManager(archive.getConfig()).setAutomirrorPath(TESTDIR);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertTrue(State.sharedState().activeManager(archive.getConfig()).isAutomirroring());
		
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		assertTrue(resp.get("automirror").asBoolean());
	}

	@Test
	public void testPutSettingsIgnoresAutomirrorPathFieldUnlessSet() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		State.sharedState().activeManager(archive.getConfig()).setAutomirrorPath(TESTDIR);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertEquals(TESTDIR, State.sharedState().activeManager(archive.getConfig()).getAutomirrorPath());
	}

	@Test
	public void testPutSettingsIgnoresAutomirrorPathFieldIfNull() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		State.sharedState().activeManager(archive.getConfig()).setAutomirrorPath(TESTDIR);
		settings.setAutomirrorPath(null);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertEquals(TESTDIR, State.sharedState().activeManager(archive.getConfig()).getAutomirrorPath());
	}

	@Test
	public void testPutSettingsSetAutomirrorPathNullIfSpecifiedAsEmptyString() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		State.sharedState().activeManager(archive.getConfig()).setAutomirrorPath(TESTDIR);
		settings.setAutomirrorPath("");

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertNull(State.sharedState().activeManager(archive.getConfig()).getAutomirrorPath());
	}

	@Test
	public void testPutSettingsSetsAutomirrorPathIfRequested() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutomirrorPath(TESTDIR);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertEquals(TESTDIR, State.sharedState().activeManager(archive.getConfig()).getAutomirrorPath());
	}

	@Test
	public void testPutSettingsSetAutomirrorPathDoesNotStartAutomirroring() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutomirrorPath(TESTDIR);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertFalse(State.sharedState().activeManager(archive.getConfig()).isAutomirroring());
	}

	@Test
	public void testPutSettingsSetAutomirrorTrueAndAutomirrorPathStartsAutomirroring() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutomirrorPath(TESTDIR);
		settings.setAutomirror(true);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);
		assertEquals(TESTDIR, State.sharedState().activeManager(archive.getConfig()).getAutomirrorPath());
		assertTrue(State.sharedState().activeManager(archive.getConfig()).isAutomirroring());
	}

	@Test
	public void testPutSettingsSetAutomirrorToBadPathReturns409() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAutomirrorPath("/path/to/doesntexist");

		WebTestUtils.requestPutWithError(target, 409, "archives/" + transformArchiveId(archive) + "/settings", settings);
	}

	@Test
	public void testPutKeysSetsPassphraseRootFromPassphraseIfSpecified() throws IOException {
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadPassphrase(passphrase);

		archive.getConfig().clearWriteRoot();
		archive.getConfig().getAccessor().becomeSeedOnly();
		archive.getConfig().getMaster().storedAccess().storeArchiveAccess(archive.getConfig(), StoredAccess.ACCESS_LEVEL_SEED);
		assertTrue(archive.getConfig().isReadOnly());
		assertTrue(archive.getConfig().getAccessor().isSeedOnly());

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/keys", spec);
		assertReadable(archive);
	}

	@Test
	public void testPutKeysSetsPassphraseRootFromReadKeyIfSpecified() throws IOException {
		byte[] passphraseRoot = State.sharedCrypto().deriveKeyFromPassphrase(passphrase.getBytes(), CryptoSupport.PASSPHRASE_SALT_READ);
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadKey(passphraseRoot);

		archive.getConfig().clearWriteRoot();
		archive.getConfig().getAccessor().becomeSeedOnly();
		archive.getConfig().getMaster().storedAccess().storeArchiveAccess(archive.getConfig(), StoredAccess.ACCESS_LEVEL_SEED);
		assertTrue(archive.getConfig().isReadOnly());
		assertTrue(archive.getConfig().getAccessor().isSeedOnly());

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/keys", spec);
		assertReadable(archive);
	}

	@Test
	public void testPutKeysSetsWriteRootFromWritePassphraseIfSpecified() throws IOException {
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setWritePassphrase(passphrase);

		archive.getConfig().clearWriteRoot();
		archive.getConfig().getMaster().storedAccess().storeArchiveAccess(archive.getConfig(), StoredAccess.ACCESS_LEVEL_READ);
		assertTrue(archive.getConfig().isReadOnly());

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/keys", spec);
		assertWritable(archive);
	}

	@Test
	public void testPutKeysSetsWriteRootFromWriteKeyIfSpecified() throws IOException {
		byte[] writeRoot = State.sharedCrypto().deriveKeyFromPassphrase(passphrase.getBytes(), CryptoSupport.PASSPHRASE_SALT_WRITE);
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setWriteKey(writeRoot);

		archive.getConfig().clearWriteRoot();
		archive.getConfig().getMaster().storedAccess().storeArchiveAccess(archive.getConfig(), StoredAccess.ACCESS_LEVEL_READ);
		assertTrue(archive.getConfig().isReadOnly());

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/keys", spec);
		assertWritable(archive);
	}

	@Test
	public void testPutKeysAllowsSimultaneousSpecificationOfReadAndWriteKeys() throws IOException {
		byte[] wroot = State.sharedCrypto().deriveKeyFromPassphrase(passphrase.getBytes(), CryptoSupport.PASSPHRASE_SALT_WRITE);
		byte[] rroot = State.sharedCrypto().deriveKeyFromPassphrase(passphrase.getBytes(), CryptoSupport.PASSPHRASE_SALT_READ);
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setWriteKey(wroot);
		spec.setReadKey(rroot);

		archive.getConfig().clearWriteRoot();
		archive.getConfig().getAccessor().becomeSeedOnly();
		archive.getConfig().getMaster().storedAccess().storeArchiveAccess(archive.getConfig(), StoredAccess.ACCESS_LEVEL_SEED);
		assertTrue(archive.getConfig().isReadOnly());
		assertTrue(archive.getConfig().getAccessor().isSeedOnly());

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/keys", spec);
		assertFalse(archive.getConfig().isReadOnly());
		assertFalse(archive.getConfig().getAccessor().isSeedOnly());

		assertReadable(archive);
		assertWritable(archive);
	}

	@Test
	public void testPutKeysDoesNotAlterStorageLevelifAccessLevelOmitted() throws IOException {
		byte[] passphraseRoot = State.sharedCrypto().deriveKeyFromPassphrase(passphrase.getBytes(), CryptoSupport.PASSPHRASE_SALT_READ);
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setReadKey(passphraseRoot);
		spec.setWriteKey(passphraseRoot);

		archive.getConfig().clearWriteRoot();
		archive.getConfig().getAccessor().becomeSeedOnly();
		archive.getConfig().getMaster().storedAccess().storeArchiveAccess(archive.getConfig(), StoredAccess.ACCESS_LEVEL_SEED);
		assertTrue(archive.getConfig().isReadOnly());
		assertTrue(archive.getConfig().getAccessor().isSeedOnly());

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/keys", spec);

		try(State state2 = new State(State.defaultPassphrase(), State.sharedState().getMaster().getStorage())) {
			byte[] archiveId = archive.getConfig().getArchiveId();
			assertTrue(state2.configForArchiveId(archiveId).getAccessor().isSeedOnly());
		}
	}

	@Test
	public void testPutKeysStoresAllKeysIfAccessLevelReadwriteRequested() throws IOException {
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setSavedAccessLevel(StoredAccess.ACCESS_LEVEL_READWRITE);

		archive.getConfig().getMaster().storedAccess().storeArchiveAccess(archive.getConfig(), StoredAccess.ACCESS_LEVEL_SEED);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/keys", spec);

		try(
			State state2 = new State(State.defaultPassphrase(), State.sharedState().getMaster().getStorage());
			ZKArchiveConfig config2 = state2.configForArchiveId(archive.getConfig().getArchiveId());
		) {
			assertWritable(config2.getArchive());
			assertReadable(config2.getArchive());
		}
	}

	@Test
	public void testPutKeysStoresReadNotWriteKeyIfAccessLevelReadRequested() throws IOException {
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setSavedAccessLevel(StoredAccess.ACCESS_LEVEL_READ);
		spec.setReadPassphrase(passphrase);

		archive.getConfig().getMaster().storedAccess().storeArchiveAccess(archive.getConfig(), StoredAccess.ACCESS_LEVEL_SEED);
		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/keys", spec);

		try(
			State state2 = new State(State.defaultPassphrase(), State.sharedState().getMaster().getStorage());
			ZKArchiveConfig config2 = state2.configForArchiveId(archive.getConfig().getArchiveId());
		) {
			assertTrue(config2.isReadOnly());
			assertReadable(config2.getArchive());
		}
	}

	@Test
	public void testPutKeysStoresSeedNotReadOrWriteKeyIfAccessLevelSeedRequested() throws IOException {
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setSavedAccessLevel(StoredAccess.ACCESS_LEVEL_SEED);
		spec.setReadPassphrase(passphrase);
		spec.setWritePassphrase(passphrase);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/keys", spec);

		try(
			State state2 = new State(State.defaultPassphrase(), State.sharedState().getMaster().getStorage());
			ZKArchiveConfig config2 = state2.configForArchiveId(archive.getConfig().getArchiveId());
		) {
			assertTrue(config2.isReadOnly());
			assertTrue(config2.getAccessor().isSeedOnly());
		}
	}

	@Test
	public void testPutKeysRemovesStoredAccessIfAccessLevelNoneRequested() throws IOException {
		XArchiveSpecification spec = new XArchiveSpecification();
		spec.setSavedAccessLevel(StoredAccess.ACCESS_LEVEL_NONE);
		spec.setReadPassphrase(passphrase);
		spec.setWritePassphrase(passphrase);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/keys", spec);

		try(
			State state2 = new State(State.defaultPassphrase(), State.sharedState().getMaster().getStorage());
			ZKArchiveConfig config2 = state2.configForArchiveId(archive.getConfig().getArchiveId());
		) {
			assertNull(config2);
		}

		// test that we still have in-memory access
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archive.getConfig().getArchiveId());
		config.getArchive().openBlank().commitAndClose();
	}
	
	@SuppressWarnings("deprecation")
	@Test
	public void testPostDiscoverTriggersDHTSearch() throws IOException {
		MutableBoolean called = new MutableBoolean();

		class DummyClient extends DHTClient {
			DummyClient() {
				this.initialized = true;
			}
			
			@Override
			public void lookup(DHTID searchId, Key lookupKey, LookupCallback callback) {
				called.setTrue();
			}
			
			@Override public void close() {}
		};
		
		DummyClient client = new DummyClient(); 
		
		State.sharedState().getMaster().getDHTClient().close();
		State.sharedState().getMaster().setDHTClient(client);
		WebTestUtils.requestPost(target, "archives/" + transformArchiveId(archive) + "/discover", null);
		assertTrue(called.booleanValue());
	}
	
	@Test
	public void testPostDiscoverReturns404IfArchiveNotFound() {
		WebTestUtils.requestPostWithError(target, 404, "archives/nonexistent/discover", null);
	}

	@Test
	public void testSettingsArePersistent() throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		settings.setAdvertising(true);
		settings.setAutocommiting(true);
		settings.setAutomerge(true);
		settings.setAutofollow(true);
		settings.setRequestingAll(true);
		settings.setAutocommitInterval(12345);
		settings.setAutomirrorPath(TESTDIR);
		settings.setAutomirror(true);
		settings.setPeerLimit(4321);

		WebTestUtils.requestPut(target, "archives/" + transformArchiveId(archive) + "/settings", settings);

		State.resetState();
		JsonNode resp = WebTestUtils.requestGet(target, "archives/" + transformArchiveId(archive) + "/settings");
		
		assertEquals(settings.isAdvertising().booleanValue(), resp.get("advertising").asBoolean());
		assertEquals(settings.isAutocommit().booleanValue(), resp.get("autocommit").asBoolean());
		assertEquals(settings.isAutomerge().booleanValue(), resp.get("automerge").asBoolean());
		assertEquals(settings.isAutofollow().booleanValue(), resp.get("autofollow").asBoolean());
		assertEquals(settings.isRequestingAll().booleanValue(), resp.get("requestingAll").asBoolean());
		assertEquals(settings.getAutocommitInterval().intValue(), resp.get("autocommitInterval").intValue());
		assertEquals(settings.getAutomirrorPath(), resp.get("automirrorPath").textValue());
		assertEquals(settings.isAutomirror().booleanValue(), resp.get("automirror").asBoolean());
		assertEquals(settings.getPeerLimit().intValue(), resp.get("peerLimit").intValue());
	}
}
