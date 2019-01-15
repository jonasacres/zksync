package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.SearchFailedException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.backedfs.BackedFS;
import com.acrescrypto.zksync.fs.swarmfs.SwarmFS;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig.InvalidArchiveConfigException;
import com.acrescrypto.zksync.utility.Util;

public class ZKArchiveConfigTest {
	final static int PAGE_SIZE = ZKArchive.DEFAULT_PAGE_SIZE;
	final static String TEST_DESCRIPTION = "test archive";
	
	static ZKMaster master;
	static ArchiveAccessor accessor, seedAccessor;
	static Key key, storedPassphraseRoot, storedSeedRoot, storedLocalRoot;
	ZKArchiveConfig config, seedConfig;
	
	public void assertReadable(ZKArchiveConfig config) throws IOException {
		ZKArchiveConfig clone = ZKArchiveConfig.openExisting(accessor, config.getArchiveId());
		
		assertEquals(config.getPageSize(), clone.getPageSize());
		assertEquals(config.getDescription(), clone.getDescription());
		
		assertTrue(Arrays.equals(config.getArchiveId(), clone.getArchiveId()));
		assertTrue(Arrays.equals(config.archiveRoot.getRaw(), clone.archiveRoot.getRaw()));
		assertTrue(Arrays.equals(config.privKey.getBytes(), clone.privKey.getBytes()));
		assertTrue(Arrays.equals(config.pubKey.getBytes(), clone.pubKey.getBytes()));

		ZKFS fs = clone.getArchive().openBlank();
		fs.write("foo", "bar".getBytes());
		RevisionTag tag = fs.commit();
		assertTrue(Arrays.equals("bar".getBytes(), clone.getArchive().openRevision(tag).read("foo")));
		
		clone.close();
	}
	
	public void assertUnreadable(ZKArchiveConfig config) throws IOException {
		assertUnreadable(config, accessor);
		assertUnreadable(config, seedAccessor);
	}
	
	public void assertUnreadable(ZKArchiveConfig config, ArchiveAccessor accessor) throws IOException {
		try {
			ZKArchiveConfig.openExisting(accessor, config.getArchiveId());
			fail();
		} catch(InvalidArchiveConfigException exc) {
		}
	}
	
	public byte[][] makeValidPageInfo() throws IOException {
		byte[][] info = new byte[2][];
		ZKFS fs = config.archive.openBlank();
		fs.write("foo", new byte[config.pageSize]);
		fs.commit();
		PageTree tree = new PageTree(fs.inodeForPath("foo"));
		info[0] = tree.getPageTag(0);
		info[1] = fs.archive.storage.read(Page.pathForTag(info[0]));
		return info;
	}
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		master = ZKMaster.openBlankTestVolume();
		key = new Key(master.crypto, master.crypto.rng(master.crypto.symKeyLength()));
		accessor = master.makeAccessorForRoot(key, false);
		storedPassphraseRoot = accessor.passphraseRoot;
		storedSeedRoot = accessor.seedRoot;
		storedLocalRoot = accessor.localRoot;
		seedAccessor = accessor.makeSeedOnly();
	}
	
	@Before
	public void beforeEach() throws IOException {
		config = ZKArchiveConfig.create(accessor, TEST_DESCRIPTION, PAGE_SIZE);
		seedConfig = ZKArchiveConfig.openExisting(seedAccessor, config.getArchiveId());
	}
	
	@After
	public void afterEach() throws IOException {
		master.storage.purge();
		accessor.passphraseRoot = storedPassphraseRoot;
		accessor.seedRoot = storedSeedRoot;
		accessor.localRoot = storedLocalRoot;
		config.close();
		seedConfig.close();
	}
	
	@AfterClass
	public static void afterAll() {
		master.close();
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testCreateNewArchive() throws IOException {
		assertNotNull(config.getArchive());
		ZKFS fs = config.getArchive().openBlank();
		fs.write("foo", "bar".getBytes());
		RevisionTag tag = fs.commit();
		assertTrue(Arrays.equals("bar".getBytes(), config.getArchive().openRevision(tag).read("foo")));
		fs.close();
	}
	
	@Test
	public void testLoadExistingArchiveFromID() throws IOException {
		assertReadable(config);
	}
	
	@Test
	public void testVerifyReturnsFalseIfSignatureTampered() throws IOException {
		byte[] configData = config.storage.read(Page.pathForTag(config.tag()));
		int offset = configData.length-1;
		configData[offset] ^= 0x01;
		assertFalse(config.verify(configData));
	}
	
	// TODO verification tests
	@Test
	public void testVerifyReturnsFalseIfDataTampered() throws IOException {
		byte[] configData = config.storage.read(Page.pathForTag(config.tag()));
		configData[0] ^= 0x01;
		assertFalse(config.verify(configData));
	}
	
	@Test
	public void testReadThrowsExceptionIfTampered() throws IOException {
		byte[] configData = config.storage.read(Page.pathForTag(config.tag()));
		for(int i = 0; i < configData.length; i += configData.length/512) {
			configData[i] ^= 0x01;
			config.storage.write(Page.pathForTag(config.tag()), configData);
			assertUnreadable(config);
			configData[i] ^= 0x01;
		}
	}	
	
	@Test
	public void testGetArchiveId() {
		// TODO EasySafe: (test) test that the archive id is correct
		assertTrue(Arrays.equals(config.archiveId, seedConfig.archiveId));
	}
	
	@Test
	public void testGetPageSize() {
		assertEquals(PAGE_SIZE, config.getPageSize());
	}
	
	@Test
	public void testGetPageSizeSeedOnly() {
		assertEquals(PAGE_SIZE, seedConfig.getPageSize());
	}
	
	@Test
	public void testGetPrivKey() {
		assertNull(seedConfig.getPrivKey());
		assertTrue(Arrays.equals(master.crypto.makePrivateSigningKey(config.writeRoot.getRaw()).getBytes(), config.getPrivKey().getBytes()));
	}
	
	@Test
	public void testGetPubKey() {
		assertTrue(Arrays.equals(config.getPrivKey().publicKey().getBytes(), config.getPubKey().getBytes()));
		assertTrue(Arrays.equals(config.getPubKey().getBytes(), seedConfig.getPubKey().getBytes()));
	}
	
	@Test
	public void testGetDescription() {
		assertEquals(TEST_DESCRIPTION, config.getDescription());
	}
	
	@Test
	public void testGetDescriptionSeedOnly() {
		assertNull(seedConfig.getDescription());
	}
	
	@Test
	public void testGetLocalStorage() throws IOException {
		FS example = master.localStorageFsForArchiveId(config.archiveId);
		assertTrue(config.getLocalStorage().getClass().isInstance(example));
		assertTrue(seedConfig.getLocalStorage().getClass().isInstance(example));
		example.close();
	}
	
	@Test
	public void testGetStorage() throws IOException {
		ZKArchiveConfig[] configs = { config, seedConfig };
		for(ZKArchiveConfig cfg : configs) {
			assertTrue(cfg.getStorage() instanceof BackedFS);
			BackedFS backed = (BackedFS) cfg.getStorage();
			assertTrue(backed.getBackupFS() instanceof SwarmFS);
			SwarmFS swarmfs = (SwarmFS) backed.getBackupFS();
			assertEquals(cfg, swarmfs.getSwarm().getConfig());
			assertEquals(master.storageFsForArchiveId(cfg.getArchiveId()).getClass(), backed.getCacheFS().getClass());
		}
	}
	
	@Test
	public void getCacheStorage() throws IOException {
		ZKArchiveConfig[] configs = { config, seedConfig };
		for(ZKArchiveConfig cfg : configs) {
			assertTrue(cfg.getStorage() instanceof BackedFS);
			BackedFS backed = (BackedFS) cfg.getStorage();
			assertEquals(backed.getCacheFS(), cfg.getCacheStorage());
		}
	}
	
	@Test
	public void testDeriveKeyMatchesArchiveAccessor() {
		assertTrue(Arrays.equals(config.deriveKey(0, "foo").getRaw(), accessor.deriveKey(0, "foo").getRaw()));
		assertTrue(Arrays.equals(config.deriveKey(2, "foo").getRaw(), accessor.deriveKey(2, "foo").getRaw()));
		assertTrue(Arrays.equals(config.deriveKey(0, "bar").getRaw(), accessor.deriveKey(0, "bar").getRaw()));
		assertTrue(Arrays.equals(config.deriveKey(0, "foo", "bar".getBytes()).getRaw(), accessor.deriveKey(0, "foo", "bar".getBytes()).getRaw()));
	}
	
	@Test
	public void testDeriveKeyWithSeed() {
		assertTrue(Arrays.equals(config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, "foo").getRaw(), seedConfig.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, "foo").getRaw()));
		assertTrue(Arrays.equals(config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, "bar").getRaw(), seedConfig.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, "bar").getRaw()));
	}
	
	@Test
	public void testDeriveKeyAcceptsArchiveRoot() {
		config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, "foo");
	}
	
	@Test
	public void testDeriveKeyForArchiveRootMatchesTestVectors() {
		class ArchiveKeyDerivationExample {
			public Key key;
			public byte[] expectedResult, tweak;
			public String id;
			
			public ArchiveKeyDerivationExample(String keyStr, String id, String tweakStr, String expectedResultStr) {
				this.key = new Key(accessor.getMaster().getCrypto(), Util.hexToBytes(keyStr));
				this.id = id;
				this.tweak = Util.hexToBytes(tweakStr);
				this.expectedResult = Util.hexToBytes(expectedResultStr);
			}
			
			public void validate() {
				int[] roots = {
						ArchiveAccessor.KEY_ROOT_PASSPHRASE,
						ArchiveAccessor.KEY_ROOT_ARCHIVE,
						ArchiveAccessor.KEY_ROOT_SEED,
						ArchiveAccessor.KEY_ROOT_LOCAL,
				};
				
				for(int root : roots) {
					accessor.passphraseRoot = accessor.seedRoot = accessor.localRoot = null;
					config.archiveRoot = null;
					switch(root) {
					case ArchiveAccessor.KEY_ROOT_PASSPHRASE:
						accessor.passphraseRoot = key;
						break;
					case ArchiveAccessor.KEY_ROOT_ARCHIVE:
						config.archiveRoot = key;
						break;
					case ArchiveAccessor.KEY_ROOT_SEED:
						accessor.seedRoot = key;
						break;
					case ArchiveAccessor.KEY_ROOT_LOCAL:
						accessor.localRoot = key;
						break;
					default:
						fail();
					}
					
					Key derived = config.deriveKey(root, id, tweak);
					assertArrayEquals(expectedResult, derived.getRaw());
				}
			}
		}
		
		// These vectors are self-generated, and are here as a canary against unintended changes to key derivation logic.
		// Generated from test-vectors.py, Python 3.6.5, commit 125943e0d5ec57fcf91365dfca6ad3355aafd0f1
		new ArchiveKeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			"foo",
			"",
			"010c58af3dcaf904b08b657f9278f18bf2bfb65efbd92000b646f5ac66ebdc2f").validate();
		new ArchiveKeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			"bar",
			"",
			"82eb5c004e2890e274faa46e0dd16b8c476d558ff8ecc9ff162d7dc3ad5411f1").validate();
		new ArchiveKeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			"foo",
			"10111213",
			"89809b56c6a5bb2c79db364c46bafdd93ded5dbb12d2a580c65a45aa2781f29d").validate();
		new ArchiveKeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			"808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf",
			"c84b4ee9d379680a9a5abc0d93fda4e5e8fad56cc473878a9709027690e8ff22").validate();
	}
	
	@Test
	public void testArchiveRootIsNondeterministic() throws IOException {
		ZKArchiveConfig newConfig = ZKArchiveConfig.create(accessor, TEST_DESCRIPTION, PAGE_SIZE);
		assertFalse(Arrays.equals(config.archiveRoot.getRaw(), newConfig.archiveRoot.getRaw()));
		newConfig.close();
	}
	
	@Test
	public void testPubKeyDeterministicWithWriteKey() throws IOException {
		ZKArchiveConfig newConfig = ZKArchiveConfig.create(accessor, TEST_DESCRIPTION, PAGE_SIZE, config.archiveRoot, config.writeRoot);
		assertArrayEquals(config.getPubKey().getBytes(), newConfig.getPubKey().getBytes());
		newConfig.close();
	}
	
	@Test
	public void testPubKeyChangesWithWriteKey() throws IOException {
		Key newWriteKey = new Key(master.crypto);
		ZKArchiveConfig newConfig = ZKArchiveConfig.create(accessor, TEST_DESCRIPTION, PAGE_SIZE, config.archiveRoot, newWriteKey);
		assertFalse(Arrays.equals(config.getPubKey().getBytes(), newConfig.getPubKey().getBytes()));
		newConfig.close();
	}
	
	@Test
	public void testDefaultArchiveGeneratesConsistentArchiveId() throws IOException {
		ZKArchiveConfig config1 = ZKArchiveConfig.createDefault(accessor);
		ZKArchiveConfig config2 = ZKArchiveConfig.createDefault(accessor);
		
		assertArrayEquals(config1.archiveId, config2.archiveId);
		
		config1.close();
		config2.close();
	}
	
	@Test
	public void testDefaultArchiveUsesPassphraseForRootKey() throws IOException {
		ZKArchiveConfig config = ZKArchiveConfig.createDefault(accessor);
		assertArrayEquals(accessor.passphraseRoot.getRaw(), config.archiveRoot.getRaw());
		config.close();
	}

	@Test
	public void testDefaultArchiveUsesPassphraseForWriteKey() throws IOException {
		ZKArchiveConfig config = ZKArchiveConfig.createDefault(accessor);
		assertArrayEquals(accessor.passphraseRoot.getRaw(), config.writeRoot.getRaw());
		config.close();
	}

	@Test
	public void testValidatePageReturnsTrueOnValidPages() throws IOException {
		byte[][] info = makeValidPageInfo();
		assertTrue(config.validatePage(info[0], info[1]));
		assertTrue(seedConfig.validatePage(info[0], info[1]));
	}
	
	@Test
	public void testValidatePageReturnsFalseForTamperedConfigPage() throws IOException {
		byte[] serialized = config.storage.read(Page.pathForTag(config.tag()));
		serialized[19] ^= 0x04;
		assertFalse(config.validatePage(config.tag(), serialized));
	}
	
	@Test
	public void testValidatePageReturnsFalseOnTamperedPages() throws IOException {
		byte[][] info = makeValidPageInfo();
		info[1][2] ^= 0x40;
		assertFalse(config.validatePage(info[0], info[1]));
		assertFalse(seedConfig.validatePage(info[0], info[1]));
	}
	
	@Test
	public void testValidatePageReturnsFalseOnTamperedTags() throws IOException {
		byte[][] info = makeValidPageInfo();
		info[0][1] ^= 0x08;
		assertFalse(config.validatePage(info[0], info[1]));
		assertFalse(seedConfig.validatePage(info[0], info[1]));
	}
	
	@Test
	public void testValidatePageReturnsFalseOnForgedTag() throws IOException {
		Key authKey = config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, "easysafe-page-auth-key");
		byte[] fakePage = new byte[config.pageSize];
		byte[] tag = authKey.authenticate(fakePage);
		assertFalse(config.validatePage(tag, fakePage));
		assertFalse(seedConfig.validatePage(tag, fakePage));
	}
	
	@Test
	public void testValidatePageReturnsFalseOnForgedSignature() throws IOException {
		ByteBuffer fakePage = ByteBuffer.allocate(PAGE_SIZE+master.crypto.asymSignatureSize());
		fakePage.position(fakePage.capacity()-master.crypto.asymSignatureSize());
		fakePage.put(config.privKey.sign(fakePage.array(), 0, fakePage.position()));
		byte[] tag = new byte[master.crypto.hashLength()];
		assertFalse(config.validatePage(tag, fakePage.array()));
		assertFalse(seedConfig.validatePage(tag, fakePage.array()));
	}
	
	@Test
	public void testValidatePageReturnsTrueOnForgedSignatureAndTag() throws IOException {
		Key authKey = config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, "easysafe-page-auth-key");
		ByteBuffer fakePage = ByteBuffer.allocate(PAGE_SIZE+master.crypto.asymSignatureSize());
		fakePage.position(fakePage.capacity()-master.crypto.asymSignatureSize());
		fakePage.put(config.privKey.sign(fakePage.array(), 0, fakePage.position()));
		byte[] tag = authKey.authenticate(fakePage.array());
		assertTrue(config.validatePage(tag, fakePage.array()));
		assertTrue(seedConfig.validatePage(tag, fakePage.array()));
	}
	
	@Test
	public void testValidatePageReturnsTrueForValidConfigFile() throws IOException {
		byte[] configData = config.storage.read(Page.pathForTag(config.tag()));
		assertTrue(config.validatePage(config.tag(), configData));
	}
	
	@Test
	public void testValidatePageReturnsFalseForTamperedConfigFile() throws IOException {
		byte[] configData = config.storage.read(Page.pathForTag(config.tag()));
		configData[1209] ^= 0x40;
		assertFalse(config.validatePage(config.tag(), configData));
	}
	
	@Test
	public void testSerializedPageMatchesExpectedLength() throws IOException {
		config.write();
		long expectedSize = config.getSerializedPageSize();
		long actualSize = config.getCacheStorage().stat(Page.pathForTag(config.tag())).getSize();
		assertEquals(expectedSize, actualSize);
	}
	
	@Test
	public void testArchivesHaveUniqueConfigFilePathTags() throws IOException {
		ZKArchiveConfig config2 = ZKArchiveConfig.create(accessor, TEST_DESCRIPTION, PAGE_SIZE);
		assertFalse(Arrays.equals(config.tag(), config2.tag()));
		config2.close();
	}
	
	@Test
	public void testNondefaultWriteKeyGeneratesUniqueArchiveId() throws IOException {
		Key writeKey = new Key(master.crypto);
		ZKArchiveConfig wConfig = ZKArchiveConfig.create(accessor, TEST_DESCRIPTION, PAGE_SIZE, accessor.passphraseRoot, writeKey);
		assertFalse(Arrays.equals(wConfig.getArchiveId(), config.getArchiveId()));
	}
	
	@Test
	public void testArchiveIdDeterministicWithWriteKey() throws IOException {
		Key writeKey = new Key(master.crypto);
		ZKArchiveConfig wConfig1 = ZKArchiveConfig.create(accessor, TEST_DESCRIPTION, PAGE_SIZE, accessor.passphraseRoot, writeKey);
		ZKArchiveConfig wConfig2 = ZKArchiveConfig.create(accessor, TEST_DESCRIPTION, PAGE_SIZE, accessor.passphraseRoot, writeKey);
		assertArrayEquals(wConfig1.getArchiveId(), wConfig2.getArchiveId());
	}
	
	@Test
	public void testFinishOpeningFromSwarmCausesAdvertisementIfNoConfigFileStoredLocally() throws IOException {
		config.getStorage().purge();
		ZKArchiveConfig sConfig = ZKArchiveConfig.openExisting(accessor, config.getArchiveId(), false, null);
		try {
			sConfig.finishOpeningFromSwarm(1);
			fail();
		} catch(SearchFailedException exc) {}
		assertTrue(sConfig.isAdvertising());
	}
	
	@Test
	public void testFinishOpeningFromSwarmCausesAdvertisementIfConfigFileIsStoredLocally() throws IOException {
		ZKArchiveConfig sConfig = ZKArchiveConfig.openExisting(accessor, config.getArchiveId(), false, null);
		sConfig.finishOpeningFromSwarm(1);
		assertTrue(sConfig.isAdvertising());
	}

	@Test
	public void testFinishOpeningFromSwarmBlocksUntilPeersAreConnectedIfNoConfigFileStoredLocally() throws IOException {
		config.getStorage().purge();
		ZKArchiveConfig sConfig = ZKArchiveConfig.openExisting(accessor, config.getArchiveId(), false, null);
		long expectedTs = System.currentTimeMillis() + 100;
		try {
			sConfig.finishOpeningFromSwarm(100);
			fail();
		} catch(SearchFailedException exc) {}
		assertTrue(System.currentTimeMillis() >= expectedTs);
	}
	
	@Test
	public void testFinishOpeningFromSwarmDoesNotBlockIfConfigFileStoredLocally() throws IOException {
		ZKArchiveConfig sConfig = ZKArchiveConfig.openExisting(accessor, config.getArchiveId(), false, null);
		long ts = System.currentTimeMillis();
		sConfig.finishOpeningFromSwarm(100);
		assertTrue(System.currentTimeMillis() <= ts+5);
	}
	
	@Test
	public void testFinishOpeningFromSwarmThrowsSearchFailedExceptionIfNoPeersFoundWithinTimeout() throws IOException {
		config.getStorage().purge();
		ZKArchiveConfig sConfig = ZKArchiveConfig.openExisting(accessor, config.getArchiveId(), false, null);
		long expectedTs = System.currentTimeMillis() + 100;
		try {
			sConfig.finishOpeningFromSwarm(100);
			fail();
		} catch(SearchFailedException exc) {
			assertTrue(System.currentTimeMillis() >= expectedTs);
		}
	}

	// TODO API: (test) finishOpeningFromSwarm() returns if config file obtained from swarm
	
	// TODO API: (test) advertise() causes accessor to be discovered on DHT
	// TODO API: (test) advertise() causes TCP listener to advertise swarm
	// TODO API: (test) advertise() is idempotent
	
	@Test
	public void testIsAdvertisingReturnsTrueWhenAdvertising() {
		config.advertise();
		assertTrue(config.isAdvertising());
	}
	
	@Test
	public void testIsAdvertisingReturnsFalseWhenNotAdvertising() {
		// doubles as a check that we're not advertising by default
		assertFalse(config.isAdvertising());
	}
	
	// TODO API: (test) stopAdvertising() causes accessor to no longer be advertised on DHT
	// TODO API: (test) stopAdvertising() causes TCP listener to no longer advertise swarm
	
	@Test
	public void testStopAdvertisingCausesIsAdvertisingToReturnFalse() {
		config.advertise();
		assertTrue(config.isAdvertising());
		config.stopAdvertising();
		assertFalse(config.isAdvertising());
	}
	
	// TODO API: (test) stopAdvertising() is idempotent
	
	@Test
	public void testCloseCausesAdvertisingToStop() {
		config.advertise();
		config.close();
		assertFalse(config.isAdvertising());
	}
}
