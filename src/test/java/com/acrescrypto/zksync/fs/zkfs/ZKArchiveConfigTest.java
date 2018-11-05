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
	
	public void writeModifiedPageSize(int newSize) throws IOException {
		ZKArchiveConfig modified = new ZKArchiveConfig(accessor, config.archiveId, true, Key.blank(config.getCrypto())) {
			@Override
			protected byte[] serializeSecurePortion() {
				byte[] descString = description.getBytes();
				int headerSize = 4; // magic
				int sectionHeaderSize = 2 + 4; // section_type + length
				int archiveInfoSize = 8 + archiveRoot.getRaw().length + descString.length; // pageSize + textRoot + authRoot + description
				
				assertState(descString.length <= Short.MAX_VALUE);
				
				ByteBuffer buf = ByteBuffer.allocate(headerSize+sectionHeaderSize+archiveInfoSize);
				buf.putInt(CONFIG_MAGIC);
				buf.putShort((short) CONFIG_SECTION_ARCHIVE_INFO);
				buf.putInt(archiveInfoSize);
				buf.putLong(newSize);
				buf.put(archiveRoot.getRaw());
				buf.put(descString);
				
				assertState(!buf.hasRemaining());
				
				return buf.array();
			}
		};
		
		modified.write();
		modified.close();
	}
	
	public void writePhonySection(int statedSize, int actualSize) throws IOException {
		Key key = new Key(master.crypto);
		ZKArchiveConfig modified = new ZKArchiveConfig(accessor, "", ZKArchive.DEFAULT_PAGE_SIZE, key, key) {
			@Override
			protected byte[] serializeSecurePortion() {
				ByteBuffer securePortion = ByteBuffer.wrap(super.serializeSecurePortion());
				
				ByteBuffer buf = ByteBuffer.allocate(securePortion.limit() + 2 + 4 + actualSize);
				buf.putInt(securePortion.getInt());
				buf.putShort(Short.MAX_VALUE);
				buf.putInt(statedSize);
				buf.put(new byte[actualSize]);
				buf.put(securePortion);
				
				return buf.array();
			}
		};
		
		modified.write();
		config.close();
		config = modified;
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
	public void testSkipUnknownRecordTypes() throws IOException {
		writePhonySection(1024, 1024);
		assertReadable(config);
	}
	
	@Test
	public void testRefuseNegativeRecordLength() throws IOException {
		writePhonySection(-1024, 1024);
		assertUnreadable(config, accessor);
	}
	
	@Test
	public void testRefuseZeroPageSize() throws IOException {
		writeModifiedPageSize(0);
		assertUnreadable(config, accessor);
	}
	
	@Test
	public void testRefuseNegativePageSize() throws IOException {
		writeModifiedPageSize(-config.pageSize);
		assertUnreadable(config, accessor);
	}
	
	@Test
	public void testRefuseExcessivePageSize() throws IOException {
		writeModifiedPageSize(Integer.MAX_VALUE+1);
		assertUnreadable(config, accessor);
	}
	
	@Test
	public void testRefuseBadMagic() throws IOException {
		ZKArchiveConfig modified = new ZKArchiveConfig(accessor, config.archiveId, true, Key.blank(config.getCrypto())) {
			@Override
			protected byte[] serializeSecurePortion() {
				byte[] serialized = super.serializeSecurePortion();
				serialized[0] ^= 0x01;
				return serialized;
			}
		};
		
		modified.write();
		assertUnreadable(config, accessor);
		modified.close();
	}
	
	@Test
	public void testRefuseBadArchivePubKey() throws IOException {
		ZKArchiveConfig modified = new ZKArchiveConfig(accessor, config.archiveId, true, Key.blank(config.getCrypto())) {
			@Override
			protected byte[] serializeSeedPortion() {
				byte[] fakePubKey = accessor.master.crypto.makePrivateSigningKey(new byte[32]).publicKey().getBytes();
				return ByteBuffer.wrap(super.serializeSeedPortion()).put(fakePubKey).array();
			}
		};
		
		modified.write();
		assertUnreadable(config);
		modified.close();
	}
	
	@Test
	public void testRefuseBadIV() throws IOException {
		ZKArchiveConfig modified = new ZKArchiveConfig(accessor, config.archiveId, true, Key.blank(config.getCrypto())) {
			@Override
			public void write() throws IOException {
				configFileIv[0] ^= 0x01;
				super.write();
			}
		};
		
		modified.write();
		assertUnreadable(config);
		modified.close();
	}
	
	@Test
	public void testRefuseCorruptedIVWriteSide() throws IOException {
		ZKArchiveConfig modified = new ZKArchiveConfig(accessor, config.archiveId, true, Key.blank(config.getCrypto())) {
			@Override
			public void write() throws IOException {
				configFileIv[0] ^= 0x01;
				super.write();
				configFileIv[0] ^= 0x01;
				
				ByteBuffer buf = ByteBuffer.wrap(storage.read(Page.pathForTag(tag())));
				buf.put(configFileIv);
				storage.write(Page.pathForTag(tag()), buf.array());
			}
		};
		
		modified.write();
		assertUnreadable(config);
		modified.close();
	}

	@Test
	public void testRefuseCorruptedIVReadSide() throws IOException {
		ZKArchiveConfig modified = new ZKArchiveConfig(accessor, config.archiveId, true, Key.blank(config.getCrypto())) {
			@Override
			public void write() throws IOException {
				super.write();
				configFileIv[0] ^= 0x01;
				
				ByteBuffer buf = ByteBuffer.wrap(storage.read(Page.pathForTag(tag())));
				buf.put(configFileIv);
				storage.write(Page.pathForTag(tag()), buf.array());
			}
		};
		
		modified.write();
		assertUnreadable(config);
		modified.close();
	}

	@Test
	public void testRefuseShortSeedPortion() throws IOException {
		Key key = new Key(master.crypto);
		ZKArchiveConfig modified = new ZKArchiveConfig(accessor, "", ZKArchive.DEFAULT_PAGE_SIZE, key, key) {
			@Override
			protected int seedPortionPadSize() {
				return super.seedPortionPadSize()-1;
			}
		};
		
		modified.write();
		assertUnreadable(modified);
		modified.close();
	}
	
	@Test
	public void testRefuseLongSeedPortion() throws IOException {
		Key key = new Key(master.crypto);
		ZKArchiveConfig modified = new ZKArchiveConfig(accessor, "", ZKArchive.DEFAULT_PAGE_SIZE, key, key) {
			@Override
			protected int seedPortionPadSize() {
				return super.seedPortionPadSize()+1;
			}
		};
		
		modified.write();
		assertUnreadable(modified);
		modified.close();
	}
	
	@Test
	public void testRefuseModified() throws IOException {
		byte[] raw = config.archive.storage.read(Page.pathForTag(config.tag()));		
		int[] steps = {0, config.archive.crypto.symIvLength(), config.seedPortionPadSize(), -1};
		int offset = 0;
		for(int step : steps) {
			if(step < 0) offset = -step;
			else offset += step;
			
			raw[offset] ^= 0x01;
			config.archive.storage.write(Page.pathForTag(config.tag()), raw);
			raw[offset] ^= 0x01;
			assertUnreadable(config);
		}
	}
	
	@Test
	public void testGetArchiveId() {
		assertTrue(Arrays.equals(config.getArchiveId(), config.calculateArchiveId(config.deriveArchiveFingerprint())));
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
		assertTrue(Arrays.equals(config.deriveKey(0, 0, 0).getRaw(), accessor.deriveKey(0, 0, 0).getRaw()));
		assertTrue(Arrays.equals(config.deriveKey(2, 0, 0).getRaw(), accessor.deriveKey(2, 0, 0).getRaw()));
		assertTrue(Arrays.equals(config.deriveKey(0, 1, 0).getRaw(), accessor.deriveKey(0, 1, 0).getRaw()));
		assertTrue(Arrays.equals(config.deriveKey(0, 0, 1).getRaw(), accessor.deriveKey(0, 0, 1).getRaw()));
	}
	
	@Test
	public void testDeriveKeyWithSeed() {
		assertTrue(Arrays.equals(config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, 0, 0).getRaw(), seedConfig.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, 0, 0).getRaw()));
		assertTrue(Arrays.equals(config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, 0, 0).getRaw(), seedConfig.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, 0, 0).getRaw()));
	}
	
	@Test
	public void testDeriveKeyAcceptsArchiveRoot() {
		config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, 0, 0);
	}
	
	@Test
	public void testDeriveKeyForArchiveRootMatchesTestVectors() {
		class ArchiveKeyDerivationExample {
			public Key key;
			public byte[] expectedResult, tweak;
			public int type, index;
			
			public ArchiveKeyDerivationExample(String keyStr, int type, int index, String tweakStr, String expectedResultStr) {
				this.key = new Key(accessor.getMaster().getCrypto(), Util.hexToBytes(keyStr));
				this.type = type;
				this.index = index;
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
					
					Key derived = config.deriveKey(root, type, index, tweak);
					assertArrayEquals(expectedResult, derived.getRaw());
				}
			}
		}
		
		new ArchiveKeyDerivationExample("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", 0, 0, "", "7b631364edb74ad050f72914790f9ded649379120b8ae8ba80f43748714b946a").validate();
	    new ArchiveKeyDerivationExample("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", 1, 0, "", "dd45557dbc6cbd60db505fbb19c1f609e9df1d78214ba9af6155552ff58ca49e").validate();
	    new ArchiveKeyDerivationExample("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", 2, 0, "", "5fe9d3cd188c4627e6d97f5d026203b1f0c02d208ee13718559767e99f274e7a").validate();
	    new ArchiveKeyDerivationExample("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", 0, 1, "", "7b0ae3920ec7d24eddf74411d0e77be1f564216ab08965f6f0d04a6854b8ef46").validate();
	    new ArchiveKeyDerivationExample("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", 0, 0xffff, "", "749011256450328dd2eece13243f773f0d4ba701d1c7c776a6f8f7dec40d35c6").validate();
	    new ArchiveKeyDerivationExample("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", 0, 0, "10111213", "636688f05d98667950861b405cd097f33585899f50e63ae37dd53e1e150f1be3").validate();
	    new ArchiveKeyDerivationExample("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", 0x5555, 0x8888, "808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf", "8bc66d27bb9b7af2d7b425e0434c8db03bbcbde69676ef6daec056d4e2b1d1bf").validate();
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
	public void testValidatePageReturnsTrueForValidConfigPage() throws IOException {
		byte[] serialized = config.storage.read(Page.pathForTag(config.tag()));
		assertTrue(config.validatePage(config.tag(), serialized));
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
		Key authKey = config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_PAGE);
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
		Key authKey = config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_PAGE);
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
	public void testVerifyReturnsTrueIfValidFileSupplied() throws IOException {
		byte[] configData = config.storage.read(Page.pathForTag(config.tag()));
		assertTrue(config.verify(configData));
	}
	
	@Test
	public void testVerifyReturnsFalseIfVersionPortionTampered() throws IOException {
		byte[] configData = config.storage.read(Page.pathForTag(config.tag()));
		configData[0] ^= 0x01;
		assertFalse(config.verify(configData));
	}
	
	@Test
	public void testVerifyReturnsFalseIfIvPortionTampered() throws IOException {
		byte[] configData = config.storage.read(Page.pathForTag(config.tag()));
		int offset = master.crypto.hashLength();
		configData[offset] ^= 0x01;
		assertFalse(config.verify(configData));
	}
	
	@Test
	public void testVerifyReturnsFalseIfSeedPortionTampered() throws IOException {
		byte[] configData = config.storage.read(Page.pathForTag(config.tag()));
		int offset = master.crypto.hashLength();
		offset += master.crypto.symIvLength();
		configData[offset] ^= 0x01;
		assertFalse(config.verify(configData));
	}
	
	@Test
	public void testVerifyReturnsFalseIfSeedPortionForgedWithFakePubKey() throws IOException {
		byte[] configData = config.storage.read(Page.pathForTag(config.tag()));
		ByteBuffer configBuf = ByteBuffer.wrap(configData);
		
		byte[] forgedPubKey = config.getPubKey().getBytes().clone();
		forgedPubKey[4] ^= 0x20;
		ByteBuffer plaintext = ByteBuffer.allocate(master.crypto.asymPublicSigningKeySize() + 8);
		plaintext.put(forgedPubKey);
		plaintext.putLong(config.pageSize);
		
		configBuf.position(master.crypto.hashLength() + master.crypto.symIvLength());
		configBuf.put(config.accessor.configFileSeedKey.encrypt(config.configFileIv, plaintext.array(), config.seedPortionPadSize()));
		
		assertFalse(config.verify(configData));
	}

	@Test
	public void testVerifyReturnsFalseIfSeedPortionForgedWithFakePageSize() throws IOException {
		byte[] configData = config.storage.read(Page.pathForTag(config.tag()));
		ByteBuffer configBuf = ByteBuffer.wrap(configData);
		
		ByteBuffer plaintext = ByteBuffer.allocate(master.crypto.asymPublicSigningKeySize() + 8);
		plaintext.put(config.getPubKey().getBytes());
		plaintext.putLong(config.pageSize+1);
		
		configBuf.position(master.crypto.hashLength() + master.crypto.symIvLength());
		configBuf.put(config.accessor.configFileSeedKey.encrypt(config.configFileIv, plaintext.array(), config.seedPortionPadSize()));
		
		assertFalse(config.verify(configData));
	}
	
	@Test
	public void testVerifyReturnsFalseIfSecurePortionTampered() throws IOException {
		byte[] configData = config.storage.read(Page.pathForTag(config.tag()));
		int offset = master.crypto.hashLength();
		offset += master.crypto.symIvLength();
		offset += master.crypto.symPaddedCiphertextSize(config.seedPortionPadSize());
		configData[offset] ^= 0x01;
		assertFalse(config.verify(configData));
	}
	
	@Test
	public void testVerifyReturnsFalseIfSignatureTampered() throws IOException {
		byte[] configData = config.storage.read(Page.pathForTag(config.tag()));
		int offset = configData.length-1;
		configData[offset] ^= 0x01;
		assertFalse(config.verify(configData));
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
}
