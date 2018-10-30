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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor.ArchiveAccessorDiscoveryCallback;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor.ArchiveDiscovery;
import com.acrescrypto.zksync.utility.Util;

public class ArchiveAccessorTest {
	static ZKMaster master;
	Key key, seedKey;
	ArchiveAccessor accessor, seedAccessor;
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		master = ZKMaster.openBlankTestVolume();
	}
	
	@Before
	public void beforeEach() throws IOException {
		key = new Key(master.crypto, master.crypto.rng(master.crypto.symKeyLength()));
		seedKey = new Key(master.crypto, master.crypto.rng(master.crypto.symKeyLength()));
		accessor = master.makeAccessorForRoot(key, false);
		seedAccessor = master.makeAccessorForRoot(seedKey, true);
	}
	
	@AfterClass
	public static void afterAll() throws IOException {
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testTimeslice() {
		long interval = ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS;
		int currentOffset = (int) (Util.currentTimeMillis() / interval);
		long nowTimeslice = currentOffset * interval;
		assertEquals(currentOffset, accessor.timeSliceIndex());
		assertEquals(nowTimeslice, accessor.timeSlice(currentOffset));
		assertTrue(accessor.timeSlice(0) % interval == 0);
		for(int i = -3; i <= 3; i++) {
			assertEquals(i*interval, accessor.timeSlice(currentOffset+i) - accessor.timeSlice(currentOffset));
		}
	}
	
	@Test
	public void testSeedDerivations() {
		assertTrue(Arrays.equals(seedKey.getRaw(), seedAccessor.seedRoot.getRaw()));
		// we could test the contents, but it would pretty much be copy-paste from the code we're testing
		assertNotNull(seedAccessor.seedId);
		assertNotNull(seedAccessor.seedRegId);
		assertNotNull(seedAccessor.localRoot);
		assertNotNull(seedAccessor.configFileTagKey);
		assertNotNull(seedAccessor.configFileSeedKey);
	}
	
	@Test
	public void testPassphraseDerivation() {
		assertTrue(Arrays.equals(key.getRaw(), accessor.passphraseRoot.getRaw()));
		assertNotNull(seedAccessor.seedRoot);
		assertNotNull(seedAccessor.seedId);
		assertNotNull(seedAccessor.seedRegId);
		assertNotNull(seedAccessor.localRoot);
		assertNotNull(seedAccessor.configFileTagKey);
		assertNotNull(seedAccessor.configFileSeedKey);
	}
	
	@Test
	public void testTemporalProofWithPassphrase() {
		byte[] secret = { 1, 2, 3, 4 };
		for(int i = -3; i <= 3; i++) {
			for(byte step = 0; step <= 1; step++) {
				ByteBuffer tweak = ByteBuffer.allocate(8 + 1 + secret.length);
				tweak.put(step);
				tweak.putLong(accessor.timeSlice(i));
				tweak.put(secret);
				byte[] expected = accessor.deriveKey(ArchiveAccessor.KEY_ROOT_PASSPHRASE, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_SEED_TEMPORAL, tweak.array()).getRaw();
				byte[] proof = accessor.temporalProof(i, step, secret);
				assertTrue(Arrays.equals(expected, proof));
			}
		}
	}

	@Test
	public void testTemporalProofWithoutPassphrase() {
		byte[] secret = { 1, 2, 3, 4 };
		for(int i = -3; i <= 3; i++) {
			for(byte step = 0; step <= 1; step++) {
				ByteBuffer tweak = ByteBuffer.allocate(8 + 1 + secret.length);
				tweak.put(step);
				tweak.putLong(accessor.timeSlice(i));
				tweak.put(secret);
				byte[] expected = seedAccessor.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_SEED_TEMPORAL, tweak.array()).getRaw();
				byte[] proof = seedAccessor.temporalProof(i, step, secret);
				assertTrue(Arrays.equals(expected, proof));
			}
		}
	}
	
	@Test
	public void testDeriveKeyDeterministic() {
		assertTrue(Arrays.equals(accessor.deriveKey(0, 0, 0).getRaw(), accessor.deriveKey(0, 0, 0).getRaw()));
	}
	
	@Test
	public void testDeriveKeyMutatesWithRoot() {
		assertFalse(Arrays.equals(accessor.deriveKey(0, 0, 0).getRaw(), accessor.deriveKey(2, 0, 0).getRaw()));
	}
	
	@Test
	public void testDeriveKeyMutatesWithType() {
		assertFalse(Arrays.equals(accessor.deriveKey(0, 0, 0).getRaw(), accessor.deriveKey(0, 1, 0).getRaw()));
	}
	
	@Test
	public void testDeriveKeyMutatesWithIndex() {
		assertFalse(Arrays.equals(accessor.deriveKey(0, 0, 0).getRaw(), accessor.deriveKey(0, 0, 1).getRaw()));
	}
	
	@Test
	public void testDeriveKeyMatchesTestVectors() {
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
						ArchiveAccessor.KEY_ROOT_SEED,
						ArchiveAccessor.KEY_ROOT_LOCAL,
				};
				
				for(int root : roots) {
					accessor.passphraseRoot = accessor.seedRoot = accessor.localRoot = null;
					switch(root) {
					case ArchiveAccessor.KEY_ROOT_PASSPHRASE:
						accessor.passphraseRoot = key;
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
					
					Key derived = accessor.deriveKey(root, type, index, tweak);
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
	public void testDeriveKeyRejectsArchiveRoot() {
		try {
			accessor.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, 0, 0);
			fail();
		} catch(IllegalArgumentException exc) {
		}
	}
	
	@Test
	public void testIsSeedOnly() {
		assertFalse(accessor.isSeedOnly());
		assertTrue(seedAccessor.isSeedOnly());
	}
	
	@Test
	public void testBecomeSeedOnly() {
		assertFalse(accessor.isSeedOnly());
		assertNotNull(accessor.passphraseRoot);
		assertEquals(ArchiveAccessor.KEY_ROOT_PASSPHRASE, accessor.type);
		accessor.becomeSeedOnly();
		assertNull(accessor.passphraseRoot);
		assertTrue(accessor.isSeedOnly());
		assertEquals(ArchiveAccessor.KEY_ROOT_SEED, accessor.type);
	}
	
	@Test
	public void testMakeSeedOnly() {
		assertFalse(accessor.isSeedOnly());
		assertNotNull(accessor.passphraseRoot);
		ArchiveAccessor seedOnly = accessor.makeSeedOnly();
		assertNull(seedOnly.passphraseRoot);
		assertTrue(seedOnly.isSeedOnly());
		assertEquals(ArchiveAccessor.KEY_ROOT_SEED, seedOnly.type);
	}
	
	@Test
	public void testAddCallback() throws IOException {
		class Holder { boolean passed; };
		Holder holder = new Holder();
		
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		
		accessor.addCallback(new ArchiveAccessorDiscoveryCallback() {
			@Override
			public void discoveredArchiveConfig(ZKArchiveConfig config) {
				assertArrayEquals(archive.config.archiveId, config.archiveId);
				holder.passed = true;
			}
		});
		
		accessor.discoveredArchiveConfig(archive.config);
		assertTrue(holder.passed);
		
		archive.close();
	}
	
	@Test
	public void testRemoveCallback() throws IOException {
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		
		ArchiveAccessorDiscoveryCallback callback = new ArchiveAccessorDiscoveryCallback() {
			@Override
			public void discoveredArchiveConfig(ZKArchiveConfig config) {
				fail();
			}
		};
		
		accessor.addCallback(callback);
		accessor.removeCallback(callback);
		accessor.discoveredArchiveConfig(archive.config);
		
		archive.close();
	}
	
	@Test
	public void testAddDiscovery() {
		class Holder { boolean passed; };
		Holder holder = new Holder();

		ArchiveDiscovery discovery = new ArchiveDiscovery() {
			@Override
			public void discoverArchives(ArchiveAccessor cbAccessor) {
				assert(accessor == cbAccessor);
				holder.passed = true;
			}

			@Override
			public void stopDiscoveringArchives(ArchiveAccessor accessor) {
				fail();
			}
		};
		
		accessor.addDiscovery(discovery);		
		assertTrue(holder.passed);
	}
	
	@Test
	public void testRemoveDiscovery() {
		class Holder { boolean passed; };
		Holder holder = new Holder();

		ArchiveDiscovery discovery = new ArchiveDiscovery() {
			@Override
			public void discoverArchives(ArchiveAccessor cbAccessor) {
				assert(accessor == cbAccessor);
			}

			@Override
			public void stopDiscoveringArchives(ArchiveAccessor cbAccessor) {
				assert(accessor == cbAccessor);
				holder.passed = true;
			}
		};
		
		accessor.addDiscovery(discovery);
		assertFalse(holder.passed);
		accessor.removeDiscovery(discovery);
		assertTrue(holder.passed);
	}
	
	@Test
	public void testRemoveAllDiscoveries() {
		class IntHolder { int seen; };
		IntHolder added = new IntHolder();
		IntHolder removed = new IntHolder();

		ArchiveDiscovery[] discoveries = new ArchiveDiscovery[2];
		for(int i = 0; i < discoveries.length; i++) {
			discoveries[i] = new ArchiveDiscovery() {
				@Override
				public void discoverArchives(ArchiveAccessor cbAccessor) {
					assert(accessor == cbAccessor);
					added.seen++;
				}
	
				@Override
				public void stopDiscoveringArchives(ArchiveAccessor cbAccessor) {
					assert(accessor == cbAccessor);
					removed.seen++;
				}
			};
			
			accessor.addDiscovery(discoveries[i]);
		}
		
		assertEquals(discoveries.length, added.seen);
		assertEquals(0, removed.seen);
		accessor.removeAllDiscoveries();
		assertEquals(discoveries.length, removed.seen);
	}
	
	@Test
	public void testConfigWithIdReturnsNullIfNoConfigWithIdPresent() {
		assertNull(accessor.configWithId(master.getCrypto().rng(master.getCrypto().hashLength())));
	}
	
	@Test
	public void testConfigWithIdReturnsCachedConfig() throws IOException {
		ZKArchiveConfig config = ZKArchiveConfig.createDefault(accessor);
		assertEquals(config, accessor.configWithId(config.getArchiveId()));
		config.archive.close();
	}
}
