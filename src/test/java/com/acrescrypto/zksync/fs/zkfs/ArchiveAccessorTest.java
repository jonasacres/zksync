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
		
		// Generated from test-vectors.py, Python 3.6.5, commit 60bb78be2c5359f73c465b31ead8e34ebfc8608f
		new ArchiveKeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			0x0000,
			0x0000,
			"",
			"b6abfc6470a720a02b3c11cc12d62aac86502bcc79bc13670191730695a95ff0").validate();
		new ArchiveKeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			0x0001,
			0x0000,
			"",
			"1ea8d06ffa94e7f4d34e04ac72f3ee13d8b532b83a13fe33bbecbe6ec5d57fd2").validate();
		new ArchiveKeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			0x0002,
			0x0000,
			"",
			"f0d74adb5f77df2d37adffe0541024a374922558016a210cd80259979f7f05e3").validate();
		new ArchiveKeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			0x0000,
			0x0001,
			"",
			"fdcad2202cd184924bd7911b222471320c6e4a44871eb6cafbc8435bc9eba6bd").validate();
		new ArchiveKeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			0x0000,
			0xffff,
			"",
			"9783b80dbcce3817bbb8579ff77258e37f9e32ea9c3275f60788b4e2efa1f5e7").validate();
		new ArchiveKeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			0x0000,
			0x0000,
			"10111213",
			"fcc462f1c3eb17749969104f54a82e829969a9d340746c181c3f2c2ab8c817cb").validate();
		new ArchiveKeyDerivationExample(
			"000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f",
			0x5555,
			0x8888,
			"808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf",
			"ffd291c2d5fa1f452e5a3540d60f791ea21cab6e5d90c8a0422fd4dd8002a008").validate();
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
