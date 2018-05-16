package com.acrescrypto.zksync.fs.zkfs;

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
import org.junit.Ignore;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor.ArchiveAccessorDiscoveryCallback;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor.ArchiveDiscovery;

public class ArchiveAccessorTest {
	static ZKMaster master;
	Key key, seedKey;
	ArchiveAccessor accessor, seedAccessor;
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		master = ZKMaster.openAtPath((String reason) -> { return "zksync".getBytes(); }, "/tmp/zksync-archive-accessor");
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
		master.purge();
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Test
	public void testTimeslice() {
		int interval = ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS;
		int currentOffset = (int) (System.currentTimeMillis() / interval);
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
		assertNotNull(seedAccessor.configFileTag);
		assertNotNull(seedAccessor.configFileSeedKey);
	}
	
	@Test
	public void testPassphraseDerivation() {
		assertTrue(Arrays.equals(key.getRaw(), accessor.passphraseRoot.getRaw()));
		assertNotNull(seedAccessor.seedRoot);
		assertNotNull(seedAccessor.seedId);
		assertNotNull(seedAccessor.seedRegId);
		assertNotNull(seedAccessor.localRoot);
		assertNotNull(seedAccessor.configFileTag);
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
	
	@Test @Ignore
	public void testDeriveKeyMatchesTestVectors() {
		// TODO: test vectors
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
			public void discoveredArchive(ZKArchive cbArchive) {
				assertTrue(archive == cbArchive);
				holder.passed = true;
			}
		});
		accessor.discoveredArchive(archive);
		assertTrue(holder.passed);
	}
	
	@Test
	public void testRemoveCallback() throws IOException {
		ZKArchive archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		
		ArchiveAccessorDiscoveryCallback callback = new ArchiveAccessorDiscoveryCallback() {
			@Override
			public void discoveredArchive(ZKArchive cbArchive) {
				fail();
			}
		};
		
		accessor.addCallback(callback);
		accessor.removeCallback(callback);
		accessor.discoveredArchive(archive);
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
}
