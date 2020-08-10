package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.utility.Util;

public class ArchiveAccessor {
	public final static int TEMPORAL_SEED_KEY_INTERVAL_MS = 1000*60*60*3; // Rotate temporal seed IDs every 3 hours

	public final static int KEY_ROOT_PASSPHRASE = 0;
	public final static int KEY_ROOT_ARCHIVE = 1;
	public final static int KEY_ROOT_SEED = 2;
	public final static int KEY_ROOT_LOCAL = 3;
	public final static int KEY_ROOT_WRITE = 4;
	
	public final static int KEY_TYPE_CIPHER = 0;
	public final static int KEY_TYPE_AUTH = 1;
	public final static int KEY_TYPE_ROOT = 2;

	protected ZKMaster master;

	protected Key passphraseRoot; // derived from passphrase; used to generate seedRoot and configFileKey/Tag
	protected Key seedRoot; // derived from passphrase; used to participate in DHT and peering, cannot decipher archives
	protected Key localRoot; // derived from locally-stored entropy combined with seedRoot; encrypts user preferences and any other data not shared with peers
	
	protected Key seedId; // derived from seed root; identifies archive family (all archives bearing the same passphrase)
	
	protected int type; // KEY_ROOT_PASSPHRASE or KEY_ROOT_SEED

	protected HashSet<ArchiveDiscovery> discoveryMethods = new HashSet<ArchiveDiscovery>();
	protected ArrayList<ZKArchiveConfig> knownArchiveConfigs = new ArrayList<ZKArchiveConfig>();
	protected ArrayList<ArchiveAccessorDiscoveryCallback> callbacks = new ArrayList<ArchiveAccessorDiscoveryCallback>();
	protected ThreadGroup threadGroup;
	
	public interface ArchiveDiscovery {
		void discoverArchives(ArchiveAccessor accessor);
		void stopDiscoveringArchives(ArchiveAccessor accessor);
	}
	
	public interface ArchiveAccessorDiscoveryCallback {
		ZKArchiveConfig discoveredArchiveConfig(ZKArchiveConfig config);
	}

	public ArchiveAccessor(ZKMaster master, Key root, int type) {
		this.master = master;
		this.type = type;
		this.threadGroup = new ThreadGroup(master.threadGroup, "ArchiveAccessor " + System.identityHashCode(this));
		
		switch(type) {
		case KEY_ROOT_PASSPHRASE:
			deriveFromPassphraseRoot(root);
			break;
		case KEY_ROOT_SEED:
			deriveFromSeedRoot(root);
			break;
		default:
			throw new RuntimeException("Invalid key type " + type);
		}
	}
	
	// useful for building test cases
	public ArchiveAccessor(ZKMaster master, ArchiveAccessor existing) {
		this.master = master;
		this.type = existing.type;
		this.threadGroup = new ThreadGroup(master.threadGroup, "ArchiveAccessor " + System.identityHashCode(this));
		deriveFromPassphraseRoot(existing.passphraseRoot);
	}
	
	public ArchiveAccessor discoverOnDHT() {
		master.dhtDiscovery.discoverArchives(this);
		return this;
	}
	
	public ZKArchiveConfig discoveredArchiveId(byte[] archiveId) throws IOException {
		synchronized(this) {
			for(ZKArchiveConfig config : knownArchiveConfigs) {
				if(Arrays.equals(config.archiveId, archiveId)) return config;
			}
		}
		
		ZKArchiveConfig config = new ZKArchiveConfig(this, archiveId, false, Key.blank(master.crypto));
		discoveredArchiveConfig(config);
		
		return config;
	}
	
	public void forceAdvertisement() {
		master.dhtDiscovery.forceUpdate(this);
	}
	
	public void discoveredArchiveConfig(ZKArchiveConfig config) {
		synchronized(this) {
			knownArchiveConfigs.remove(config);
			knownArchiveConfigs.add   (config);
		}
		
		forceAdvertisement();
		
		for(ArchiveAccessorDiscoveryCallback callback : callbacks) {
			callback.discoveredArchiveConfig(config);
		}
	}
	
	public ArchiveAccessor addDiscovery(ArchiveDiscovery discovery) {
		discoveryMethods.add(discovery);
		discovery.discoverArchives(this);
		return this;
	}
	
	public ArchiveAccessor removeDiscovery(ArchiveDiscovery discovery) {
		discoveryMethods.remove(discovery);
		discovery.stopDiscoveringArchives(this);
		return this;
	}
	
	public ArchiveAccessor removeAllDiscoveries() {
		for(ArchiveDiscovery discovery : discoveryMethods) {
			discovery.stopDiscoveringArchives(this);
		}
		
		discoveryMethods = new HashSet<ArchiveDiscovery>();
		return this;
	}
	
	public ArchiveAccessor addCallback(ArchiveAccessorDiscoveryCallback callback) {
		this.callbacks.add(callback);
		return this;
	}
	
	public ArchiveAccessor removeCallback(ArchiveAccessorDiscoveryCallback callback) {
		this.callbacks.remove(callback);
		return this;
	}
	
	public synchronized Collection<ZKArchiveConfig> knownArchiveConfigs() {
		return new ArrayList<>(knownArchiveConfigs);
	}
	
	public boolean isSeedOnly() {
		return passphraseRoot == null;
	}
	
	public ArchiveAccessor makeSeedOnly() {
		if(isSeedOnly()) return this;
		ArchiveAccessor newAccessor = new ArchiveAccessor(master, passphraseRoot, type);
		newAccessor.becomeSeedOnly();
		return newAccessor;
	}
	
	public void becomeSeedOnly() {
		passphraseRoot = null;
		type = KEY_ROOT_SEED;
	}
	
	public void setPassphraseRoot(Key passphraseRoot) {
		deriveFromPassphraseRoot(passphraseRoot);
	}
	
	public void setSeedRoot(Key seedRoot) {
		deriveFromSeedRoot(seedRoot);
	}

	public Key deriveKey(int root, String id, byte[] tweak) {
		Key[] keys = { passphraseRoot, null, seedRoot, localRoot };
		if(root >= keys.length || keys[root] == null) {
			throw new IllegalArgumentException();
		}
		
		return keys[root].derive(id, tweak);
	}
	
	public Key deriveKey(int root, String id) {
		return deriveKey(root, id, new byte[0]);
	}	

	// TODO Noise: ditch unneeded offset argument, also this is no longer a "temporal" proof
	public byte[] temporalProof(int offset, int step, byte[] sharedSecret) {
		assert(0 <= step && step <= Byte.MAX_VALUE);
		byte[] timeSecretTweak = Util.concat(Util.serializeInt(step), Util.serializeInt(offset), sharedSecret);
		
		/* Use a garbage "proof" if we don't actually have knowledge of the passphrase key. But
		 * at least keep this constant time... */
		int root = isSeedOnly() ? KEY_ROOT_LOCAL : KEY_ROOT_PASSPHRASE;
		return deriveKey(root, "easysafe-dht-temporal-proof", timeSecretTweak).getRaw();
	}
	
	protected void deriveFromPassphraseRoot(Key passphraseRoot) {
		this.passphraseRoot = passphraseRoot;
		deriveFromSeedRoot(deriveKey(KEY_ROOT_PASSPHRASE, "easysafe-seed-root"));		
	}
	
	protected void deriveFromSeedRoot(Key seedRoot) {
		this.seedRoot = seedRoot;
		seedId = deriveKey(KEY_ROOT_SEED, "easysafe-seed-id", new byte[0]);
		localRoot = deriveKey(KEY_ROOT_SEED, "easysafe-local-root", master.localKey.getRaw());
	}
	
	public byte[] temporalSeedId(int offset) {
		return master.crypto.expand(
				seedRoot.getRaw(),
				master.crypto.hashLength(),
				Util.serializeLong(timeSlice(offset)),
				"easysafe-temporal-seed-id".getBytes());
	}
	
	public int timeSliceIndex() {
		return (int) (Util.currentTimeMillis()/TEMPORAL_SEED_KEY_INTERVAL_MS);
	}
	
	public long timeSlice(int index) {
		return (long) TEMPORAL_SEED_KEY_INTERVAL_MS * (long) index;
	}

	public ZKMaster getMaster() {
		return master;
	}
	
	public Key getSeedRoot() {
		return seedRoot;
	}
	
	public synchronized ZKArchiveConfig configWithId(byte[] archiveId) {
		for(ZKArchiveConfig config : knownArchiveConfigs) {
			if(Arrays.equals(config.getArchiveId(), archiveId)) return config;
		}
		
		return null;
	}
	
	public ThreadGroup getThreadGroup() {
		return threadGroup;
	}
}
