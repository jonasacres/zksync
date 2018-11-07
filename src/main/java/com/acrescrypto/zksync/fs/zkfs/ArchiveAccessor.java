package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
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
	
	public final static int KEY_TYPE_CIPHER = 0;
	public final static int KEY_TYPE_AUTH = 1;
	public final static int KEY_TYPE_ROOT = 2;

	public final static int KEY_INDEX_ARCHIVE = 0;
	public final static int KEY_INDEX_LOCAL = 1;
	public final static int KEY_INDEX_PAGE = 2;
	public final static int KEY_INDEX_REVISION = 3;
	public final static int KEY_INDEX_CONFIG_FILE = 4;
	public final static int KEY_INDEX_REVISION_LIST = 5;
	public final static int KEY_INDEX_REVISION_TREE = 6;
	public final static int KEY_INDEX_SEED = 7;
	public final static int KEY_INDEX_SEED_REG = 8;
	public final static int KEY_INDEX_SEED_TEMPORAL = 9;
	public final static int KEY_INDEX_STORED_ACCESS = 10;
	public final static int KEY_INDEX_REFTAG = 11;
	public final static int KEY_INDEX_BLACKLIST = 12;
	public final static int KEY_INDEX_ACCUMULATOR = 13;
	public final static int KEY_INDEX_AD_IDENTITY = 14;
	public final static int KEY_INDEX_AD_ARCHIVE_ID = 15;
	public final static int KEY_INDEX_REQUEST_POOL = 16;
	public final static int KEY_INDEX_DHT_STORAGE = 17;
	public final static int KEY_INDEX_DHT_LOOKUP = 18;
	
	protected ZKMaster master;

	protected Key passphraseRoot; // derived from passphrase; used to generate seedRoot and configFileKey/Tag
	protected Key seedRoot; // derived from passphrase; used to participate in DHT and peering, cannot decipher archives
	protected Key localRoot; // derived from locally-stored entropy combined with seedRoot; encrypts user preferences and any other data not shared with peers
	
	protected Key seedId; // derived from seed root; identifies archive family (all archives bearing the same passphrase)
	protected Key seedRegId; // derived from seed root; unique identifier for registering archive family
	
	protected Key configFileKey; // derived from passphrase root; used to encrypt secure portion of configuration file
	protected Key configFileSeedKey; // derived from passphrase root; used to encrypt seed portion of configuration file
	protected Key configFileTagKey; // derived from passphrase root; used to set location in filesystem of config file
	
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
		void discoveredArchiveConfig(ZKArchiveConfig config);
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
		for(ZKArchiveConfig config : knownArchiveConfigs) {
			if(Arrays.equals(config.archiveId, archiveId)) return config;
		}
		
		ZKArchiveConfig config = new ZKArchiveConfig(this, archiveId, false, Key.blank(master.crypto));
		discoveredArchiveConfig(config);
		
		return config;
	}
	
	public void forceAdvertisement() {
		master.dhtDiscovery.forceUpdate(this);
	}
	
	public void discoveredArchiveConfig(ZKArchiveConfig config) {
		knownArchiveConfigs.remove(config);
		knownArchiveConfigs.add(config);
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
		configFileKey = null;
		type = KEY_ROOT_SEED;
	}

	public Key deriveKey(int root, int type, int index, byte[] tweak) {
		Key[] keys = { passphraseRoot, null, seedRoot, localRoot };
		if(root >= keys.length || keys[root] == null) {
			throw new IllegalArgumentException();
		}
		
		int modifier = ((type & 0xFFFF) << 16) | (index & 0xFFFF);
		return keys[root].derive(modifier, tweak);
	}
	
	public Key deriveKey(int root, int type, int index) {
		return deriveKey(root, type, index, new byte[0]);
	}	

	public byte[] temporalProof(int offset, int step, byte[] sharedSecret) {
		assert(0 <= step && step <= Byte.MAX_VALUE);
		ByteBuffer timeSecretTweak = ByteBuffer.allocate(9+sharedSecret.length);
		timeSecretTweak.put((byte) step);
		timeSecretTweak.putLong(timeSlice(offset));
		timeSecretTweak.put(sharedSecret);
		
		/* Use a garbage "proof" if we don't actually have knowledge of the passphrase key. We use a combination of
		 * time, the shared secret and a local key derivative to ensure that this garbage result "feels" like the
		 * real deal: it changes with the timestamp, it is specific to a shared secret, and cannot be distinguished
		 * from the real passphrase-derivative version unless someone knows the passphrase root. 
		 */
		int root = isSeedOnly() ? KEY_ROOT_LOCAL : KEY_ROOT_PASSPHRASE;
		return deriveKey(root, KEY_TYPE_AUTH, KEY_INDEX_SEED_TEMPORAL, timeSecretTweak.array()).getRaw();
	}
	
	protected void deriveFromPassphraseRoot(Key passphraseRoot) {
		this.passphraseRoot = passphraseRoot;
		deriveFromSeedRoot(deriveKey(KEY_ROOT_PASSPHRASE, KEY_TYPE_ROOT, KEY_INDEX_SEED));		
		configFileKey = deriveKey(KEY_ROOT_PASSPHRASE, KEY_TYPE_CIPHER, KEY_INDEX_CONFIG_FILE, new byte[0]);
	}
	
	protected void deriveFromSeedRoot(Key seedRoot) {
		this.seedRoot = seedRoot;
		seedId = deriveKey(KEY_ROOT_SEED, KEY_TYPE_AUTH, KEY_INDEX_SEED, new byte[0]);
		seedRegId = deriveKey(KEY_ROOT_SEED, KEY_TYPE_AUTH, KEY_INDEX_SEED_REG, new byte[0]);
		localRoot = deriveKey(KEY_ROOT_SEED, KEY_TYPE_ROOT, KEY_INDEX_LOCAL, master.localKey.getRaw());
		configFileTagKey = deriveKey(KEY_ROOT_SEED, KEY_TYPE_AUTH, KEY_INDEX_CONFIG_FILE, new byte[0]);
		configFileSeedKey = deriveKey(KEY_ROOT_SEED, KEY_TYPE_CIPHER, KEY_INDEX_CONFIG_FILE, new byte[0]);
	}
	
	public byte[] temporalSeedId(int offset) {
		return temporalSeedDerivative(true, offset).authenticate("dht-seed".getBytes());
	}
	
	protected Key temporalSeedKey(int offset) {
		return temporalSeedDerivative(false, offset);
	}
	
	protected Key temporalSeedDerivative(boolean isAuth, int offset) {
		ByteBuffer timeTweak = ByteBuffer.allocate(8);
		timeTweak.putLong(timeSlice(offset));
		return deriveKey(KEY_ROOT_SEED, isAuth ? KEY_TYPE_AUTH : KEY_TYPE_CIPHER, KEY_INDEX_SEED_TEMPORAL, timeTweak.array());
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
