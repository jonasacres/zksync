package com.acrescrypto.zksync.fs.zkfs;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.net.dht.DHTClient;

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
	public final static int KEY_INDEX_PAGE_MERKLE = 3;
	public final static int KEY_INDEX_REVISION = 4;
	public final static int KEY_INDEX_CONFIG_FILE = 5;
	public final static int KEY_INDEX_REVISION_TREE = 6;
	public final static int KEY_INDEX_SEED = 7;
	public final static int KEY_INDEX_SEED_REG = 8;
	public final static int KEY_INDEX_SEED_TEMPORAL = 9;
	public final static int KEY_INDEX_STORED_ACCESS = 10;
	
	protected ZKMaster master;

	protected Key passphraseRoot; // derived from passphrase; used to generate seedRoot and configFileKey/Tag
	protected Key seedRoot; // derived from passphrase; used to participate in DHT and peering, cannot decipher archives
	protected Key localRoot; // derived from locally-stored entropy combined with seedRoot; encrypts user preferences and any other data not shared with peers
	
	protected Key seedId; // derived from seed root; identifies archive family (all archives bearing the same passphrase)
	protected Key seedRegId; // derived from seed root; unique identifier for registering archive family
	
	protected Key configFileKey; // derived from passphrase root; used to encrypt configuration file
	protected byte[] configFileTag; // derived from passphrase root; used to set location in filesystem of config file
	
	protected int type; // KEY_ROOT_PASSPHRASE or KEY_ROOT_SEED

	protected HashSet<ArchiveDiscovery> discoveryMethods = new HashSet<ArchiveDiscovery>();
	protected HashSet<ZKArchive> knownArchives = new HashSet<ZKArchive>();
	protected ArrayList<ArchiveAccessorDiscoveryCallback> callbacks = new ArrayList<ArchiveAccessorDiscoveryCallback>();
	
	public interface ArchiveDiscovery {
		void discoverArchives(ArchiveAccessor accessor);
		void stopDiscoveringArchives(ArchiveAccessor accessor);
	}
	
	public interface ArchiveAccessorDiscoveryCallback {
		void discoveredArchive(ZKArchive archive);
	}	

	public ArchiveAccessor(ZKMaster master, Key root, int type) {
		this.master = master;
		this.type = type;
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
	
	public ArchiveAccessor addDefaultDiscoveries() {
		this.addDiscovery(DHTClient.defaultDHT());
		// TODO P2P: direct peer discovery
		// TODO P2P: mdns/udp multicast/other lan discovery?
		return this;
	}
	
	public void discoveredArchive(ZKArchive archive) {
		if(knownArchives.contains(archive)) return;
		knownArchives.add(archive);
		for(ArchiveAccessorDiscoveryCallback callback : callbacks) {
			callback.discoveredArchive(archive);
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
	
	public Collection<ZKArchive> knownArchives() {
		return knownArchives;
	}
	
	public boolean isSeedOnly() {
		return passphraseRoot == null;
	}

	public Key deriveKey(int root, int type, int index, byte[] tweak) {
		Key[] keys = { passphraseRoot, null, seedRoot, localRoot };
		if(root >= keys.length || keys[root] == null) throw new IllegalArgumentException();
		return keys[root].derive(((type & 0xFFFF) << 16) | (index & 0xFFFF), tweak);
	}
	
	public Key deriveKey(int root, int type, int index) {
		return deriveKey(root, type, index, new byte[0]);
	}	

	public byte[] temporalProof(int step, byte[] sharedSecret) {
		assert(0 <= step && step <= Byte.MAX_VALUE);
		ByteBuffer timestamp = ByteBuffer.allocate(9+sharedSecret.length);
		timestamp.putShort((byte) step);
		timestamp.putLong(timeSlice(0));
		timestamp.put(sharedSecret);

		if(isSeedOnly()) {
			/* Use a garbage "proof" if we don't actually have knowledge of the passphrase key. We use a combination of
			 * time, the shared secret and a local key derivative to ensure that this garbage result "feels" like the
			 * real deal: it changes with the timestamp, it is specific to a shared secret, and cannot be distinguished
			 * from the real passphrase-derivative version unless someone knows the passphrase root. 
			 */
			byte[] rngSeed = localRoot.derive(0x03, timestamp.array()).getRaw();
			return master.crypto.prng(rngSeed).getBytes(master.crypto.hashLength()); // makes logic cleaner for protocol implementation
		}
		return passphraseRoot.derive(0x03, timestamp.array()).getRaw(); // TODO P2P: Evil magic number!
	}
	
	protected void deriveFromPassphraseRoot(Key passphraseRoot) {
		this.passphraseRoot = passphraseRoot;
		deriveFromSeedRoot(deriveKey(KEY_ROOT_PASSPHRASE, KEY_TYPE_ROOT, KEY_INDEX_SEED));		
		configFileKey = deriveKey(KEY_ROOT_PASSPHRASE, KEY_TYPE_CIPHER, KEY_INDEX_CONFIG_FILE, new byte[0]);
		configFileTag = deriveKey(KEY_ROOT_PASSPHRASE, KEY_TYPE_AUTH, KEY_INDEX_CONFIG_FILE, new byte[0]).getRaw();
	}
	
	protected void deriveFromSeedRoot(Key seedRoot) {
		this.seedRoot = seedRoot;
		seedId = deriveKey(KEY_ROOT_SEED, KEY_TYPE_AUTH, KEY_INDEX_SEED, new byte[0]);
		seedRegId = deriveKey(KEY_ROOT_SEED, KEY_TYPE_AUTH, KEY_INDEX_SEED_REG, new byte[0]);
		localRoot = deriveKey(KEY_ROOT_SEED, KEY_TYPE_ROOT, KEY_INDEX_LOCAL, master.localKey.getRaw());
	}
	
	protected Key temporalSeedId(int offset) {
		return temporalSeedDerivative(true, offset);
	}
	
	protected Key temporalSeedKey(int offset) {
		return temporalSeedDerivative(false, offset);
	}
	
	protected Key temporalSeedDerivative(boolean isAuth, int offset) {
		ByteBuffer timeTweak = ByteBuffer.allocate(8);
		timeTweak.putLong(timeSlice(offset));
		return deriveKey(KEY_ROOT_SEED, isAuth ? KEY_TYPE_AUTH : KEY_TYPE_CIPHER, KEY_INDEX_SEED_TEMPORAL, timeTweak.array());
	}
	
	protected long timeSlice(int offset) {
		return TEMPORAL_SEED_KEY_INTERVAL_MS * (System.currentTimeMillis()/TEMPORAL_SEED_KEY_INTERVAL_MS + offset);
	}
}
