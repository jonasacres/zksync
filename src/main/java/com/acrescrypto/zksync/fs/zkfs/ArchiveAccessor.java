package com.acrescrypto.zksync.fs.zkfs;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.net.dht.DHTClient;

public class ArchiveAccessor {
	public final static int ROOT_TYPE_PASSPHRASE = 0;
	public final static int ROOT_TYPE_SEED = 1;
	
	public final static int TEMPORAL_SEED_KEY_INTERVAL_SECONDS = 60*60*3; // Rotate temporal seed IDs every 3 hours
	
	protected Key seedRoot, seedId, seedRegId, configFileKey;
	protected byte[] configFileTag;
	protected HashSet<ArchiveDiscovery> discoveryMethods = new HashSet<ArchiveDiscovery>();
	protected HashSet<ZKArchive> knownArchives = new HashSet<ZKArchive>();
	protected ArrayList<ArchiveAccessorDiscoveryCallback> callbacks = new ArrayList<ArchiveAccessorDiscoveryCallback>();
	
	public interface ArchiveAccessorDiscoveryCallback {
		void discoveredArchive(ZKArchive archive);
	}
	
	public interface ArchiveDiscovery {
		void discoverArchives(ArchiveAccessor accessor);
		void stopDiscoveringArchives(ArchiveAccessor accessor);
	}
	
	public ArchiveAccessor(Key root, int type) {
		derive(root, type);
	}
	
	public ArchiveAccessor addDefaultDiscoveries() {
		this.addDiscovery(DHTClient.defaultDHT());
		// TODO: filesystem discovery
		// TODO: direct peer discovery
		// TODO: mdns/udp multicast/other lan discovery?
		return this;
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
	
	protected void derive(Key root, int type) {
		switch(type) {
		case ROOT_TYPE_PASSPHRASE:
			seedRoot = root.derive(0x00, new byte[0]);
			configFileKey = root.derive(0x01, new byte[0]);
			configFileTag = root.derive(0x02, new byte[0]).getRaw();
		case ROOT_TYPE_SEED:
			seedId = seedRoot.derive(0x00, new byte[0]);
			seedRegId = seedRoot.derive(0x01, new byte[0]);
			break;
		default:
			throw new RuntimeException("Unsupported key type " + type);
		}
	}
	
	public void discoveredArchive(ZKArchive archive) {
		for(ArchiveAccessorDiscoveryCallback callback : callbacks) {
			callback.discoveredArchive(archive);
		}
	}
	
	public Key temporalSeedId(int offset) {
		return temporalSeedDerivative(0x00, offset);
	}
	
	public Key temporalSeedKey(int offset) {
		return temporalSeedDerivative(0x01, offset);
	}
	
	protected Key temporalSeedDerivative(int index, int offset) {
		ByteBuffer timeTweak = ByteBuffer.allocate(8);
		timeTweak.putLong(timeSlice(offset));
		return seedId.derive(index, timeTweak.array());
	}
	
	protected long timeSlice(int offset) {
		/* TODO: This is going to cause the DHT to get mobbed every rotation interval as everyone reregisters.
		 * Need to smear this out somehow. At any given time, we could be listening for up to 3 IDs per archive.
		 */
		return System.currentTimeMillis()/(1000*TEMPORAL_SEED_KEY_INTERVAL_SECONDS) + offset;
	}
	
	public Collection<ZKArchive> knownArchives() {
		return knownArchives;
	}
}
