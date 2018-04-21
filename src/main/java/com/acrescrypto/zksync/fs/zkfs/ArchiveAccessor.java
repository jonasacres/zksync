package com.acrescrypto.zksync.fs.zkfs;

import java.util.Collection;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;

public class ArchiveAccessor {
	public final static int ROOT_TYPE_PASSPHRASE = 0;
	public final static int ROOT_TYPE_SEED = 1;
	
	protected Key seedRoot, seedId, seedRegId, configFileKey;
	protected byte[] configFileTag;
	
	public interface ArchiveAccessorDiscoveryCallback {
		void discoveredArchive(ZKArchive archive);
	}
	
	public ArchiveAccessor(Key root, int type) {
		derive(root, type);
		discoverArchives();
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
	
	protected void discoverArchives() {
		// TODO: start DHT discovery
		// TODO: discover from existing peers
		// TODO: discover from filesystem
	}
	
	public Collection<ZKArchive> knownArchives() {
		// TODO: accumulate discovered archives into an archive
		return null;
	}
}
