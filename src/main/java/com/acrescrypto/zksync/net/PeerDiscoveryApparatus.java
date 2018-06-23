package com.acrescrypto.zksync.net;

import java.util.Collection;

import com.acrescrypto.zksync.fs.zkfs.ZKArchive;

public interface PeerDiscoveryApparatus {
	
	/** List of peers discovered by this apparatus. This call must be nonblocking and side-effect free. */
	public Collection<PeerAdvertisement> discoveredPeers(ZKArchive archive);
}
