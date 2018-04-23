package com.acrescrypto.zksync.net.dht;

import java.util.HashSet;

import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor.ArchiveDiscovery;

public class DHTClient implements ArchiveDiscovery {
	protected HashSet<ArchiveAccessor> accessors = new HashSet<ArchiveAccessor>();
	private static DHTClient defaultDHT;
	
	public static DHTClient defaultDHT() {
		if(defaultDHT == null) defaultDHT = new DHTClient(DHTEntry.bootstrapEntry());
		return defaultDHT;
	}
	
	public DHTClient(DHTEntry initialPeer) {
		launchDhtThread();
		searchId(initialPeer.id);
	}
	
	@Override
	public void discoverArchives(ArchiveAccessor accessor) {
		// TODO Auto-generated method stub
		accessors.add(accessor);
	}

	@Override
	public void stopDiscoveringArchives(ArchiveAccessor accessor) {
		// TODO Auto-generated method stub
		accessors.remove(accessor);
	}
	
	protected void searchId(byte[] id) {
		
	}
	
	private void launchDhtThread() {
		new Thread(() -> {
			dhtThread();
		}).start();
	}
	
	private void dhtThread() {
		while(true) {
		}
	}
}
