package com.acrescrypto.zksync.net.dht;

import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor.ArchiveDiscovery;

public class DHTClient implements ArchiveDiscovery {
	protected HashSet<ArchiveAccessor> accessors = new HashSet<ArchiveAccessor>();
	protected final Logger logger = LoggerFactory.getLogger(DHTClient.class);
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
		// TODO DHT: (implement) Auto-generated method stub
		accessors.add(accessor);
	}

	@Override
	public void stopDiscoveringArchives(ArchiveAccessor accessor) {
		// TODO DHT: (implement) Auto-generated method stub
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
			try {
				
			} catch(Exception exc) {
				logger.error("DHTClient thread caught exception", exc);
			}
		}
	}
}
