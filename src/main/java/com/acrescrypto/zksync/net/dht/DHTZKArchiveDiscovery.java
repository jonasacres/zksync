package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor.ArchiveDiscovery;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.net.TCPPeerAdvertisement;
import com.acrescrypto.zksync.net.TCPPeerAdvertisementListener;
import com.acrescrypto.zksync.utility.Util;

public class DHTZKArchiveDiscovery implements ArchiveDiscovery {
	public final static int DEFAULT_DISCOVERY_INTERVAL_MS = 1000*60*5;
	public final static int DEFAULT_ADVERTISEMENT_INTERVAL_MS = 1000*60*5;
	
	class DiscoveryEntry {
		ArchiveAccessor accessor;
		
		public DiscoveryEntry(ArchiveAccessor accessor) {
			this.accessor = accessor;
		}
		
		@Override
		public boolean equals(Object o) {
			if(o instanceof DiscoveryEntry) {
				return accessor.equals(((DiscoveryEntry) o).accessor);
			}
			
			return false;
		}
		
		@Override
		public int hashCode() {
			return accessor.hashCode();
		}
	}
	
	private Logger logger = LoggerFactory.getLogger(DHTClient.class);
	protected HashMap<ArchiveAccessor,DiscoveryEntry> activeDiscoveries = new HashMap<>();
	protected int discoveryIntervalMs = DEFAULT_DISCOVERY_INTERVAL_MS;
	protected int advertisementIntervalMs = DEFAULT_ADVERTISEMENT_INTERVAL_MS;
	
	public DHTZKArchiveDiscovery() {
	}

	@Override
	public synchronized void discoverArchives(ArchiveAccessor accessor) {
		DiscoveryEntry entry = new DiscoveryEntry(accessor);
		activeDiscoveries.put(accessor, entry);
		new Thread(()->discoveryThread(entry)).start();
		new Thread(()->advertisementThread(entry)).start();
	}

	@Override
	public synchronized void stopDiscoveringArchives(ArchiveAccessor accessor) {
		activeDiscoveries.remove(accessor);
	}

	protected void discoveryThread(DiscoveryEntry entry) {
		Thread.currentThread().setName("DHTZKArchiveDiscovery discovery thread");
		while(isDiscovering(entry.accessor)) {
			try {
				Util.blockOn(()->isDiscovering(entry.accessor) && !entry.accessor.getMaster().getDHTClient().initialized);
				if(isDiscovering(entry.accessor)) {
					discover(entry);
				}
				
				synchronized(entry) {
					entry.wait(discoveryIntervalMs);
				}
			} catch(Exception exc) {
				logger.error("Caught exception in DHTZKArchiveDiscovery discovery thread", exc);
			}
		}
	}
	
	protected void advertisementThread(DiscoveryEntry entry) {
		Thread.currentThread().setName("DHTZKArchiveDiscovery advertisement thread");
		while(isAdvertising(entry.accessor)) {
			try {
				Util.blockOn(()->isAdvertising(entry.accessor) && !entry.accessor.getMaster().getDHTClient().initialized);
				if(isAdvertising(entry.accessor)) {
					advertise(entry);
				}
				
				synchronized(entry) {
					entry.wait(advertisementIntervalMs);
				}
			} catch(Exception exc) {
				logger.error("Caught exception on DHTZKArchiveDiscovery advertisement thread", exc);
			}
		}
	}
	
	public synchronized boolean isDiscovering(ArchiveAccessor accessor) {
		return activeDiscoveries.containsKey(accessor);
	}
	
	public synchronized boolean isAdvertising(ArchiveAccessor accessor) {
		return isDiscovering(accessor); // might make discovery and advertisement separate someday, but not today
	}
	
	// TODO DHT: (test) test forceUpdate
	public void forceUpdate(ArchiveAccessor accessor) {
		DiscoveryEntry entry = activeDiscoveries.get(accessor);
		if(entry == null) return;
		synchronized(entry) {
			entry.notifyAll();
		}
	}
	
	protected void discover(DiscoveryEntry entry) {
		DHTID searchId = new DHTID(entry.accessor.temporalSeedId(0));
		entry.accessor.getMaster().getDHTClient().lookup(searchId, (record)->{
			if(!(record instanceof DHTAdvertisementRecord)) return;
			DHTAdvertisementRecord adRecord = (DHTAdvertisementRecord) record;
			if(!(adRecord.ad instanceof TCPPeerAdvertisement)) return;
			TCPPeerAdvertisement ad = (TCPPeerAdvertisement) adRecord.ad;
			
			try {
				byte[] archiveId = ZKArchiveConfig.decryptArchiveId(entry.accessor, ad.getPubKey().getBytes(), ad.getEncryptedArchiveId());
				ZKArchiveConfig config = entry.accessor.discoveredArchiveId(archiveId);
				config.getSwarm().addPeerAdvertisement(ad);
			} catch (IOException exc) {
				logger.warn("Caught IOException processing TCP ad", exc);
			}
		});
	}
	
	protected void advertise(DiscoveryEntry entry) {
		for(ZKArchiveConfig config : entry.accessor.knownArchiveConfigs()) {
			if(!config.isInitialized()) continue;
			if(entry.accessor.getMaster().getTCPListener() == null) continue;
			TCPPeerAdvertisementListener listener = entry.accessor.getMaster().getTCPListener().listenerForSwarm(config.getSwarm());
			if(listener == null) continue;
			
			TCPPeerAdvertisement ad;
			try {
				ad = listener.localAd();
			} catch (UnconnectableAdvertisementException e) {
				return; // socket not bound yet
			}
			DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(entry.accessor.getMaster().getCrypto(), ad);
			
			for(int i = -1; i <= 1; i++) {
				DHTID searchId = new DHTID(entry.accessor.temporalSeedId(i));
				entry.accessor.getMaster().getDHTClient().addRecord(searchId, adRecord);
			}
		}
	}
}
