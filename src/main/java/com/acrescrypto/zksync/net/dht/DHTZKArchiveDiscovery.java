package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor.ArchiveDiscovery;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.net.TCPPeerAdvertisement;
import com.acrescrypto.zksync.net.TCPPeerAdvertisementListener;
import com.acrescrypto.zksync.utility.Util;

public class DHTZKArchiveDiscovery implements ArchiveDiscovery {
	public final static int DEFAULT_DISCOVERY_INTERVAL_MS     = 1000*60*5;
	public final static int DEFAULT_ADVERTISEMENT_INTERVAL_MS = 1000*60*5;
	
	class DiscoveryEntry {
		ArchiveAccessor accessor;
		int count;
		long nextSendTime;
		
		public DiscoveryEntry(ArchiveAccessor accessor) {
			this.accessor = accessor;
		}
		
		public synchronized void sendImmediately() {
			nextSendTime = 0;
			this.notifyAll();
		}
		
		public synchronized void resetSendTime() {
			nextSendTime = Util.currentTimeMillis() + advertisementIntervalMs;
		}
		
		public boolean canSend() {
			return Util.currentTimeMillis() >= nextSendTime;
		}
		
		public synchronized boolean increment() {
			count++;
			return count == 1;
		}
		
		public synchronized boolean decrement() {
			count--;
			return count <= 0;
		}
		
		@Override
		public boolean equals(Object o) {
			// TODO API: (coverage) method / check if this is even necessary
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
	
	private Logger logger = LoggerFactory.getLogger(DHTZKArchiveDiscovery.class);
	protected HashMap<ArchiveAccessor,DiscoveryEntry> activeDiscoveries = new HashMap<>();
	protected int discoveryIntervalMs;
	protected int advertisementIntervalMs;
	public DHTZKArchiveDiscovery() {
		this(DEFAULT_DISCOVERY_INTERVAL_MS, DEFAULT_ADVERTISEMENT_INTERVAL_MS);
	}
	
	public DHTZKArchiveDiscovery(int discoveryIntervalMs, int advertisementIntervalMs) {
		this.discoveryIntervalMs = discoveryIntervalMs;
		this.advertisementIntervalMs = advertisementIntervalMs;
	}

	@Override
	public synchronized void discoverArchives(ArchiveAccessor accessor) {
		if(!activeDiscoveries.containsKey(accessor)) {
			activeDiscoveries.put(accessor, new DiscoveryEntry(accessor));
		}
		
		DiscoveryEntry entry = activeDiscoveries.get(accessor);
		if(entry.increment()) {
			logger.info("DHT -: Starting threads for accessor with temporal seed ID {}",
					Util.bytesToHex(accessor.temporalSeedId(0)));
			new Thread(accessor.getThreadGroup(), ()->discoveryThread(entry)).start();
			new Thread(accessor.getThreadGroup(), ()->advertisementThread(entry)).start();
		}
	}

	@Override
	public synchronized void stopDiscoveringArchives(ArchiveAccessor accessor) {
		DiscoveryEntry entry = activeDiscoveries.get(accessor);
		if(entry == null) return;
		if(entry.decrement()) {
			// TODO API: (test) Test increment/decrement-based deletions
			activeDiscoveries.remove(accessor);
			synchronized(entry) {
				entry.notifyAll();
			}
		}
	}

	protected void discoveryThread(DiscoveryEntry entry) {
		Util.setThreadName("DHTZKArchiveDiscovery discovery thread");
		logger.info("DHT -: Starting discovery thread for accessor with temporal seed ID {}",
				Util.bytesToHex(entry.accessor.temporalSeedId(0)));
		while(isDiscovering(entry.accessor)) {
			try {
				Util.blockOnPoll(()->isDiscovering(entry.accessor) && !entry.accessor.getMaster().getDHTClient().isInitialized());
				if(isDiscovering(entry.accessor)) {
					discover(entry);
				}
				
				synchronized(entry) {
					entry.wait(discoveryIntervalMs);
				}
			} catch(Exception exc) {
				logger.error("DHT -: Caught exception in DHTZKArchiveDiscovery discovery thread", exc);
			}
		}
		
		logger.info("DHT -: Stopping discovery thread for accessor with temporal seed ID {}",
				Util.bytesToHex(entry.accessor.temporalSeedId(0)));
	}
	
	protected void advertisementThread(DiscoveryEntry entry) {
		Util.setThreadName("DHTZKArchiveDiscovery advertisement thread");
		logger.info("DHT -: Starting advertisement thread for accessor with temporal seed ID {}, isAdvertising={}, dhtClientIsInitialized={}",
				Util.bytesToHex(entry.accessor.temporalSeedId(0)),
				isAdvertising(entry.accessor),
				entry.accessor.getMaster().getDHTClient().isInitialized());
		
		while(isAdvertising(entry.accessor)) {
			try {
				Util.blockOnPoll( () ->
					   isAdvertising(entry.accessor)
					&& !entry.accessor.getMaster().getDHTClient().isInitialized()
				  );
				if(isAdvertising(entry.accessor) && entry.canSend()) {
					advertise(entry);
					entry.resetSendTime();
				}
				
				synchronized(entry) {
					entry.wait(1);
				}
			} catch(Exception exc) {
				logger.error("DHT -: Caught exception on DHTZKArchiveDiscovery advertisement thread", exc);
			}
		}
	}
	
	public synchronized boolean isDiscovering(ArchiveAccessor accessor) {
		return activeDiscoveries.containsKey(accessor);
	}
	
	public synchronized boolean isAdvertising(ArchiveAccessor accessor) {
		return isDiscovering(accessor); // might make discovery and advertisement separate someday, but not today
	}
	
	public void forceUpdate(ArchiveAccessor accessor) {
		DiscoveryEntry entry = activeDiscoveries.get(accessor);
		if(entry == null) {
			logger.info("DHT -: Cannot force update for non-advertised archive accessor");
			return;
		}
		
		logger.info("DHT -: forcing update for archive accessor with temporal ID {}",
				Util.bytesToHex(accessor.temporalSeedId(0)));
		entry.sendImmediately();
	}
	
	protected void discover(DiscoveryEntry entry) {
		Key lookupKey = entry.accessor.deriveKey(ArchiveAccessor.KEY_ROOT_SEED,
				"easysafe-dht-lookup");
		DHTID searchId = DHTID.withBytes(entry.accessor.temporalSeedId(0));
		logger.debug("DHT -: Doing discovery for archive with temporal seed ID {}",
				Util.bytesToHex(entry.accessor.temporalSeedId(0), 8));
		entry.accessor.getMaster().getDHTClient().getProtocolManager().lookup(searchId, lookupKey, (record)->{
			if(!(record instanceof DHTAdvertisementRecord)) return;
			DHTAdvertisementRecord adRecord = (DHTAdvertisementRecord) record;
			if(!(adRecord.ad instanceof TCPPeerAdvertisement)) return;
			TCPPeerAdvertisement ad = (TCPPeerAdvertisement) adRecord.ad;
			
			try {
				byte[] archiveId = ZKArchiveConfig.decryptArchiveId(entry.accessor, ad.getPubKey().getBytes(), ad.getEncryptedArchiveId());
				ZKArchiveConfig config = entry.accessor.discoveredArchiveId(archiveId);
				config.getSwarm().addPeerAdvertisement(ad);
			} catch (IOException exc) {
				logger.warn("DHT -: Caught IOException processing TCP ad", exc);
			}
		});
	}
	
	protected void advertise(DiscoveryEntry entry) {
		if(!entry.accessor.getMaster().getTCPListener().isListening()) {
			logger.debug("DHT -: Not advertising archive with temporal seed ID {}, since TCP listener is not active",
					Util.bytesToHex(entry.accessor.temporalSeedId(0), 8));
			return;
		}
		
		for(ZKArchiveConfig config : entry.accessor.knownArchiveConfigs()) {
			logger.debug("DHT -: Advertising archive with temporal archive ID {}",
					Util.bytesToHex(config.getAccessor().temporalSeedId(0), 8));
			if(!config.isAdvertising()) continue;
			TCPPeerAdvertisementListener listener = entry.accessor.getMaster().getTCPListener().listenerForSwarm(config.getSwarm());
			if(listener == null) continue;
			
			TCPPeerAdvertisement ad;
			try {
				ad = listener.localAd();
			} catch (UnconnectableAdvertisementException e) {
				return; // socket not bound yet
			}
			
			DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(entry.accessor.getMaster().getCrypto(), ad);
			Key lookupKey = entry.accessor.deriveKey(ArchiveAccessor.KEY_ROOT_SEED,
					"easysafe-dht-lookup");
			
			for(int i = -1; i <= 1; i++) {
				DHTID searchId = DHTID.withBytes(entry.accessor.temporalSeedId(i));
				entry.accessor.getMaster().getDHTClient().getProtocolManager().addRecord(
						searchId,
						lookupKey,
						adRecord
					);
			}
		}
	}
	
	
	public void setDiscoveryIntervalMs(int discoveryIntervalMs) {
		this.discoveryIntervalMs = discoveryIntervalMs;
	}
	
	public int getDiscoveryIntervalMs() {
		return discoveryIntervalMs;
	}
	
	public void setAdvertisementIntervalMs(int advertisementIntervalMs) {
		this.advertisementIntervalMs = advertisementIntervalMs;
	}
	
	public int getAdvertisementIntervalMs() {
		return advertisementIntervalMs;
	}
}
