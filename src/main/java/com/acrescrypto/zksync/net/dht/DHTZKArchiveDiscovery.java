package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.util.HashSet;

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
	
	private Logger logger = LoggerFactory.getLogger(DHTClient.class);
	protected HashSet<ArchiveAccessor> activeDiscoveries = new HashSet<>();
	protected int discoveryIntervalMs = DEFAULT_DISCOVERY_INTERVAL_MS;
	protected int advertisementIntervalMs = DEFAULT_ADVERTISEMENT_INTERVAL_MS;
	
	public DHTZKArchiveDiscovery() {
	}

	@Override
	public synchronized void discoverArchives(ArchiveAccessor accessor) {
		activeDiscoveries.add(accessor);
		new Thread(()->discoveryThread(accessor)).start();
		new Thread(()->advertisementThread(accessor)).start();
	}

	@Override
	public synchronized void stopDiscoveringArchives(ArchiveAccessor accessor) {
		activeDiscoveries.remove(accessor);
	}

	protected void discoveryThread(ArchiveAccessor accessor) {
		while(isDiscovering(accessor)) {
			try {
				Util.blockOn(()->isDiscovering(accessor) && !accessor.getMaster().getDHTClient().initialized);
				if(isDiscovering(accessor)) {
					discover(accessor);
				}
				
				Util.sleep(discoveryIntervalMs);
			} catch(Exception exc) {
				logger.error("Caught exception in DHTZKArchiveDiscovery discovery thread", exc);
			}
		}
	}
	
	protected void advertisementThread(ArchiveAccessor accessor) {
		while(isAdvertising(accessor)) {
			try {
				Util.blockOn(()->isAdvertising(accessor) && !accessor.getMaster().getDHTClient().initialized);
				if(isAdvertising(accessor)) {
					advertise(accessor);
				}
				
				Util.sleep(advertisementIntervalMs);
			} catch(Exception exc) {
				logger.error("Caught exception on DHTZKArchiveDiscovery advertisement thread", exc);
			}
		}
	}
	
	public synchronized boolean isDiscovering(ArchiveAccessor accessor) {
		return activeDiscoveries.contains(accessor);
	}
	
	public synchronized boolean isAdvertising(ArchiveAccessor accessor) {
		return isDiscovering(accessor); // might make discovery and advertisement separate someday, but not today
	}
	
	protected void discover(ArchiveAccessor accessor) {
		DHTID searchId = new DHTID(accessor.temporalSeedId(accessor.timeSliceIndex()));
		accessor.getMaster().getDHTClient().lookup(searchId, (record)->{
			if(!(record instanceof DHTAdvertisementRecord)) return;
			DHTAdvertisementRecord adRecord = (DHTAdvertisementRecord) record;
			if(!(adRecord.ad instanceof TCPPeerAdvertisement)) return;
			TCPPeerAdvertisement ad = (TCPPeerAdvertisement) adRecord.ad;
			
			try {
				byte[] archiveId = ZKArchiveConfig.decryptArchiveId(accessor, ad.getPubKey().getBytes(), ad.getEncryptedArchiveId());
				ZKArchiveConfig config = accessor.discoveredArchiveId(archiveId);
				config.getSwarm().addPeerAdvertisement(ad);
			} catch (IOException exc) {
				logger.warn("Caught IOException processing TCP ad", exc);
			}
		});
	}
	
	protected void advertise(ArchiveAccessor accessor) {
		for(ZKArchiveConfig config : accessor.knownArchiveConfigs()) {
			if(!config.isInitialized()) continue;
			if(accessor.getMaster().getTCPListener() == null) continue;
			TCPPeerAdvertisementListener listener = accessor.getMaster().getTCPListener().listenerForSwarm(config.getSwarm());
			if(listener == null) continue;
			
			TCPPeerAdvertisement ad;
			try {
				ad = listener.localAd();
			} catch (UnconnectableAdvertisementException e) {
				return; // socket not bound yet
			}
			DHTAdvertisementRecord adRecord = new DHTAdvertisementRecord(accessor.getMaster().getCrypto(), ad);

			for(int i = -1; i <= 1; i++) {
				DHTID searchId = new DHTID(accessor.temporalSeedId(i));
				accessor.getMaster().getDHTClient().addRecord(searchId, adRecord);
			}
		}
	}
}
