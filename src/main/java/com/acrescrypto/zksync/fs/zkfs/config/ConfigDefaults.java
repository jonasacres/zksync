package com.acrescrypto.zksync.fs.zkfs.config;

import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.net.dht.DHTZKArchiveDiscovery;
import com.acrescrypto.zksync.utility.MemLogAppender;

public class ConfigDefaults {
	protected static ConfigFile activeDefaults;
	
	public static ConfigFile getActiveDefaults() {
		if(activeDefaults == null) {
			resetDefaults();
		}
		
		return activeDefaults;
	}
	
	public static void resetDefaults() {
		activeDefaults = initBaseDefaults();
	}
	
	public static ConfigFile initBaseDefaults() {
		ConfigFile config = new ConfigFile();
		
		config.setDefault("crypto.pbkdf.maxsimultaneous", 1);
		
		config.setDefault("net.dht.enabled",           true);
		config.setDefault("net.dht.bindaddress",  "0.0.0.0");
		config.setDefault("net.dht.localaddress", "0.0.0.0");
		config.setDefault("net.dht.port",                 0);
		config.setDefault("net.dht.upnp",             false);
		config.setDefault("net.dht.network",     "easysafe");
		
		config.setDefault("net.dht.bootstrap.enabled",   true);
		config.setDefault("net.dht.bootstrap.peerfile",  "http://dht1.easysafe.io/dht/peerfile");
		
		config.setDefault("net.dht.discoveryintervalms",      DHTZKArchiveDiscovery.DEFAULT_DISCOVERY_INTERVAL_MS);
		config.setDefault("net.dht.advertisementintervalms",  DHTZKArchiveDiscovery.DEFAULT_ADVERTISEMENT_INTERVAL_MS);
		
		config.setDefault("net.swarm.enabled",                              false);
		config.setDefault("net.swarm.bindaddress",                      "0.0.0.0");
		config.setDefault("net.swarm.backlog",                                 50);
		config.setDefault("net.swarm.port",                                     0);
		config.setDefault("net.swarm.upnp",                                 false);
		config.setDefault("net.swarm.maxOpenMessages",                         16);
		config.setDefault("net.swarm.rejectionCacheSize",                      16);
		config.setDefault("net.swarm.pageSendAvailabilityTimeoutMs",         1000);
		
		config.setDefault("fs.default.fileMode",                             0644);
		config.setDefault("fs.default.username",                           "root");
		config.setDefault("fs.default.uid",                                     0);
		config.setDefault("fs.default.groupname",                          "root");
		config.setDefault("fs.default.gid",                                     0);
		config.setDefault("fs.default.directoryMode",                        0755);
		config.setDefault("fs.settings.maxOpenBlocks",                          4);
		
		config.setDefault("fs.settings.pageReadyMaxRetries",                   50);
		config.setDefault("fs.settings.pageReadyRetryDelayMs",                 10);
		config.setDefault("fs.settings.pageTreeChunkCacheSize",                16);
		config.setDefault("fs.settings.directoryCacheSize",                   128);
		config.setDefault("fs.settings.inodeTablePageCacheSize",               16);
		config.setDefault("fs.settings.revisionTreeCacheSize",                256);
		config.setDefault("fs.settings.readOnlyFilesystemCacheSize",           64);
		config.setDefault("fs.settings.mergeRevisionAcquisitionMaxWaitMs",  30000);
		config.setDefault("fs.settings.automergeDelayMs",                   10000);
		config.setDefault("fs.settings.maxAutomergeDelayMs",                60000);
		config.setDefault("fs.settings.maxAutomergeAcquireWaitTimeMs",      60000);
		config.setDefault("fs.settings.revtagHasLocalCacheTimeout",         60000);
		
		config.setDefault("fs.settings.mirror.pathSquelchPeriodMs",           100);
		config.setDefault("fs.settings.mirror.zkfsToHostSyncDelayMs",         100);
		config.setDefault("fs.settings.mirror.zkfsToHostSyncMaxDelayMs",     1000);
		
		config.setDefault("fs.fileHandleTelemetry", FS.fileHandleTelemetryEnabled);
		
		config.setDefault("net.limits.tx",                                     -1);
		config.setDefault("net.limits.rx",                                     -1);
		
		config.setDefault("log.includeLogRequests", false);
		config.setDefault("log.historyDepth",       MemLogAppender.sharedInstance().getHistoryDepth());
		config.setDefault("log.threshold",          MemLogAppender.sharedInstance().getThreshold());
		
		return config;
	}
	
	private ConfigDefaults() {
	}
}