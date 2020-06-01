package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.net.SocketException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor.ArchiveAccessorDiscoveryCallback;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.net.TCPPeerSocketListener;
import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.net.dht.DHTPeer;
import com.acrescrypto.zksync.net.dht.DHTZKArchiveDiscovery;
import com.acrescrypto.zksync.utility.BandwidthAllocator;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.MemLogAppender;
import com.acrescrypto.zksync.utility.Util;

public class ZKMaster implements ArchiveAccessorDiscoveryCallback, AutoCloseable {
	public final static String TEST_VOLUME = "test";
	public final static String KEYFILE = "keyfile";
	
	protected CryptoSupport crypto;
	protected FS storage;
	protected PassphraseProvider passphraseProvider;
	protected StoredAccess storedAccess;
	protected Key localKey;
	protected LinkedList<ArchiveAccessor> accessors = new LinkedList<>();
	protected LinkedList<ZKArchiveConfig> allConfigs = new LinkedList<>();
	protected Blacklist blacklist;
	protected static Logger logger = LoggerFactory.getLogger(ZKMaster.class);
	protected TCPPeerSocketListener listener;
	protected DHTClient dhtClient;
	protected DHTZKArchiveDiscovery dhtDiscovery;
	protected ThreadGroup threadGroup;
	protected ConfigFile globalConfig;
	protected long debugTime = -1;
	protected String name;
	
	private BandwidthMonitor bandwidthMonitorTx;
	private BandwidthMonitor bandwidthMonitorRx;
	private BandwidthAllocator bandwidthAllocatorTx;
	private BandwidthAllocator bandwidthAllocatorRx;
	
	// TODO Someday: (refactor) this is really test code, which shouldn't be in here.
	public static PassphraseProvider demoPassphraseProvider() {
		return (String reason) -> {
			if(reason.contains("writing")) return "write".getBytes();
			return "zksync".getBytes();
		};
	}
	
	public static ZKMaster openTestVolume() throws IOException {
		return openTestVolume(demoPassphraseProvider(), TEST_VOLUME);
	}
	
	public static ZKMaster openBlankTestVolume() throws IOException {
		return openBlankTestVolume(TEST_VOLUME);
	}
	
	public static ZKMaster openBlankTestVolume(String name) throws IOException {
		RAMFS.removeVolume(name);
		return openTestVolume(demoPassphraseProvider(), name);
	}
	
	public static ZKMaster openTestVolume(PassphraseProvider ppProvider, String name) throws IOException {
		try {
			return new ZKMaster(CryptoSupport.defaultCrypto(), RAMFS.volumeWithName(name), ppProvider);
		} catch (InvalidBlacklistException e) {
			// InvalidBlacklistException masked as a runtime to avoid having to add a bunch of throws InBlEx to a zillion tests
			throw new RuntimeException();
		}
	}
	
	public static ZKMaster open(PassphraseProvider ppProvider, FS storage) throws IOException {
		try {
			return new ZKMaster(CryptoSupport.defaultCrypto(), storage, ppProvider);
		} catch (InvalidBlacklistException e) {
			throw new RuntimeException();
		}
	}
	
	public static ZKMaster openAtPath(PassphraseProvider ppProvider, String path) throws IOException {
		try {
			logger.info("Opening EasySafe at {}", path);
			return new ZKMaster(CryptoSupport.defaultCrypto(), new LocalFS(path), ppProvider);
		} catch (InvalidBlacklistException e) {
			// InvalidBlacklistException masked as a runtime to avoid having to add a bunch of throws InBlEx to a zillion tests
			throw new RuntimeException();
		}
	}
	
	protected ZKMaster() {}
	
	public ZKMaster(CryptoSupport crypto, FS storage, PassphraseProvider passphraseProvider) throws IOException, InvalidBlacklistException {
		this.crypto = crypto;
		this.storage = storage;
		
		this.globalConfig = new ConfigFile(storage, "config.json");
		setupDefaultConfig();
		setupSubscriptions();
		setupBandwidth();
		
		this.crypto.setMaxSimultaneousArgon2(globalConfig.getInt("crypto.pbkdf.maxsimultaneous"));
		this.passphraseProvider = passphraseProvider;
		this.threadGroup = new ThreadGroup("ZKMaster " + System.identityHashCode(this));
		getLocalKey();
		this.storedAccess = new StoredAccess(this);
		this.blacklist = new Blacklist(storage, "blacklist", localKey.derive("easysafe-blacklist"));
		this.dhtClient = new DHTClient(localKey.derive("easysafe-dht-storage"), this);
		this.dhtDiscovery = new DHTZKArchiveDiscovery(
				globalConfig.getInt("net.dht.discoveryintervalms"),
				globalConfig.getInt("net.dht.advertisementintervalms"));
		listener = new TCPPeerSocketListener(this);
		loadStoredAccessors();
	}
	
	public void getLocalKey() throws IOException {
		logger.info("Setting up local key...");
		Key ppKey;
		do {
			byte[] passphrase = passphraseProvider.requestPassphrase("ZKSync storage passphrase");
			if(passphrase == null) {
				logger.info("No local passphrase provided; using default key.");
				ppKey = new Key(crypto, crypto.makeSymmetricKey(crypto.symNonce(0)));
			} else {
				ppKey = new Key(crypto, crypto.deriveKeyFromPassphrase(passphrase, CryptoSupport.PASSPHRASE_SALT_LOCAL));
			}
		} while(!attemptPassphraseKey(ppKey));
	}
	
	public void close() {
		storedAccess.close();
		dhtClient.close();
		if(listener != null) {
			try {
				listener.close();
			} catch (IOException exc) {
				logger.error("Caught exception closing TCP listener", exc);
			}
		}
	}
	
	public Blacklist getBlacklist() {
		return blacklist;
	}
	
	public CryptoSupport getCrypto() {
		return crypto;
	}
	
	// Expect this to be deprecated someday.
	public TCPPeerSocketListener getTCPListener() {
		return listener;
	}
	
	public FS getStorage() {
		return storage;
	}
	
	public void purge() throws IOException {
		if(storage.exists("/")) storage.rmrf("/");
	}
	
	public ZKArchive createDefaultArchive(byte[] passphrase) throws IOException {
		ArchiveAccessor accessor = makeAccessorForPassphrase(passphrase);
		ZKArchiveConfig config = ZKArchiveConfig.createDefault(accessor);
		return config.archive;
	}
	
	public ZKArchive createDefaultArchive() throws IOException {
		byte[] passphrase = passphraseProvider.requestPassphrase("Passphrase for new archive");
		return createDefaultArchive(passphrase);
	}
	
	public ZKArchive createArchive(int pageSize, String description) throws IOException {
		byte[] passphrase = passphraseProvider.requestPassphrase("Passphrase for new archive '" + description + "'");
		return createArchiveWithPassphrase(pageSize, description, passphrase);
	}
	
	public ZKArchive createArchiveWithPassphrase(int pageSize, String description, byte[] readPassphrase) throws IOException {
		ArchiveAccessor accessor = makeAccessorForPassphrase(readPassphrase);
		ZKArchiveConfig config = ZKArchiveConfig.create(accessor, description, pageSize);
		return config.archive;
	}
	
	public ZKArchive createArchiveWithWriteRoot(int pageSize, String description) throws IOException {
		byte[] passphrase = passphraseProvider.requestPassphrase("Passphrase for reading new archive '" + description + "'");
		byte[] writePassphrase = passphraseProvider.requestPassphrase("Passphrase for writing new archive '" + description + "'");
		Key writeRoot = new Key(crypto, crypto.deriveKeyFromPassphrase(writePassphrase, CryptoSupport.PASSPHRASE_SALT_WRITE));
		
		ArchiveAccessor accessor = makeAccessorForPassphrase(passphrase);
		ZKArchiveConfig config = ZKArchiveConfig.create(accessor, description, pageSize, accessor.passphraseRoot, writeRoot);
		return config.archive;
	}
	
	public ZKArchive createArchiveWithPassphrase(int pageSize, String description, byte[] readPassphrase, byte[] writePassphrase) throws IOException {
		// TODO API: (test) createArchiveWithPassphrase
		Key writeRoot = new Key(crypto, crypto.deriveKeyFromPassphrase(writePassphrase, CryptoSupport.PASSPHRASE_SALT_READ));
		Key readRoot = new Key(crypto, crypto.deriveKeyFromPassphrase(readPassphrase, CryptoSupport.PASSPHRASE_SALT_WRITE));
		return createArchiveWithWriteRoot(pageSize, description, readRoot, writeRoot);
	}
	
	public ZKArchive createArchiveWithWriteRoot(int pageSize, String description, Key readRoot, Key writeRoot) throws IOException {
		ArchiveAccessor accessor = makeAccessorForRoot(readRoot, false);
		ZKArchiveConfig config = ZKArchiveConfig.create(accessor, description, pageSize, accessor.passphraseRoot, writeRoot);
		return config.archive;
	}
	
	public String storagePathForArchiveId(byte[] archiveId) {
		return "archives/" + Util.bytesToHex(archiveId);
	}

	public FS storageFsForArchiveId(byte[] archiveId) throws IOException {
		return storage.scopedFS(storagePathForArchiveId(archiveId));
	}
	
	public FS scratchStorage() throws IOException {
		return storage.scopedFS("scratch");
	}
	
	public synchronized ArchiveAccessor accessorForRoot(Key rootKey) {
		for(ArchiveAccessor accessor : accessors) {
			if(!accessor.isSeedOnly()) {
				if(accessor.passphraseRoot.equals(rootKey)) return accessor;
			}
			
			if(accessor.seedRoot.equals(rootKey)) return accessor;
		}
		
		return null;
	}
	
	public ArchiveAccessor makeAccessorForPassphrase(byte[] passphrase) {
		byte[] passphraseRootRaw = crypto.deriveKeyFromPassphrase(passphrase, CryptoSupport.PASSPHRASE_SALT_READ);
		Key passphraseRoot = new Key(crypto, passphraseRootRaw);
		return makeAccessorForRoot(passphraseRoot, false);
	}
	
	public synchronized ArchiveAccessor makeAccessorForRoot(Key rootKey, boolean isSeed) {
		ArchiveAccessor accessor = accessorForRoot(rootKey);
		if(accessor != null && isSeed == accessor.isSeedOnly()) {
			return accessor;
		}
		
		accessor = new ArchiveAccessor(this, rootKey, isSeed ? ArchiveAccessor.KEY_ROOT_SEED : ArchiveAccessor.KEY_ROOT_PASSPHRASE);
		accessor.addCallback(this);
		ArchiveAccessor existing = accessorForRoot(accessor.seedRoot);
		if(existing != null) {
			accessors.remove(existing);
		}
		accessors.add(accessor);
		return accessor;
	}
	
	public FS localStorageFsForArchiveId(byte[] archiveId) throws IOException {
		long tag = Util.shortTag(localKey.authenticate(archiveId));
		return storage.scopedFS("local/" + String.format("%016x", tag));
	}

	@Override
	public void discoveredArchiveConfig(ZKArchiveConfig config) {
		for(ZKArchiveConfig existing : allConfigs) {
			if(Arrays.equals(existing.archiveId, config.archiveId)) {
				if(!existing.accessor.isSeedOnly()) {
					return; // already have full access to this archive
				} else if(config.accessor.isSeedOnly()) {
					return; // no point in replacing one seed-only version with another
				}
				
				// replace seed-only with full access
				allConfigs.remove(existing);
			}
		}
		
		allConfigs.add(config);
	}
	
	public void removedArchiveConfig(ZKArchiveConfig config) {
		allConfigs.remove(config);
	}
	
	public Collection<ZKArchiveConfig> allConfigs() {
		return allConfigs;
	}
	
	public DHTClient getDHTClient() {
		return dhtClient;
	}

	protected void loadStoredAccessors() {
		try {
			storedAccess.read();
		} catch (SecurityException exc) {
			logger.warn("Security error reading stored accessors; initializing as blank", exc);
		} catch (IOException e) {
		}
	}
	
	public StoredAccess storedAccess() {
		return storedAccess;
	}
	
	protected boolean attemptPassphraseKey(Key ppKey) throws IOException {
		MutableSecureFile keyFile = MutableSecureFile.atPath(storage, KEYFILE, ppKey);
		if(storage.exists(KEYFILE)) {
			try {
				localKey = new Key(crypto, keyFile.read());
				assert(localKey.getRaw().length == crypto.symKeyLength());
				logger.info("Successfully decrypted key file with passphrase");
				return true;
			} catch(SecurityException exc) {
				// TODO API: (coverage) branch
				logger.warn("Supplied passphrase did not match key file");
				return false;
			}
		} else {
			// TODO Someday: (design) it'd be nice to ask for a passphrase confirmation here...
			logger.info("No keyfile found; creating...");
			localKey = new Key(crypto, crypto.rng(crypto.symKeyLength()));
			keyFile.write(localKey.getRaw(), 512);
			logger.info("Wrote key to {}", keyFile.getPath());
			return true;
		}
	}

	@Deprecated // test purposes only
	public void setDHTClient(DHTClient dhtClient) {
		this.dhtClient = dhtClient;
	}

	public ThreadGroup getThreadGroup() {
		return threadGroup;
	}
	
	public DHTZKArchiveDiscovery getDHTDiscovery() {
		return dhtDiscovery;
	}

	public BandwidthMonitor getBandwidthMonitorTx() {
		return bandwidthMonitorTx;
	}

	public void setBandwidthMonitorTx(BandwidthMonitor bandwidthMonitorTx) {
		this.bandwidthMonitorTx = bandwidthMonitorTx;
	}

	public BandwidthMonitor getBandwidthMonitorRx() {
		return bandwidthMonitorRx;
	}

	public void setBandwidthMonitorRx(BandwidthMonitor bandwidthMonitorRx) {
		this.bandwidthMonitorRx = bandwidthMonitorRx;
	}

	public BandwidthAllocator getBandwidthAllocatorTx() {
		return bandwidthAllocatorTx;
	}

	public void setBandwidthAllocatorTx(BandwidthAllocator bandwidthAllocatorTx) {
		this.bandwidthAllocatorTx = bandwidthAllocatorTx;
	}

	public BandwidthAllocator getBandwidthAllocatorRx() {
		return bandwidthAllocatorRx;
	}

	public void setBandwidthAllocatorRx(BandwidthAllocator bandwidthAllocatorRx) {
		this.bandwidthAllocatorRx = bandwidthAllocatorRx;
	}

	public void regenerateDHTClient() throws IOException {
		dhtClient.purge();

		Key key = localKey.derive("easysafe-dht-storage");
		this.dhtClient = new DHTClient(key, this);
	}

	public ConfigFile getGlobalConfig() {
		return globalConfig;
	}
	
	protected void setupDefaultConfig() {
		globalConfig.setDefault("crypto.pbkdf.maxsimultaneous", 1);
		
		globalConfig.setDefault("net.dht.enabled", true);
		globalConfig.setDefault("net.dht.bindaddress", "0.0.0.0");
		globalConfig.setDefault("net.dht.port", 0);
		globalConfig.setDefault("net.dht.upnp", false);
		globalConfig.setDefault("net.dht.network", "easysafe");
		
		globalConfig.setDefault("net.dht.bootstrap.enabled", true);
		globalConfig.setDefault("net.dht.bootstrap.host", "dht1.easysafe.io");
		globalConfig.setDefault("net.dht.bootstrap.port", 49921);
		globalConfig.setDefault("net.dht.bootstrap.key", "+y+OStBfRsEE3L51aVoJIHX6C+FYcBYN+MVqz+/zFXE=");
		
		globalConfig.setDefault("net.dht.discoveryintervalms", DHTZKArchiveDiscovery.DEFAULT_DISCOVERY_INTERVAL_MS);
		globalConfig.setDefault("net.dht.advertisementintervalms", DHTZKArchiveDiscovery.DEFAULT_ADVERTISEMENT_INTERVAL_MS);
		
		globalConfig.setDefault("net.swarm.enabled", false);
		globalConfig.setDefault("net.swarm.bindaddress", "0.0.0.0");
		globalConfig.setDefault("net.swarm.backlog", 50);
		globalConfig.setDefault("net.swarm.port", 0);
		globalConfig.setDefault("net.swarm.upnp", false);
		globalConfig.setDefault("net.swarm.maxOpenMessages", 16);
		globalConfig.setDefault("net.swarm.rejectionCacheSize", 16);
		globalConfig.setDefault("net.swarm.pageSendAvailabilityTimeoutMs", 1000);
		
		globalConfig.setDefault("fs.default.fileMode", 0644);
		globalConfig.setDefault("fs.default.username", "root");
		globalConfig.setDefault("fs.default.uid", 0);
		globalConfig.setDefault("fs.default.groupname", "root");
		globalConfig.setDefault("fs.default.gid", 0);
		globalConfig.setDefault("fs.default.directoryMode", 0755);
		globalConfig.setDefault("fs.settings.maxOpenBlocks", 4);
		
		globalConfig.setDefault("fs.settings.pageReadyMaxRetries", 50);
		globalConfig.setDefault("fs.settings.pageReadyRetryDelayMs", 10);
		globalConfig.setDefault("fs.settings.pageTreeChunkCacheSize", 16);
		globalConfig.setDefault("fs.settings.directoryCacheSize", 128);
		globalConfig.setDefault("fs.settings.inodeTablePageCacheSize", 16);
		globalConfig.setDefault("fs.settings.revisionTreeCacheSize", 256);
		globalConfig.setDefault("fs.settings.readOnlyFilesystemCacheSize", 64);
		globalConfig.setDefault("fs.settings.mergeRevisionAcquisitionMaxWaitMs", 30000);
		globalConfig.setDefault("fs.settings.automergeDelayMs", 10000);
		globalConfig.setDefault("fs.settings.maxAutomergeDelayMs", 60000);
		globalConfig.setDefault("fs.settings.maxAutomergeAcquireWaitTimeMs", 60000);
		globalConfig.setDefault("fs.settings.revtagHasLocalCacheTimeout", 60000);
		
		globalConfig.setDefault("fs.settings.mirror.pathSquelchPeriodMs", 100);
		globalConfig.setDefault("fs.settings.mirror.zkfsToHostSyncDelayMs", 100);
		globalConfig.setDefault("fs.settings.mirror.zkfsToHostSyncMaxDelayMs", 1000);
		
		globalConfig.setDefault("fs.fileHandleTelemetry", FS.fileHandleTelemetryEnabled);
		
		globalConfig.setDefault("net.limits.tx", -1);
		globalConfig.setDefault("net.limits.rx", -1);
		
		globalConfig.setDefault("log.includeLogRequests", false);
		globalConfig.setDefault("log.historyDepth", MemLogAppender.sharedInstance().getHistoryDepth());
		globalConfig.setDefault("log.threshold", MemLogAppender.sharedInstance().getThreshold());
	}
	
	protected void setupSubscriptions() {
		globalConfig.subscribe("fs.fileHandleTelemetry").asBoolean((enabled)->FS.fileHandleTelemetryEnabled = enabled);
		globalConfig.subscribe("net.limits.tx").asLong((v)->bandwidthAllocatorTx.setBytesPerSecond(v));
		globalConfig.subscribe("net.limits.rx").asLong((v)->bandwidthAllocatorRx.setBytesPerSecond(v));
		
		globalConfig.subscribe("crypto.pbkdf.maxsimultaneous").asInt((v)->crypto.setMaxSimultaneousArgon2(v));
		
		globalConfig.subscribe("net.dht.discoveryintervalms").asInt((v)->dhtDiscovery.setDiscoveryIntervalMs(v));
		globalConfig.subscribe("net.dht.advertisementintervalms").asInt((v)->dhtDiscovery.setAdvertisementIntervalMs(v));
		
		globalConfig.subscribe("log.historyDepth").asInt((depth)->MemLogAppender.sharedInstance().setHistoryDepth(depth));
		globalConfig.subscribe("log.threshold").asInt((threshold)->MemLogAppender.sharedInstance().setThreshold(threshold));
	}
	
	protected void setupBandwidth() {
		bandwidthMonitorTx = new BandwidthMonitor(100, 3000);
		bandwidthMonitorRx = new BandwidthMonitor(100, 3000);
		bandwidthAllocatorTx = new BandwidthAllocator(globalConfig.getLong("net.limits.tx"));
		bandwidthAllocatorRx = new BandwidthAllocator(globalConfig.getLong("net.limits.rx"));
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
