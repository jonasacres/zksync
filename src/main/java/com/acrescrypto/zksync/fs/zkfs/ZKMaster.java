package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
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
import com.acrescrypto.zksync.fs.zkfs.config.ConfigDefaults;
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

/***
 * 
 * @author jonas
 * 
 * This class is the entry-point into running EasySafe. When bootstrapping, this is the first class to
 * instantiate.
 *
 */
public class ZKMaster implements ArchiveAccessorDiscoveryCallback, AutoCloseable {
	public final static String TEST_VOLUME = "test";
	public final static String KEYFILE = "keyfile";
	
	protected CryptoSupport crypto; // wrapper for low-level cryptographic operations (hashes, symmetric, asymmetric, etc.)
	protected FS storage; // underlying host storage. This is what we'll use to write all of our archive pages, config files, etc.
	protected PassphraseProvider passphraseProvider; // used to read archive passphrases 
	protected StoredAccess storedAccess; // stores archive info like passphrases so we can reconnect at start 
	protected Key localKey; // User-specific key used to encrypt local sensitive info (e.g. for StoredAccress)
	protected LinkedList<ArchiveAccessor> accessors = new LinkedList<>();
	protected LinkedList<ZKArchiveConfig> allConfigs = new LinkedList<>();
	protected Blacklist blacklist; // Peers that we will not exchange information with
	protected static Logger logger = LoggerFactory.getLogger(ZKMaster.class);
	protected TCPPeerSocketListener listener; // Handles TCP connections for archive-specific traffic
	protected DHTClient dhtClient; // Manages the UDP-based DHT network stuff, wherein we advertise/find advertisements for archives
	protected DHTZKArchiveDiscovery dhtDiscovery; // Tells the DHTClient what archives we're advertising or seeking advertisements for
	protected ThreadGroup threadGroup; // Used to manage various threads during execution
	protected ConfigFile globalConfig; // User-specific configuration settings
	protected long debugTime = -1;
	protected String name; // Name for this instance (gets used in debugging)
	
	private BandwidthMonitor bandwidthMonitorTx; // Monitors total network traffic being transmitted
	private BandwidthMonitor bandwidthMonitorRx; // Monitors total network traffic being received
	private BandwidthAllocator bandwidthAllocatorTx; // Regulates total network traffic being transmitted
	private BandwidthAllocator bandwidthAllocatorRx; // Regulates total network traffic being received
	
	// TODO Someday: (refactor) this is really test code, which shouldn't be in here.
	public static PassphraseProvider demoPassphraseProvider() {
		return (String reason) -> {
			if(reason.contains("writing")) return "write".getBytes();
			return "zksync".getBytes();
		};
	}
	
	/** Instantiate a ZKMaster backed by a RAMFS without clearing anything that is already there. */
	public static ZKMaster openTestVolume() throws IOException {
		return openTestVolume(demoPassphraseProvider(), TEST_VOLUME);
	}
	
	/** Instantiate a ZKMaster backed by a RAMFS with the default test volume name. Any existing data is deleted. */
	public static ZKMaster openBlankTestVolume() throws IOException {
		return openBlankTestVolume(TEST_VOLUME);
	}
	
	/** Instantiate a ZKMaster backed by a RAMFS with a specified volume name. Any existing data for that volume name is deleted. */
	public static ZKMaster openBlankTestVolume(String name) throws IOException {
		RAMFS.removeVolume(name);
		return openTestVolume(demoPassphraseProvider(), name);
	}
	
	/** Instantiate a ZKMaster backed by a RAMFS with a specified passphrase provider and volume name. Any existing data for that volume name is NOT deleted. */
	public static ZKMaster openTestVolume(PassphraseProvider ppProvider, String name) throws IOException {
		try {
			return new ZKMaster(CryptoSupport.defaultCrypto(), RAMFS.volumeWithName(name), ppProvider);
		} catch (InvalidBlacklistException e) {
			// InvalidBlacklistException masked as a runtime to avoid having to add a bunch of throws InBlEx to a zillion tests
			throw new RuntimeException();
		}
	}
	
	/** Instantiate a ZKMaster backed by a specified filesystem. */
	public static ZKMaster open(PassphraseProvider ppProvider, FS storage) throws IOException {
		try {
			return new ZKMaster(CryptoSupport.defaultCrypto(), storage, ppProvider);
		} catch (InvalidBlacklistException e) {
			throw new RuntimeException();
		}
	}
	
	/** Instantiate a ZKMaster with storage on the host filesystem at a specified path. */
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
	
	/** Construct a ZKMaster backed at a specific FS instance. It is advised to use one of the static methods rather than invoking
	 *  this constructor directly.
	 *  
	 * @param crypto Object implementing cryptographic primitives to be used by this ZKMaster instance
	 * @param storage FS instance reflecting where files will be stored. It is assumed that this filesystem is appropriately scoped so that the root directory ("/") points to the actual location where files should be written, and all files and directories in the filesystem are presumed to be managed by this ZKMaster.
	 * @param passphraseProvider Used to read passphrases for deriving encryption keys, or other sensitive information 
	 * @throws IOException
	 * @throws InvalidBlacklistException
	 */
	public ZKMaster(CryptoSupport crypto, FS storage, PassphraseProvider passphraseProvider) throws IOException, InvalidBlacklistException {
	    this.crypto             = crypto;
	    this.storage            = storage;

	    this.globalConfig       = new ConfigFile(storage, "config.json");
	    globalConfig.apply(ConfigDefaults.getActiveDefaults());
	    setupSubscriptions();
	    setupBandwidth();

	    this.crypto.setMaxSimultaneousArgon2(globalConfig.getInt("crypto.pbkdf.maxsimultaneous"));
	    this.passphraseProvider = passphraseProvider;
	    this.threadGroup        = new ThreadGroup("ZKMaster " + System.identityHashCode(this));

	    getLocalKey();

	    this.storedAccess       = new StoredAccess(this);
	    this.blacklist          = new Blacklist(storage, "blacklist", localKey.derive("easysafe-blacklist"));
	    this.dhtClient          = new DHTClient(localKey.derive("easysafe-dht-storage"), this);
	    this.dhtDiscovery       = new DHTZKArchiveDiscovery(
	                                    globalConfig.getInt("net.dht.discoveryintervalms"),
	                                    globalConfig.getInt("net.dht.advertisementintervalms"));
	    listener                = new TCPPeerSocketListener(this);
	    loadStoredAccessors();
	}
	
	/** Read the local storage key using the PassphraseProvider. */
	public void getLocalKey() throws IOException {
		logger.info("Setting up local key...");
		Key ppKey;
		do {
			// Continue reading until we receive a key that decrypts existing storage (if any).
			byte[] passphrase = passphraseProvider.requestPassphrase("ZKSync storage passphrase");
			if(passphrase == null) {
				// The possibility of an infinite loop exists here if a passphrase provider always returns null. Refactor if that comes up.
				logger.info("No local passphrase provided; using default key.");
				ppKey = new Key(crypto, crypto.makeSymmetricKey(crypto.symNonce(0)));
			} else {
				ppKey = new Key(crypto, crypto.deriveKeyFromPassphrase(passphrase, CryptoSupport.PASSPHRASE_SALT_LOCAL));
			}
		} while(!attemptPassphraseKey(ppKey));
	}
	
	/** Closes all open network and filesystem stuff. */
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
		
		for(ZKArchiveConfig config : allConfigs) {
		    config.close();
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
	
	/** Delete all existing stored configurations, accessors, encrypted filesystems, etc. */
	public void purge() throws IOException {
		if(storage.exists("/")) storage.rmrf("/");
	}
	
	/** Create an archive with default settings and a specified passphrase. */
	public ZKArchive createDefaultArchive(byte[] passphrase) throws IOException {
		ArchiveAccessor accessor = makeAccessorForPassphrase(passphrase);
		ZKArchiveConfig config = ZKArchiveConfig.createDefault(accessor);
		return config.archive;
	}
	
	/** Create an archive with default settings, using a passphrase to be read from the passphrase provider. */
	public ZKArchive createDefaultArchive() throws IOException {
		byte[] passphrase = passphraseProvider.requestPassphrase("Passphrase for new archive");
		return createDefaultArchive(passphrase);
	}
	
	/** Create an archive with specified settings, using a passphrase to be read from the passphrase provider.
	 * 
	 * @param pageSize Size of individual filesystem pages, in bytes.
	 * @param description Human-readable description of this archive.
	 * @return Instantiated ZKArchive object
	 * @throws IOException
	 */
	public ZKArchive createArchive(int pageSize, String description) throws IOException {
		byte[] passphrase = passphraseProvider.requestPassphrase("Passphrase for new archive '" + description + "'");
		return createArchiveWithPassphrase(pageSize, description, passphrase);
	}
	
	/** Create an archive with specified settings and a specified passphrase. The read and write key for this
	 * archive will be identical.
	 * 
	 * @param pageSize Size of individual filesystem pages, in bytes.
	 * @param description Human-readable description of this archive.
	 * @return Instantiated ZKArchive object
	 * @throws IOException
	 */
	public ZKArchive createArchiveWithPassphrase(int pageSize, String description, byte[] readPassphrase) throws IOException {
		ArchiveAccessor accessor = makeAccessorForPassphrase(readPassphrase);
		ZKArchiveConfig config = ZKArchiveConfig.create(accessor, description, pageSize);
		return config.archive;
	}
	
	/** Create an archive with specified settings, and potentially distinct read and write passphrases to be read from the
	 * passphrase provider.
	 * 
	 * @param pageSize Size of individual filesystem pages, in bytes.
	 * @param description Human-readable description of this archive.
	 * @return Instantiated ZKArchive object
	 * @throws IOException
	 */
	public ZKArchive createArchiveWithWriteRoot(int pageSize, String description) throws IOException {
		byte[] passphrase = passphraseProvider.requestPassphrase("Passphrase for reading new archive '" + description + "'");
		byte[] writePassphrase = passphraseProvider.requestPassphrase("Passphrase for writing new archive '" + description + "'");
		Key writeRoot = new Key(crypto, crypto.deriveKeyFromPassphrase(writePassphrase, CryptoSupport.PASSPHRASE_SALT_WRITE));
		
		ArchiveAccessor accessor = makeAccessorForPassphrase(passphrase);
		ZKArchiveConfig config = ZKArchiveConfig.create(accessor, description, pageSize, accessor.passphraseRoot, writeRoot);
		return config.archive;
	}

	/** Create an archive with specified settings, and potentially distinct read and write passphrases.
	 * 
	 * @param pageSize Size of individual filesystem pages, in bytes.
	 * @param description Human-readable description of this archive.
	 * @param readPassphrase Passphrase used to derive read key
	 * @param writePassphrase Passphrase used to derive write key
	 * @return Instantiated ZKArchive object
	 * @throws IOException
	 */
	public ZKArchive createArchiveWithPassphrase(int pageSize, String description, byte[] readPassphrase, byte[] writePassphrase) throws IOException {
		// TODO API: (test) createArchiveWithPassphrase
		Key writeRoot = new Key(crypto, crypto.deriveKeyFromPassphrase(writePassphrase, CryptoSupport.PASSPHRASE_SALT_WRITE));
		Key readRoot = new Key(crypto, crypto.deriveKeyFromPassphrase(readPassphrase, CryptoSupport.PASSPHRASE_SALT_READ));
		return createArchiveWithWriteRoot(pageSize, description, readRoot, writeRoot);
	}
	
	/** Create an archive with specified settings, and potentially distinct read and write passphrases.
	 * 
	 * @param pageSize Size of individual filesystem pages, in bytes.
	 * @param description Human-readable description of this archive.
	 * @param readRoot Derived read root key
	 * @param writeRoot Derived write root key
	 * @return Instantiated ZKArchive object
	 * @throws IOException
	 */
	public ZKArchive createArchiveWithWriteRoot(int pageSize, String description, Key readRoot, Key writeRoot) throws IOException {
		ArchiveAccessor accessor = makeAccessorForRoot(readRoot, false);
		ZKArchiveConfig config = ZKArchiveConfig.create(accessor, description, pageSize, accessor.passphraseRoot, writeRoot);
		return config.archive;
	}
	
	/** Path to appropriate storage location for a given archive ID, relative to storage filesystem root */
	public String storagePathForArchiveId(byte[] archiveId) {
		return "archives/" + Util.bytesToHex(archiveId);
	}

	/** FS object rooted at appropriate storage location for page data for archive ID (i.e. root directory of this filesystem points to where the archive data should live) */
	public FS storageFsForArchiveId(byte[] archiveId) throws IOException {
		return storage.scopedFS(storagePathForArchiveId(archiveId));
	}
	
	/** FS object rooted at appropriate storage location for miscellaneous scratch data not tied to a specific archive or other use */
	public FS scratchStorage() throws IOException {
		return storage.scopedFS("scratch");
	}
	
	/** Retrieve existing accessor for a given root key, or null if no such accessor is presently stored.
	 * Specified root key can be either a passphrase root, or a seed root. */
	public synchronized ArchiveAccessor accessorForRoot(Key rootKey) {
		for(ArchiveAccessor accessor : accessors) {
			if(!accessor.isSeedOnly()) {
				if(accessor.passphraseRoot.equals(rootKey)) return accessor;
			}
			
			if(accessor.seedRoot.equals(rootKey)) return accessor;
		}
		
		return null;
	}
	
	/** Returns an accessor for a given read key derived from the specified passphrase.
	 * If an accessor already exists with this read key, then the existing accessor is returned.
	 * If an accessor exists with the seed key corresponding to this read key, then that existing accessor is removed and replaced with a new accessor containing this read key, and the new accessor is returned.
	 * If no matching accessor exists, an entirely new accessor is created with this read key, and that new accessor is added to the list of accessors and returned.
	 * */
	public ArchiveAccessor makeAccessorForPassphrase(byte[] passphrase) {
		byte[] passphraseRootRaw = crypto.deriveKeyFromPassphrase(passphrase, CryptoSupport.PASSPHRASE_SALT_READ);
		Key passphraseRoot = new Key(crypto, passphraseRootRaw);
		return makeAccessorForRoot(passphraseRoot, false);
	}
	
	/** Returns an accessor for a given root key.
	 * @param rootKey The root key to be used for the new accessor.
	 * @param isSeed Set true if rootKey represents a seed key; false if the rootKey represents a read key.
	 * 
	 * If an accessor exists with the specified root key (either as a seed key if isSeed is set true, or a read key if isSeed is set false), that accessor is returned.
	 * If isSeed is false and an accessor exists with the seed key corresponding to this read key, then that existing accessor is removed and replaced with a new accessor containing this read key, and the new accessor is returned.
	 * If no matching accessor exists, an entirely new accessor is created with this read key, and that new accessor is added to the list of accessors and returned.
	 * */
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
	
	/** FS object rooted at appropriate storage location for unshared local data corresponding archive ID (i.e. root directory of this filesystem points to where the local archive data should live) */
	public FS localStorageFsForArchiveId(byte[] archiveId) throws IOException {
		long tag = Util.shortTag(localKey.authenticate(archiveId));
		return storage.scopedFS("local/" + String.format("%016x", tag));
	}
	
	/** Called to notify ZKMaster that we have discovered a new archive, either by local instantiation, through DHT discovery, or API access.
	 * 
	 * @return New ZKArchiveConfig if this discovered config has an archive we did not previously have, or has read access to an archive we preivously only had seed access to. Otherwise, returns existing ZKArchiveConfig. 
	 * */
	@Override
	public ZKArchiveConfig discoveredArchiveConfig(ZKArchiveConfig config) {
		for(ZKArchiveConfig existing : allConfigs) {
			if(Arrays.equals(existing.archiveId, config.archiveId)) {
				if(!existing.accessor.isSeedOnly()) {
					return existing; // already have full access to this archive
				} else if(config.accessor.isSeedOnly()) {
					return existing; // no point in replacing one seed-only version with another
				}
				
				// replace seed-only with full access
				allConfigs.remove(existing);
			}
		}
		
		allConfigs.add(config);
		return config;
	}
	
	/** Notify ZKMaster that a given ZKArchiveConfig should be removed from the list of known configs. */
	public void removedArchiveConfig(ZKArchiveConfig config) {
		allConfigs.remove(config);
	}
	
	/** Return current list of all known ZKArchiveConfigs. */
	public Collection<ZKArchiveConfig> allConfigs() {
		return allConfigs;
	}
	
	public DHTClient getDHTClient() {
		return dhtClient;
	}
	
	@Deprecated
	/** @deprecated use net.dht.enabled config property instead for production use */
	public void activateDHTForTest(String address, int port, DHTPeer root) throws SocketException, UnknownHostException {
		// this basically exists just to avoid rewriting all the tests now.
		DHTPeer localizedRoot = new DHTPeer(dhtClient, root);
		dhtClient.addPeer(localizedRoot);
		dhtClient.listen(address, port);
	}
	
	/** Read StoredAccessRecords from storage, which have ArchiveAccessor/ZKArchiveConfig data in encrypted form. */
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
	
	/** Attempt to decrypt local keyfile using supplied passphrase key. Creates new keyfile if
	 * none exists.
	 * 
	 * @return false if keyfile exists and could not be decrypted using supplied key; true otherwise.
	 * */
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
	
	/** Tear down the existing dhtClient, and re-initialize. */
	public void regenerateDHTClient() throws IOException {
		dhtClient.purge();

		Key key = localKey.derive("easysafe-dht-storage");
		this.dhtClient = new DHTClient(key, this);
	}

	public ConfigFile getGlobalConfig() {
		return globalConfig;
	}
	
	/** Set up subscriptions for changes to configuration values that affect ZKMaster. */
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
	
	/** Initiialize bandwidth monitors and allocators. */
	protected void setupBandwidth() {
		bandwidthMonitorTx   = new BandwidthMonitor  (100, 3000);
		bandwidthMonitorRx   = new BandwidthMonitor  (100, 3000);
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
