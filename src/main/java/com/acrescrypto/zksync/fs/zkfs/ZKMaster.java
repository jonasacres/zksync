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
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.net.TCPPeerSocketListener;
import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.net.dht.DHTPeer;
import com.acrescrypto.zksync.net.dht.DHTZKArchiveDiscovery;
import com.acrescrypto.zksync.utility.Util;

public class ZKMaster implements ArchiveAccessorDiscoveryCallback {
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
	protected Logger logger = LoggerFactory.getLogger(ZKMaster.class);
	protected TCPPeerSocketListener listener;
	protected DHTClient dhtClient;
	protected DHTZKArchiveDiscovery dhtDiscovery;
	protected ThreadGroup threadGroup;
	protected long debugTime = -1;
	
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
			return new ZKMaster(new CryptoSupport(), RAMFS.volumeWithName(name), ppProvider);
		} catch (InvalidBlacklistException e) {
			// InvalidBlacklistException masked as a runtime to avoid having to add a bunch of throws InBlEx to a zillion tests
			throw new RuntimeException();
		}
	}
	
	public static ZKMaster open(PassphraseProvider ppProvider, FS storage) throws IOException {
		try {
			return new ZKMaster(new CryptoSupport(), storage, ppProvider);
		} catch (InvalidBlacklistException e) {
			throw new RuntimeException();
		}
	}
	
	public static ZKMaster openAtPath(PassphraseProvider ppProvider, String path) throws IOException {
		try {
			return new ZKMaster(new CryptoSupport(), new LocalFS(path), ppProvider);
		} catch (InvalidBlacklistException e) {
			// InvalidBlacklistException masked as a runtime to avoid having to add a bunch of throws InBlEx to a zillion tests
			throw new RuntimeException();
		}
	}
	
	protected ZKMaster() {}
	
	public ZKMaster(CryptoSupport crypto, FS storage, PassphraseProvider passphraseProvider) throws IOException, InvalidBlacklistException {
		this.crypto = crypto;
		this.storage = storage;
		this.passphraseProvider = passphraseProvider;
		this.threadGroup = new ThreadGroup("ZKMaster " + System.identityHashCode(this));
		getLocalKey();
		this.storedAccess = new StoredAccess(this);
		this.blacklist = new Blacklist(storage, "blacklist", localKey.derive(ArchiveAccessor.KEY_INDEX_BLACKLIST, "blacklist".getBytes()));
		this.dhtClient = new DHTClient(localKey.derive(ArchiveAccessor.KEY_INDEX_DHT_STORAGE, "dht-storage".getBytes()), this);
		this.dhtDiscovery = new DHTZKArchiveDiscovery();
		listener = new TCPPeerSocketListener(this);
		loadStoredAccessors();
	}
	
	public void activateDHT(String address, int port, DHTPeer root) throws SocketException {
		dhtClient.listen(address, port);
		dhtClient.addPeer(root);
		dhtClient.autoFindPeers();
	}
	
	// Expect this to be deprecated someday.
	public void listenOnTCP(int port) throws IOException {
		listener.startListening(port);
	}
	
	public void getLocalKey() throws IOException {
		Key ppKey;
		do {
			byte[] passphrase = passphraseProvider.requestPassphrase("ZKSync storage passphrase");
			ppKey = new Key(crypto, crypto.deriveKeyFromPassphrase(passphrase));
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
		Key writeRoot = new Key(crypto, crypto.deriveKeyFromPassphrase(writePassphrase));
		
		ArchiveAccessor accessor = makeAccessorForPassphrase(passphrase);
		ZKArchiveConfig config = ZKArchiveConfig.create(accessor, description, pageSize, accessor.passphraseRoot, writeRoot);
		return config.archive;
	}
	
	public ZKArchive createArchiveWithPassphrase(int pageSize, String description, byte[] readPassphrase, byte[] writePassphrase) throws IOException {
		// TODO API: (test) createArchiveWithPassphrase
		Key writeRoot = new Key(crypto, crypto.deriveKeyFromPassphrase(writePassphrase));
		Key readRoot = new Key(crypto, crypto.deriveKeyFromPassphrase(readPassphrase));
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
		byte[] passphraseRootRaw = crypto.deriveKeyFromPassphrase(passphrase);
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
		return storage.scopedFS("local/" + String.format("%16x", tag));
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
}
