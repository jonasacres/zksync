package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor.ArchiveAccessorDiscoveryCallback;
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.utility.Util;

public class ZKMaster implements ArchiveAccessorDiscoveryCallback {
	public final static String KEYFILE = "keyfile"; 
	protected CryptoSupport crypto;
	protected FS storage;
	protected PassphraseProvider passphraseProvider;
	protected StoredAccess storedAccess;
	protected Key localKey;
	protected LinkedList<ArchiveAccessor> accessors = new LinkedList<ArchiveAccessor>();
	protected HashSet<ZKArchive> allArchives = new HashSet<ZKArchive>();
	protected Blacklist blacklist;
	protected Logger logger = LoggerFactory.getLogger(ZKMaster.class);
	
	public static ZKMaster openAtPath(PassphraseProvider ppProvider, String path) throws IOException {
		return new ZKMaster(new CryptoSupport(), new LocalFS(path), ppProvider);
	}
	
	public ZKMaster(CryptoSupport crypto, FS storage, PassphraseProvider passphraseProvider) throws IOException {
		this.crypto = crypto;
		this.storage = storage;
		this.passphraseProvider = passphraseProvider;
		getLocalKey();
		this.storedAccess = new StoredAccess(this);
		this.blacklist = new Blacklist(storage, "blacklist", localKey.derive(ArchiveAccessor.KEY_INDEX_BLACKLIST, new byte[0]));
		loadStoredAccessors();
	}
	
	public void loadStoredAccessors() {
		try {
			storedAccess.read();
		} catch (IOException e) {
		}
	}
	
	public void getLocalKey() throws IOException {
		Key ppKey;
		do {
			byte[] passphrase = passphraseProvider.requestPassphrase("ZKSync storage passphrase");
			ppKey = new Key(crypto, crypto.deriveKeyFromPassphrase(passphrase));
		} while(!attemptPassphraseKey(ppKey));
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
				logger.warn("Supplied passphrase did not match key file");
				return false;
			}
		} else {
			// TODO: it'd be nice to ask for a passphrase confirmation here...
			logger.info("No keyfile found; creating...");
			localKey = new Key(crypto, crypto.rng(crypto.symKeyLength()));
			return true;
		}
	}
	
	public Blacklist getBlacklist() {
		return blacklist;
	}
	
	public CryptoSupport getCrypto() {
		return crypto;
	}
	
	public FS getStorage() {
		return storage;
	}
	
	public void purge() throws IOException {
		if(storage.exists("/")) storage.rmrf("/");
	}
	
	public ZKArchive createArchive(int pageSize, String description) throws IOException {
		byte[] passphrase = passphraseProvider.requestPassphrase("Passphrase for new archive '" + description + "'");
		byte[] passphraseRootRaw = crypto.deriveKeyFromPassphrase(passphrase);
		Key passphraseRoot = new Key(crypto, passphraseRootRaw);
		ArchiveAccessor accessor = makeAccessorForRoot(passphraseRoot, false);
		ZKArchiveConfig config = new ZKArchiveConfig(accessor, description, pageSize);
		return config.archive;
	}
	
	public String storagePathForArchiveId(byte[] archiveId) {
		return "archives/" + Util.bytesToHex(archiveId);
	}

	public FS storageFsForArchiveId(byte[] archiveId) throws IOException {
		return storage.scopedFS(storagePathForArchiveId(archiveId));
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
	
	public synchronized ArchiveAccessor makeAccessorForRoot(Key rootKey, boolean isSeed) {
		ArchiveAccessor accessor = accessorForRoot(rootKey);
		if(accessor != null) return accessor;
		
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
	public void discoveredArchive(ZKArchive archive) {
		allArchives.add(archive);
	}
}
