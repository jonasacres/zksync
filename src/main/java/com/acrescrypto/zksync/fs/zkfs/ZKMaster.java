package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.LinkedList;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.localfs.LocalFS;

public class ZKMaster {
	protected CryptoSupport crypto;
	protected FS storage;
	protected PassphraseProvider passphraseProvider;
	protected StoredAccess storedAccess;
	protected Key localKey;
	protected LinkedList<ArchiveAccessor> accessors = new LinkedList<ArchiveAccessor>();
	
	public static ZKMaster openAtPath(PassphraseProvider ppProvider, String path) {
		return new ZKMaster(new CryptoSupport(), new LocalFS(path), ppProvider);
	}
	
	public ZKMaster(CryptoSupport crypto, FS storage, PassphraseProvider passphraseProvider) {
		this.crypto = crypto;
		this.storage = storage;
		this.passphraseProvider = passphraseProvider;
		
		byte[] passphrase = passphraseProvider.requestPassphrase("ZKSync storage passphrase");
		localKey = new Key(crypto, crypto.deriveKeyFromPassphrase(passphrase));
		
		this.storedAccess = new StoredAccess(this);
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
		ArchiveAccessor accessor = accessorForRoot(passphraseRoot);
		if(accessor == null) {
			accessor = makeAccessorForRoot(passphraseRoot, false);
		}
		
		ZKArchiveConfig config = new ZKArchiveConfig(accessor, description, pageSize);
		return new ZKArchive(config);
	}
	
	public String storagePathForArchiveId(byte[] archiveId) {
		return "archives/" + Util.bytesToHex(archiveId);
	}

	public FS storageFsForArchiveId(byte[] archiveId) throws IOException {
		return storage.scopedFS(storagePathForArchiveId(archiveId));
	}
	
	public ArchiveAccessor accessorForRoot(Key rootKey) {
		for(ArchiveAccessor accessor : accessors) {
			if(!accessor.isSeedOnly()) {
				if(accessor.passphraseRoot.equals(rootKey)) return accessor;
			}
			
			if(accessor.seedRoot.equals(rootKey)) return accessor;
		}
		
		return null;
	}
	
	public ArchiveAccessor makeAccessorForRoot(Key rootKey, boolean isSeed) {
		ArchiveAccessor accessor = new ArchiveAccessor(this, rootKey, isSeed ? ArchiveAccessor.KEY_ROOT_SEED : ArchiveAccessor.KEY_ROOT_PASSPHRASE);
		accessors.add(accessor);
		return accessor;
	}
}
