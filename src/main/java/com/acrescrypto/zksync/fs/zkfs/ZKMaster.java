package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

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
		ZKArchiveConfig config = new ZKArchiveConfig(this, passphraseRoot, description, pageSize);
		return new ZKArchive(config);
	}
}
