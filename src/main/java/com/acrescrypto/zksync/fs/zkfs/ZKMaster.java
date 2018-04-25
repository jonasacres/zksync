package com.acrescrypto.zksync.fs.zkfs;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.localfs.LocalFS;

public class ZKMaster {
	protected CryptoSupport crypto;
	protected FS storage;
	protected PassphraseProvider passphraseProvider;
	protected StoredAccess storedAccess;
	
	public ZKMaster openAtPath(PassphraseProvider ppProvider, String path) {
		return new ZKMaster(new CryptoSupport(), new LocalFS(path), ppProvider);
	}
	
	public ZKMaster(CryptoSupport crypto, FS storage, PassphraseProvider passphraseProvider) {
		this.crypto = crypto;
		this.storage = storage;
		this.passphraseProvider = passphraseProvider;
		
		this.storedAccess = new StoredAccess(this);
	}
	
	public CryptoSupport getCrypto() {
		return crypto;
	}
	
	public FS getStorage() {
		return storage;
	}
}
