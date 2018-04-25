package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import com.acrescrypto.zksync.HashCache;
import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.config.LocalConfig;

public class ZKArchive {
	public final static int KEY_TYPE_CIPHER = 0;
	public final static int KEY_TYPE_AUTH = 1;
	public final static int KEY_TYPE_PRNG = 2;
	
	public final static int KEY_INDEX_ARCHIVE = 0;
	public final static int KEY_INDEX_PAGE = 1;
	public final static int KEY_INDEX_PAGE_MERKLE = 2;
	public final static int KEY_INDEX_REVISION = 3;
	public final static int KEY_INDEX_CONFIG_PRIVATE = 4;
	public final static int KEY_INDEX_CONFIG_LOCAL = 5;
	public final static int KEY_INDEX_REVISION_TREE = 6;
	public final static int KEY_INDEX_SEED = 7;

	public final static String GLOBAL_DATA_DIR = ".zksync/archive/data/";
	public final static String CONFIG_DIR = ".zksync/archive/config/";
	public final static String REVISION_DIR = ".zksync/archive/revisions/";
	public final static String LOCAL_DIR = ".zksync/local/";
	public final static String ACTIVE_REVISION = ".zskync/local/active-revision";
	
	public final static int DEFAULT_PAGE_SIZE = 65536;

	// TODO: it'll hurt, but crypto and storage need to go away and everyone needs to access through master
	protected CryptoSupport crypto;
	protected FS storage;

	protected LocalConfig localConfig;
	protected Keychain keychain; // TODO: rename to keyfile, kill old version of field/class
	protected ZKMaster master;
	protected HashCache<RefTag,ZKFS> readOnlyFilesystems;
	
	public ZKArchive(Keychain keychain) throws IOException {
		this.master = keychain.master;
		this.storage = keychain.master.storage;
		this.crypto = keychain.master.crypto;
		
		this.keychain = keychain;
		this.localConfig = new LocalConfig(storage, deriveKey(KEY_TYPE_CIPHER, KEY_INDEX_CONFIG_LOCAL)); // TODO: use a local key
		this.readOnlyFilesystems = new HashCache<RefTag,ZKFS>(64, (RefTag tag) -> {
			return tag.getFS();
		}, (RefTag tag, ZKFS fs) -> {});
	}
	
	// TODO: Have Keychain do this
	public Key deriveKey(int type, int index, byte[] tweak) {
		Key[] keys = { keychain.textRoot, keychain.authRoot };
		if(type >= keys.length) throw new IllegalArgumentException();
		return keys[type].derive(index, tweak);
	}
	
	public Key deriveKey(int type, int index) {
		byte[] empty = {};
		return deriveKey(type, index, empty);
	}
	
	public ZKFS openRevision(byte[] revision) throws IOException {
		return new ZKFS(new RefTag(this, revision));
	}
	
	public ZKFS openRevision(RefTag revision) throws IOException {
		return openRevision(revision.getBytes());
	}
	
	public ZKFS openBlank() throws IOException {
		return new ZKFS(RefTag.blank(this));
	}
	
	public FS getStorage() {
		return storage;
	}
	
	public CryptoSupport getCrypto() {
		return crypto;
	}
	
	public LocalConfig getLocalConfig() {
		return localConfig;
	}

	public void setLocalConfig(LocalConfig localConfig) {
		this.localConfig = localConfig;
	}
	
	public RevisionTree getRevisionTree() throws IOException {
		return new RevisionTree(this);
	}
	
	public int refTagSize() {
		return RefTag.REFTAG_EXTRA_DATA_SIZE + crypto.hashLength();
	}
	
	public static String dataDirForArchiveId(byte[] archiveId) {
		return GLOBAL_DATA_DIR + Util.bytesToHex(archiveId);
	}

	public String dataDir() {
		return dataDirForArchiveId(keychain.archiveId);
	}

	public Keychain getKeychain() {
		return keychain;
	}
}
