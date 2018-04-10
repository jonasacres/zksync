package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import com.acrescrypto.zksync.HashCache;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.KeyFile;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.zkfs.config.LocalConfig;
import com.acrescrypto.zksync.fs.zkfs.config.PrivConfig;
import com.acrescrypto.zksync.fs.zkfs.config.PubConfig;

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

	public final static String DATA_DIR = ".zksync/archive/data/";
	public final static String CONFIG_DIR = ".zksync/archive/config/";
	public final static String REVISION_DIR = ".zksync/archive/revisions/";
	public final static String LOCAL_DIR = ".zksync/local/";
	public final static String ACTIVE_REVISION = ".zskync/local/active-revision";

	protected CryptoSupport crypto;
	protected PubConfig pubConfig;
	protected PrivConfig privConfig;
	protected LocalConfig localConfig;
	protected KeyFile keyfile;
	protected FS storage;
	protected HashCache<RefTag,ZKFS> readOnlyFilesystems;
	
	public static ZKArchive archiveAtPath(String path, char[] passphrase) throws IOException {
		return new ZKArchive(new LocalFS(path), (byte[] id) -> { return passphrase; });
	}
	
	public ZKArchive(FS storage, PassphraseProvider provider) throws IOException {
		this.storage = storage;
		this.pubConfig = new PubConfig(storage);
		this.crypto = new CryptoSupport(pubConfig);
		this.keyfile = new KeyFile(this).readOrCreate(provider.passphraseForArchive(pubConfig.getArchiveId()));
		this.privConfig = new PrivConfig(storage, deriveKey(KEY_TYPE_CIPHER, KEY_INDEX_CONFIG_PRIVATE));
		this.localConfig = new LocalConfig(storage, deriveKey(KEY_TYPE_CIPHER, KEY_INDEX_CONFIG_LOCAL));
		this.readOnlyFilesystems = new HashCache<RefTag,ZKFS>(64, (RefTag tag) -> {
			return tag.getFS();
		}, (RefTag tag, ZKFS fs) -> {});
	}

	public Key deriveKey(int type, int index, byte[] tweak) {
		Key[] keys = { keyfile.getCipherRoot(), keyfile.getAuthRoot() };
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
	
	public PubConfig getPubConfig() {
		return pubConfig;
	}
	
	public PrivConfig getPrivConfig() {
		return privConfig;
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
}
