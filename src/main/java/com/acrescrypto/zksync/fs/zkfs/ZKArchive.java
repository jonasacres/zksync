package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.DirectoryTraverser;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.config.LocalConfig;
import com.acrescrypto.zksync.utility.HashCache;
import com.acrescrypto.zksync.utility.Util;

public class ZKArchive {
	
	public final static String GLOBAL_DATA_DIR = ".zksync/archive/data/";
	public final static String CONFIG_DIR = ".zksync/archive/config/";
	public final static String REVISION_DIR = ".zksync/archive/revisions/";
	public final static String LOCAL_DIR = ".zksync/local/";
	public final static String ACTIVE_REVISION = ".zskync/local/active-revision";
	
	public final static int DEFAULT_PAGE_SIZE = 65536;

	// TODO: it'll hurt, but crypto and storage need to go away and everyone needs to access through config
	protected CryptoSupport crypto;
	protected FS storage;

	protected LocalConfig localConfig;
	protected ZKArchiveConfig config;
	protected ZKMaster master;
	protected HashCache<RefTag,ZKFS> readOnlyFilesystems;
	protected ArrayList<byte[]> allTags;
	
	public ZKArchive(ZKArchiveConfig config) throws IOException {
		this.master = config.accessor.master;
		this.storage = config.storage;
		this.crypto = config.accessor.master.crypto;
		
		this.config = config;
		Key localKey = config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_CONFIG_FILE);
		this.localConfig = new LocalConfig(storage, localKey);
		this.readOnlyFilesystems = new HashCache<RefTag,ZKFS>(64, (RefTag tag) -> {
			return tag.getFS();
		}, (RefTag tag, ZKFS fs) -> {});
		
		this.config.accessor.discoveredArchive(this);
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
		return dataDirForArchiveId(config.archiveId);
	}

	public ZKArchiveConfig getConfig() {
		return config;
	}
	
	@Override
	public int hashCode() {
		return ByteBuffer.wrap(config.archiveId).getInt();
	}

	public byte[] expandShortTag(long shortTag) {
		for(byte[] tag : allTags) {
			if(ByteBuffer.wrap(tag).getLong() == shortTag) return tag;
		}
		return null;
	}
	
	protected void loadAllTags() {
		if(allTags != null && allTags.size() > 0) return;
		allTags = new ArrayList<byte[]>();
		try {
			DirectoryTraverser traverser = new DirectoryTraverser(storage, storage.opendir("/"));
			while(traverser.hasNext()) {
				String path = traverser.next();
				byte[] tag = Util.hexToBytes(path.replace("/", ""));
				allTags.add(tag);
			}
		} catch (IOException e) {
			return;
		}
	}
	
	public ArrayList<byte[]> allTags() {
		loadAllTags();
		return allTags;
	}
}
