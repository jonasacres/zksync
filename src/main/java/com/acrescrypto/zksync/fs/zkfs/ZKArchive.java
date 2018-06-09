package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;
import com.acrescrypto.zksync.fs.DirectoryTraverser;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.backedfs.BackedFS;
import com.acrescrypto.zksync.fs.swarmfs.SwarmFS;
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
	protected LinkedList<byte[]> allPageTags;
	
	protected ZKArchive(ZKArchiveConfig config) throws IOException {
		this.master = config.accessor.master;
		this.storage = config.storage;
		this.crypto = config.accessor.master.crypto;
		this.allPageTags = new LinkedList<>();
		this.config = config;
		Key localKey = config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_CONFIG_FILE);
		this.localConfig = new LocalConfig(config.localStorage, localKey);
		this.readOnlyFilesystems = new HashCache<RefTag,ZKFS>(64, (RefTag tag) -> {
			return tag.getFS();
		}, (RefTag tag, ZKFS fs) -> {});
		
		if(!isCacheOnly()) { // only need the list for non-networked archives, which are not cache-only
			buildAllPageTagsList();
		}
		
		this.config.accessor.discoveredArchive(this);
	}
	
	public ZKArchive cacheOnlyArchive() throws IOException {
		ZKArchive cacheOnly = new ZKArchive(config);
		cacheOnly.storage = config.getCacheStorage();
		return cacheOnly;
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
	
	public boolean isCacheOnly() {
		if(!(storage instanceof BackedFS) && !(storage instanceof SwarmFS)) return true;
		return false;
	}
	
	public ZKMaster getMaster() {
		return master;
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
	
	// TODO DHT: (refactor) deprecate; moved to config
	public RevisionTree getRevisionTree() throws IOException {
		return config.getRevisionTree();
	}
	
	// TODO DHT: (refactor) deprecate; moved to config
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
	
	/** Test if we have a given page cached locally. */
	public boolean hasPageTag(byte[] pageTag) {
		return config.getCacheStorage().exists(Page.pathForTag(pageTag));
	}
	
	/** Test if we have every page of a given reftag cached locally. */
	public boolean hasRefTag(RefTag refTag) throws IOException {
		if(refTag.getRefType() == RefTag.REF_TYPE_IMMEDIATE) return true;
		try {
			PageMerkle merkle = new PageMerkle(refTag.makeCacheOnly());
			if(!merkle.exists()) return false;
			for(int i = 0; i < merkle.numPages(); i++) {
				if(!config.getCacheStorage().exists(Page.pathForTag(merkle.getPageTag(i)))) return false;
			}
			
			return true;
		} catch(InaccessibleStorageException exc) {
			return false;
		}
	}
	
	/** Test if we have every page of a given revision cached locally. 
	 * @throws IOException */
	public boolean hasRevision(RefTag revTag) throws IOException {
		if(!hasRefTag(revTag)) return false; // check if we have the inode table
		ZKFS fs = openRevision(revTag);
		for(int i = 0; i < fs.inodeTable.nextInodeId; i++) {
			Inode inode = fs.inodeTable.inodeWithId(i);
			if(inode.isDeleted()) continue;
			if(!hasRefTag(inode.refTag)) return false;
		}
		
		return true;
	}
	
	@Override
	public int hashCode() {
		return ByteBuffer.wrap(config.archiveId).getInt();
	}

	public byte[] expandShortTag(long shortTag) throws IOException {
		return Page.expandTag(storage, shortTag);
	}
	
	public boolean equals(Object other) {
		if(!(other instanceof ZKArchive)) {
			return false;
		}
		
		if(Arrays.equals(config.archiveId, ((ZKArchive) other).config.archiveId)) {
			return config.accessor.isSeedOnly() == ((ZKArchive) other).config.accessor.isSeedOnly();
		}
		
		return false;
	}

	public Collection<byte[]> allPageTags() {
		return allPageTags;
	}
	
	public void addPageTag(byte[] tag) {
		if(allPageTags != null) {
			allPageTags.add(tag);
		}
	}
	
	protected void buildAllPageTagsList() throws IOException {
		DirectoryTraverser traverser = new DirectoryTraverser(storage, storage.opendir("/"));
		while(traverser.hasNext()) {
			allPageTags.add(Page.tagForPath(traverser.next()));
		}
	}
}
