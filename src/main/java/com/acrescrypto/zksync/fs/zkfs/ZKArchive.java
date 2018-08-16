package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.ClosedException;
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
	protected HashMap<Long,byte[]> allPageTags;
	
	protected ZKArchive(ZKArchiveConfig config) throws IOException {
		this.master = config.accessor.master;
		this.storage = config.storage;
		this.crypto = config.accessor.master.crypto;
		this.allPageTags = new HashMap<>();
		this.config = config;
		Key localKey = config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_CONFIG_FILE);
		this.localConfig = new LocalConfig(config.localStorage, localKey);
		this.readOnlyFilesystems = new HashCache<RefTag,ZKFS>(64, (RefTag tag) -> {
			return tag.getFS();
		}, (RefTag tag, ZKFS fs) -> {});
		
		if(!isCacheOnly()) { // only need the list for non-networked archives, which are not cache-only
			buildAllPageTagsList();
		}
	}
	
	public void close() {
		config.close();
	}
	
	public boolean isClosed() {
		return config.isClosed();
	}
	
	public void assertOpen() throws ClosedException {
		if(isClosed()) throw new ClosedException();
	}
	
	public ZKArchive cacheOnlyArchive() throws IOException {
		assertOpen();
		ZKArchive cacheOnly = new ZKArchive(config);
		cacheOnly.storage = config.getCacheStorage();
		return cacheOnly;
	}
	
	public ZKFS openRevision(byte[] revision) throws IOException {
		assertOpen();
		return new ZKFS(new RefTag(this, revision));
	}
	
	public ZKFS openRevision(RefTag revision) throws IOException {
		assertOpen();
		return openRevision(revision.getBytes());
	}
	
	public ZKFS openBlank() throws IOException {
		assertOpen();
		return new ZKFS(RefTag.blank(this));
	}
	
	public ZKFS openLatest() throws IOException {
		assertOpen();
		return new ZKFS(config.getRevisionTree().latest());
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
	
	public static String dataDirForArchiveId(byte[] archiveId) {
		return GLOBAL_DATA_DIR + Util.bytesToHex(archiveId);
	}

	public String dataDir() {
		return dataDirForArchiveId(config.archiveId);
	}

	public ZKArchiveConfig getConfig() {
		return config;
	}
	
	/** Test if we have a given page cached locally. 
	 * @throws ClosedException */
	public boolean hasPageTag(byte[] pageTag) throws ClosedException {
		assertOpen();
		if(allPageTags.containsKey(Util.shortTag(pageTag))) return true;
		
		return config.getCacheStorage().exists(Page.pathForTag(pageTag));
	}
	
	/** Test if we have every page of a given inode cached locally. */
	public boolean hasInode(RefTag revTag, long inodeId) throws IOException {
		assertOpen();
		if(inodeId < 0) return false;
		PageTree inodeTableTree = new PageTree(revTag);
		if(!inodeTableTree.exists()) return false;

		try {
			Inode inode = revTag.getFS().inodeTable.inodeWithId(inodeId);
			if(inode.isDeleted()) return false;
			
			PageTree tree = new PageTree(inode);
			return tree.exists();	
		} catch(InaccessibleStorageException|IllegalArgumentException exc) {
			return false;
		}
	}
	
	/** Test if we have every page of a given revision cached locally. 
	 * @throws IOException */
	public boolean hasRevision(RefTag revTag) throws IOException {
		assertOpen();
		PageTree inodeTableTree = new PageTree(revTag);
		if(!inodeTableTree.exists()) return false;

		ZKFS fs = openRevision(revTag);
		for(int i = 0; i < fs.inodeTable.nextInodeId; i++) {
			Inode inode = fs.inodeTable.inodeWithId(i);
			if(inode.isDeleted()) continue;
			PageTree tree = new PageTree(inode);
			if(!tree.exists()) return false;
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
		
		return config.equals(((ZKArchive) other).config);
	}

	public Collection<byte[]> allPageTags() {
		return allPageTags.values();
	}
	
	public void addPageTag(byte[] tag) {
		long shortTag = Util.shortTag(tag);
		if(allPageTags != null && !allPageTags.containsKey(shortTag)) {
			allPageTags.put(shortTag, tag);
			config.swarm.announceTag(tag);
		}		
	}
	
	protected void buildAllPageTagsList() throws IOException {
		DirectoryTraverser traverser = new DirectoryTraverser(storage, storage.opendir("/"));
		while(traverser.hasNext()) {
			byte[] tag = Page.tagForPath(traverser.next());
			allPageTags.put(Util.shortTag(tag), tag);
		}
	}
}
