package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

	// TODO Someday: (refactor) it'll hurt, but crypto and storage need to go away and everyone needs to access through config
	protected CryptoSupport crypto;
	protected FS storage;

	protected LocalConfig localConfig;
	protected ZKArchiveConfig config;
	protected ZKMaster master;
	protected HashCache<RevisionTag,ZKFS> readOnlyFilesystems;
	protected HashMap<Long,byte[]> allPageTags;
	
	protected ZKArchive(ZKArchiveConfig config) throws IOException {
		this.master = config.accessor.master;
		this.storage = config.storage;
		this.crypto = config.accessor.master.crypto;
		this.allPageTags = new HashMap<>();
		this.config = config;
		Key localKey = config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_CONFIG_FILE);
		this.localConfig = new LocalConfig(config.localStorage, localKey);
		this.readOnlyFilesystems = new HashCache<RevisionTag,ZKFS>(64, (tag) -> {
			if(this.isCacheOnly() && !tag.cacheOnly) {
				return tag.makeCacheOnly().getFS();
			} else {
				return tag.getFS();
			}
		}, (tag, fs) -> {});
		
		if(!isCacheOnly()) { // only need the list for non-networked archives, which are not cache-only
			rescanPageTags();
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
		if(isCacheOnly()) return this;
		
		assertOpen();
		ZKArchive cacheOnly = new ZKArchive(config);
		cacheOnly.storage = config.getCacheStorage();
		return cacheOnly;
	}
	
	public ZKFS openRevision(RevisionTag revision) throws IOException {
		return new ZKFS(revision);
	}
	
	public ZKFS openBlank() throws IOException {
		assertOpen();
		if(config.isReadOnly()) {
			
		}
		return new ZKFS(new RevisionTag(RefTag.blank(this), 0, 0));
	}
	
	public ZKFS openLatest() throws IOException {
		assertOpen();
		RevisionTag latest = config.getRevisionList().latest();
		if(latest == null) return null;
		return new ZKFS(latest);
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
	
	public static String dataDirForArchiveId(byte[] archiveId) {
		return GLOBAL_DATA_DIR + Util.bytesToHex(archiveId);
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
	public boolean hasInode(RevisionTag revTag, long inodeId) throws IOException {
		assertOpen();
		if(inodeId < 0) return false;
		PageTree inodeTableTree = new PageTree(revTag.getRefTag());
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
	
	/** Test if we have the first page of a given revision inode table. */
	public boolean hasInodeTableFirstPage(RevisionTag revTag) throws IOException {
		assertOpen();
		PageTree inodeTableTree = new PageTree(revTag.getRefTag());
		return inodeTableTree.pageExists(0);
	}
	
	/** Test if we have every page of a given revision cached locally. 
	 * @throws IOException */
	public boolean hasRevision(RevisionTag revTag) throws IOException {
		assertOpen();
		PageTree inodeTableTree = new PageTree(revTag.getRefTag());
		if(!inodeTableTree.exists()) return false;

		ZKFS fs = openRevision(revTag);
		for(int i = 0; i < fs.inodeTable.nextInodeId(); i++) {
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

	public synchronized Collection<byte[]> allPageTags() {
		return new ArrayList<>(allPageTags.values());
	}
	
	public void addPageTag(byte[] tag) {
		long shortTag = Util.shortTag(tag);
		if(allPageTags != null && !allPageTags.containsKey(shortTag)) {
			synchronized(this) {
				allPageTags.put(shortTag, tag);
			}
			
			config.swarm.announceTag(tag);
		}		
	}
	
	public void rescanPageTags() throws IOException {
		allPageTags.clear();
		DirectoryTraverser traverser = new DirectoryTraverser(storage, storage.opendir("/"));
		while(traverser.hasNext()) {
			byte[] tag = Page.tagForPath(traverser.next());
			allPageTags.put(Util.shortTag(tag), tag);
		}
	}
}
