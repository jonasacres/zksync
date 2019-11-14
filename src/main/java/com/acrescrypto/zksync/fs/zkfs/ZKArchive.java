package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.ClosedException;
import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.DirectoryTraverser;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.backedfs.BackedFS;
import com.acrescrypto.zksync.fs.swarmfs.SwarmFS;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionToken;
import com.acrescrypto.zksync.utility.HashCache;
import com.acrescrypto.zksync.utility.Util;

public class ZKArchive implements AutoCloseable {
	public final static String GLOBAL_DATA_DIR = ".zksync/archive/data/";
	public final static String CONFIG_DIR = ".zksync/archive/config/";
	public final static String REVISION_DIR = ".zksync/archive/revisions/";
	public final static String LOCAL_DIR = ".zksync/local/";
	public final static String ACTIVE_REVISION = ".zskync/local/active-revision";
	
	public final static int DEFAULT_PAGE_SIZE = 65536;

	protected static ConcurrentHashMap<ZKArchive,Throwable> activeArchives = new ConcurrentHashMap<>();
	
	protected Logger logger = LoggerFactory.getLogger(ZKArchive.class);

	// TODO Someday: (refactor) it'll hurt, but crypto and storage need to go away and everyone needs to access through config
	protected CryptoSupport crypto;
	protected FS storage;

	protected ZKArchiveConfig config;
	protected ZKArchive cacheOnlyArchive;
	protected ZKMaster master;
	protected HashCache<RevisionTag,ZKFS> readOnlyFilesystems;
	protected HashMap<Long,StorageTag> allPageTags;
	protected SubscriptionToken<Integer> tok;
	protected boolean closed;
	private StorageTag blankStorageTag;
	
	public static void addActiveArchive(ZKArchive archive) {
		activeArchives.put(archive, new Throwable());
	}
	
	public static void removeActiveArchive(ZKArchive archive) {
		activeArchives.remove(archive);
	}
	
	public static ConcurrentHashMap<ZKArchive,Throwable> getActiveArchives() {
		return activeArchives;
	}
	
	protected ZKArchive(ZKArchiveConfig config) throws IOException {
		this.master = config.accessor.master;
		this.storage = config.storage;
		this.crypto = config.accessor.master.crypto;
		this.allPageTags = new HashMap<>();
		this.config = config;
		
		int cacheSize = config.getMaster().getGlobalConfig().getInt("fs.settings.readOnlyFilesystemCacheSize");
		this.readOnlyFilesystems = new HashCache<RevisionTag,ZKFS>(cacheSize, (tag) -> {
			if(this.isCacheOnly() && !tag.cacheOnly) {
				return tag.makeCacheOnly().getFS().setReadOnly();
			} else {
				return tag.getFS().setReadOnly();
			}
		}, (tag, fs) -> {
			fs.close();
		});
		
		tok = config.getMaster().getGlobalConfig().subscribe("fs.settings.readOnlyFilesystemCacheSize").asInt((s)->{
			try {
				logger.info("ZKFS {} -: Setting read only filesystem cache size to {}, was {}",
						Util.formatArchiveId(config.getArchiveId()),
						s,
						readOnlyFilesystems.getCapacity());
				readOnlyFilesystems.setCapacity(s);
			} catch (IOException exc) {
				logger.error("ZKFS {} -: Caught exception setting read only cache size to {}",
						Util.formatArchiveId(config.getArchiveId()),
						s,
						exc);
			}
		});

		if(!isCacheOnly()) { // only need the list for non-networked archives, which are not cache-only
			rescanPageTags();
		}
		
		if(FS.fileHandleTelemetryEnabled) {
			addActiveArchive(this);
		}
	}
	
	public void close() {
		if(closed) return;
		synchronized(this) {
			if(closed) return;
			closed = true;
		}
		
		if(tok != null) tok.close();
		try {
			readOnlyFilesystems.removeAll();
		} catch (IOException exc) {
			logger.error("ZKFS {}: Caught exception closing read-only filesystems during archive closure",
					Util.formatArchiveId(config.getArchiveId()),
					exc);
		}
		
		if(cacheOnlyArchive != null) {
			cacheOnlyArchive.close();
		}
		
		config.close();
		
		if(FS.fileHandleTelemetryEnabled) {
			removeActiveArchive(this);
		}
	}
	
	public boolean isClosed() {
		return config.isClosed();
	}
	
	public void assertOpen() throws ClosedException {
		if(isClosed()) throw new ClosedException();
	}
	
	public ZKArchive cacheOnlyArchive() throws IOException {
		assertOpen();
		if(isCacheOnly()) return this;
		if(cacheOnlyArchive != null) return cacheOnlyArchive;
		
		synchronized(this) {
			assertOpen();
			if(cacheOnlyArchive != null) return cacheOnlyArchive;

			cacheOnlyArchive = new ZKArchive(config);
			cacheOnlyArchive.storage = config.getCacheStorage();
			return cacheOnlyArchive;
		}
	}
	
	public ZKFS openRevision(RevisionTag revision) throws IOException {
		return new ZKFS(revision);
	}
	
	/** Returns a cached RO FS if we have only, else returns a newly-allocated FS. */
	public ZKFS openRevisionReadOnlyOpportunistic(RevisionTag revision) throws IOException {
		synchronized(readOnlyFilesystems) {
			if(readOnlyFilesystems.hasCached(revision)) {
				return readOnlyFilesystems.get(revision).retain();
			}
		}
		
		return openRevision(revision);
	}
	
	public ZKFS openRevisionReadOnly(RevisionTag revision) throws IOException {
		if(isClosed()) {
			throw new ClosedException();
		}
		
		if(!revision.hasStructureLocally()) {
			try(ZKFS tempFs = revision.getFS()) {
				/* force acquisition of missing pages without locking readOnlyFilesystems.
				 * slightly inefficient since we'll just close and reopen, but this prevents us
				 * from holding the monitor while we wait for pages to come in over the network.
				 */
			}
		}
		
		ZKFS fs;
		synchronized(readOnlyFilesystems) {
			// grab the monitor on rOF to prevent eviction before we retain
			fs = readOnlyFilesystems.get(revision).retain();
			
			if(fs.isClosed()) {
				// defensively, we will ensure that we don't return a closed FS
				readOnlyFilesystems.remove(revision);
				fs = readOnlyFilesystems.get(revision).retain();
				
			}
		}
		
		if(isClosed()) {
			synchronized(readOnlyFilesystems) {
				readOnlyFilesystems.removeAll();
			}
			fs.close();
			throw new ClosedException();
		}
		
		return fs;
	}
	
	public ZKFS openBlank() throws IOException {
		assertOpen();
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
	
	public static String dataDirForArchiveId(byte[] archiveId) {
		return GLOBAL_DATA_DIR + Util.bytesToHex(archiveId);
	}

	public ZKArchiveConfig getConfig() {
		return config;
	}
	
	/** Test if we have a given page cached locally. 
	 * @throws IOException */
	public boolean hasPageTag(StorageTag pageTag) throws IOException {
		assertOpen();
		if(allPageTags.containsKey(pageTag.shortTagPreserialized())) return true;
		
		return config.getCacheStorage().exists(pageTag.path());
	}
	
	/** Test if we have every page of a given inode cached locally. */
	public boolean hasInode(RevisionTag revTag, long inodeId) throws IOException {
		assertOpen();
		if(inodeId < 0) return false;
		PageTree inodeTableTree = new PageTree(revTag.getRefTag());
		if(!inodeTableTree.exists()) return false;

		try(ZKFS fs = revTag.readOnlyFS()) {
			if(!fs.inodeTable.hasInodeWithId(inodeId)) return false;
			Inode inode = fs.inodeTable.inodeWithId(inodeId);
			if(inode.isDeleted()) return false;
			
			PageTree tree = new PageTree(inode);
			return tree.exists();
		} catch(InaccessibleStorageException|IllegalArgumentException exc) {
			return false;
		}
	}
	
	/** Test if we have every page of a given inode table cached locally. */
	public boolean hasInodeTable(RevisionTag revTag) throws IOException {
		assertOpen();
		PageTree inodeTableTree = new PageTree(revTag.getRefTag());
		return inodeTableTree.exists();
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

		try(ZKFS fs = openRevision(revTag)) {
			for(int i = 0; i < fs.inodeTable.nextInodeId(); i++) {
				Inode inode = fs.inodeTable.inodeWithId(i);
				if(inode.isDeleted()) continue;
				PageTree tree = new PageTree(inode);
				if(!tree.exists()) return false;
			}
		}
		
		return true;
	}
	
	@Override
	public int hashCode() {
		return ByteBuffer.wrap(config.archiveId).getInt();
	}

	public StorageTag expandShortTag(long shortTag) throws IOException {
		return Page.expandTag(crypto, storage, shortTag);
	}
	
	public boolean equals(Object other) {
		if(!(other instanceof ZKArchive)) {
			return false;
		}
		
		return config.equals(((ZKArchive) other).config);
	}

	public synchronized Collection<StorageTag> allPageTags() {
		return new ArrayList<>(allPageTags.values());
	}
	
	public void addPageTag(StorageTag tag) {
		if(tag.isImmediate()) {
			Util.debugLog(String.format("Added page tag %s", tag));
		}
		long shortTag = tag.shortTagPreserialized();
		if(allPageTags != null && !allPageTags.containsKey(shortTag)) {
			synchronized(this) {
				allPageTags.put(shortTag, tag);
			}
			
			config.swarm.announceTag(tag);
		}		
	}
	
	public void rescanPageTags() throws IOException {
		allPageTags.clear();
		try(Directory dir = storage.opendir("/")) {
			DirectoryTraverser traverser = new DirectoryTraverser(storage, dir);
			while(traverser.hasNext()) {
				StorageTag tag = new StorageTag(crypto, traverser.next().getPath());
				allPageTags.put(tag.shortTag(), tag);
			}
		}
	}
	
	public StorageTag getBlankStorageTag() {
		if(blankStorageTag == null) {
			byte[] blankBytes = new byte[crypto.hashLength()];
			blankStorageTag = new StorageTag(crypto, blankBytes);
		}
		
		return blankStorageTag;
	}
}
