package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.lang3.mutable.MutableLong;

import com.acrescrypto.zksync.exceptions.ClosedException;
import com.acrescrypto.zksync.exceptions.EACCESException;
import com.acrescrypto.zksync.exceptions.EMLINKException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.NonexistentPageException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.FreeList.FreeListExhaustedException;
import com.acrescrypto.zksync.fs.zkfs.resolver.InodeDiff;
import com.acrescrypto.zksync.utility.HashCache;
import com.acrescrypto.zksync.utility.Util;

// represents inode table for ZKFS instance. 
public class InodeTable extends ZKFile {
	/** storage for all inode data */
	public final static long INODE_ID_INODE_TABLE = 0;
	
	/** top level directory of filesystem */
	public final static long INODE_ID_ROOT_DIRECTORY = 1;
	
	/** freelist of avaialble inode IDs */
	public final static long INODE_ID_FREELIST = 2;

	/** first inode ID issued to actual files */
	public final static long USER_INODE_ID_START = 16;
	
	/** fake value used to fill path field */
	public final static String INODE_TABLE_PATH = "(inode table)";
	
	protected RevisionInfo revision; /** current revision metadata */
	protected FreeList freelist;
	protected long nextInodeId; /** next inode ID to be issued when freelist is exhausted */
	protected String pendingTitle;
	
	protected HashCache<Long,Inode[]> inodesByPage; /** in-memory cache of inode data */
	
	/** serialized size of an inode for a given archive, in bytes */
	public static int inodeSize(ZKArchive archive) {
		return Stat.STAT_SIZE + 2*8 + 1*4 + 1 + archive.config.refTagSize() + RevisionTag.sizeForConfig(archive.config);		
	}
	
	/** initialize inode table for FS.
	 * 
	 * @param fs filesystem to init inode table for
	 * @param tag tag for revision to be loaded. if blank tag supplied, a blank inode table is created.
	 * @throws IOException
	 */
	public InodeTable(ZKFS fs, RevisionTag tag) throws IOException {
		super(fs);
		try {
			this.trusted = false;
			this.path = INODE_TABLE_PATH;
			this.mode = O_RDWR;
			int cacheSize = fs.getArchive().getMaster().getGlobalConfig().getInt("fs.settings.inodeTablePageCacheSize");
			this.inodesByPage = new HashCache<Long,Inode[]>(cacheSize, (Long pageNum) -> {
				logger.trace("ZKFS {} {}: Caching inode table page {}",
						Util.formatArchiveId(fs.getArchive().getConfig().getArchiveId()),
						Util.formatRevisionTag(fs.baseRevision),
						pageNum);
				return inodesForPage(pageNum);
			}, (Long pageNum, Inode[] inodes) -> {
				logger.trace("ZKFS {} {}: Evicting inode table page {} from cache",
						Util.formatArchiveId(fs.getArchive().getConfig().getArchiveId()),
						Util.formatRevisionTag(fs.baseRevision),
						pageNum);
				commitInodePage(pageNum, inodes);
			});
			
			fs.getArchive().getMaster().getGlobalConfig().subscribe("fs.settings.inodeTablePageCacheSize").asInt((s)->{
				try {
					logger.info("ZKFS {} {}: Setting InodeTable page cache size to {}; was {}",
							Util.formatArchiveId(fs.getArchive().getConfig().getArchiveId()),
							Util.formatRevisionTag(fs.baseRevision),
							s,
							this.inodesByPage.getCapacity());
					this.inodesByPage.setCapacity(s);
				} catch(IOException exc) {
					logger.error("Unable to set inode table page cache size", exc);
				}
			});
			
			if(tag.getRefTag().isBlank()) initialize();
			else readExisting(tag);
		} catch(Throwable exc) {
			this.close();
			throw exc;
		}
	}
	
	@Override
	public synchronized void close() throws IOException {
		if(freelist != null) {
			freelist.close();
		}
		
		super.close();
		if(inodesByPage != null) {
			if(!this.getFS().getArchive().getConfig().isReadOnly()) {
				inodesByPage.removeAll();
			}
			inodesByPage = null;
		}
	}
	
	/** calculate the next inode ID to be issued (by scanning for the largest issued inode ID) */
	protected synchronized long lookupNextInodeId() throws IOException {
		long pageNum = inode.refTag.numPages-1;
		Inode[] inodes = inodesByPage.get(pageNum);
		
		long maxInodeId = USER_INODE_ID_START-1;
		for(Inode inode : inodes) {
			if(inode.isDeleted()) continue;
			maxInodeId = Math.max(maxInodeId, inode.stat.getInodeId());
		}
		
		Util.debugLog(String.format("InodeTable %s: %s, dirty=%s last page is %d, scanned max inodeId %d (next inodeId %d)",
				zkfs.archive.master.getName(),
				Util.formatRevisionTag(zkfs.baseRevision),
				zkfs.dirty ? "true" : "false",
				pageNum,
				maxInodeId,
				maxInodeId + 1));
		return maxInodeId+1;
	}
	
	/** write inode table to filesystem, creating a new revision
	 * 
	 * @return RefTag for the newly created revision
	 *  */
	public synchronized Collection<RevisionTag> commitWithTimestamp(RevisionTag[] additionalParents, long timestamp) throws IOException {
		// TODO Someday: (design) Objective merge logic breaks down if we have additional parents AND we had changes to the filesystem. Throw an exception if someone tries to do that.
		// TODO Someday: (refactor) I regret doing these as arrays instead of collections. Refactor.
		if(zkfs.archive.config.isReadOnly()) throw new EACCESException("cannot commit new revisions when archive is opened read-only");
		freelist.commit();
		ArrayList<RevisionTag> parents = makeParentList(additionalParents);
		updateRevisionInfo(parents);
		
		if(timestamp >= 0) {
			Inode[] fixedTimestampInodes = new Inode[] {
					this.inodeWithId(INODE_ID_ROOT_DIRECTORY),
					this.inodeWithId(INODE_ID_FREELIST),
			};
			
			for(Inode fixedInode : fixedTimestampInodes) {
				fixedInode.setModifiedTime(timestamp);
				fixedInode.getStat().setMtime(timestamp);
				fixedInode.getStat().setAtime(timestamp);
			}
		}

		syncInodes();
		return parents;
	}
	
	/** Sets the title to be used for the next commit.
	 * 
	 * @param title New archive title
	 */
	public void setNextTitle(String title) {
		this.pendingTitle = title;
	}
	
	public String getNextTitle() {
		if(pendingTitle != null) return pendingTitle;
		if(revision != null) return revision.title;
		return zkfs.getArchive().getConfig().getDescription();
	}
	
	/** clear the old revision info and replace with a new one */
	protected void updateRevisionInfo(Collection<RevisionTag> parents) throws IOException {
		String title = revision.title;
		if(pendingTitle != null) {
			title = pendingTitle;
			pendingTitle = null;
		}
		
		RevisionInfo newRevision = new RevisionInfo(this, parents, revision.generation+1, title);
		revision = newRevision;
	}
	
	/** write out all cached inodes */
	protected synchronized void syncInodes() throws IOException {
		for(Long pageNum : inodesByPage.cachedKeys()) {
			commitInodePage(pageNum, inodesByPage.get(pageNum));
		}
		flush();
	}
		
	protected ArrayList<RevisionTag> makeParentList(RevisionTag[] additionalParents) {
		ArrayList<RevisionTag> parents = new ArrayList<>(1+additionalParents.length);
		parents.add(zkfs.baseRevision);
		for(RevisionTag parent : additionalParents) {
			if(!parents.contains(parent)) {
				parents.add(parent);
			}
		}
		
		parents.sort(null);
		
		return parents;
	}
	
	/** size of an inode for this table, in bytes */
	public int inodeSize() {
		return inodeSize(zkfs.archive);
	}
	
	/** remove an inode from the inode table
	 * 
	 * @param inodeId inode to be removed
	 * @throws IllegalArgumentException attempted to remove mandatory inode (eg inode table or root directory)
	 * @throws EMLINKException inode nlink > 0
	 */
	public void unlink(long inodeId) throws IOException {
		if(!hasInodeWithId(inodeId)) throw new ENOENTException(String.format("inode %d", inodeId));
		if(inodeId <= 1) throw new IllegalArgumentException();
		
		Inode inode = inodeWithId(inodeId);
		if(inode.nlink > 0) {
			throw new EMLINKException(String.format("inode %d", inodeId));
		}
		
		logger.debug("ZKFS {} {}: Unlinking inode {}",
				Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(zkfs.baseRevision),
				inodeId);
		
		// don't need to clear the inode if it's already clear
		if(inode.identity != 0 || !inode.refTag.isBlank() || inode.flags != 0) {
			inode.markDeleted();
			
			// don't need to add to the freelist if we're already in it implicitly (e.g. we deleted the last inode in the table)
			if(USER_INODE_ID_START <= inodeId && inodeId < nextInodeId()) {
				freelist.freeInodeId(inodeId);
			}
		}
	}
	
	/** return an inode with a given ID */
	public synchronized Inode inodeWithId(long inodeId) throws IOException {
		if(closed || closing) {
			throw new ClosedException();
		}
		try {
			return inodesByPage.get(pageNumForInodeId(inodeId))[pageOffsetForInodeId(inodeId)];
		} catch(NullPointerException exc) {
			exc.printStackTrace();
			throw exc;
		}
	}
	
	/** test if table contains an inode with the given ID 
	 * @throws IOException */
	public boolean hasInodeWithId(long inodeId) throws IOException {
		if(inodeId > nextInodeId()) return false;
		// TODO Someday: (refactor) not reliable, freelist can only say if cached listings are present
		/* Right now, we don't care because hasInodeWithId is only checked during unlink,
		 * which gracefully handles unlinking inodes that are already in the freelist.
		 */
		if(freelist.contains(inodeId)) return false;
		return true;
	}
	
	/** issue next inode ID (draw from freelist if available, or issue next sequential ID if freelist is empty) */
	public synchronized long issueInodeId() throws IOException {
		try {
			/* try pulling an ID from the freelist, ignoring any ID that exceeds the next one in sequence.
			 * we don't want to take a larger ID than our next sequential ID from the freelist, because we'll
			 * be in danger of reissuing the same ID to someone else later!
			 * 
			 * we also have to be careful that the inode really is deleted, since it's possible for these to
			 * get reissued out-of-order in diff merges, meaning that they will remain in the freelist
			 * despite being allocated...
			 */
			long inodeId = Long.MAX_VALUE;
			while(inodeId >= nextInodeId() || !inodeWithId(inodeId).isDeleted()) {
				inodeId = freelist.issueInodeId(); 
			}

			Util.debugLog(String.format("InodeTable %s: %s, dirty=%s issuing inodeId %d from freelist",
					zkfs.archive.master.getName(),
					Util.formatRevisionTag(zkfs.baseRevision),
					zkfs.dirty ? "true" : "false",
					inodeId));
			
			return inodeId;
		} catch(FreeListExhaustedException exc) {
			nextInodeId(); // ensure we have nextInodeId loaded
			Util.debugLog(String.format("InodeTable %s: %s, dirty=%s issuing inodeId %d from sequence",
					zkfs.archive.master.getName(),
					Util.formatRevisionTag(zkfs.baseRevision),
					zkfs.dirty ? "true" : "false",
					nextInodeId));
			return nextInodeId++;
		}
	}
	
	/** create a new inode */
	public Inode issueInode() throws IOException {
		return issueInode(issueInodeId());
	}
	
	/** issue a new inode with blank content and default metadata */
	public Inode issueInode(long inodeId) throws IOException {
		Inode inode = inodeWithId(inodeId);
		long now = Util.currentTimeNanos();
		/* The identity field in an inode is the only source of nondeterminism in a zksync archive,
		 * at least as of this writing (10/30/18). */
		inode.setIdentity(ByteBuffer.wrap(zkfs.archive.crypto.rng(8)).getLong());
		logger.debug("ZKFS {} {}: Issuing inode {} with identity {}",
				Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(zkfs.getBaseRevision()),
				inodeId,
				inode.getIdentity());
		inode.getStat().setInodeId(inodeId);
		inode.getStat().setCtime(now);
		inode.getStat().setAtime(now);
		inode.getStat().setMtime(now);
		inode.getStat().setType(0);
		inode.getStat().setDevMajor(0);
		inode.getStat().setDevMinor(0);
		inode.getStat().setSize(0);
		inode.getStat().setMode (zkfs.archive.master.getGlobalConfig().getInt   ("fs.default.fileMode"));
		inode.getStat().setUser (zkfs.archive.master.getGlobalConfig().getString("fs.default.username"));
		inode.getStat().setUid  (zkfs.archive.master.getGlobalConfig().getInt   ("fs.default.uid"));
		inode.getStat().setGroup(zkfs.archive.master.getGlobalConfig().getString("fs.default.groupname"));
		inode.getStat().setGid  (zkfs.archive.master.getGlobalConfig().getInt   ("fs.default.gid"));
		inode.setRefTag(RefTag.blank(zkfs.archive));
		return inode;
	}
	
	/** Manually check each directory and inode to ensure nlink field is consistent. Called after each merge. 
	 * @throws IOException */
	public synchronized void rebuildLinkCounts() throws IOException {
		zkfs.lockedOperation(()->{
			freelist.clearList();
			MutableLong maxInodeId = new MutableLong(nextInodeId()-1);
			HashMap<Long,Integer> inodeCounts = new HashMap<>();
			StringBuffer sb = new StringBuffer(String.format("InodeTable %s: rebuilding, base revision %s, nextInodeId %d\n",
					zkfs.archive.master.getName(),
					Util.formatRevisionTag(zkfs.baseRevision),
					nextInodeId()));
			
			try(ZKDirectory dir = zkfs.opendir("/")) {
				dir.walk(Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS|Directory.LIST_OPT_INCLUDE_DOT_DOTDOT, (path, stat, isBroken)->{
					long inodeId = stat.getInodeId();
					Inode inode = inodeWithId(inodeId);
					int nlinkCount = inodeCounts.getOrDefault(inodeId, 0) + 1;
					sb.append(String.format("\t%-50s, inodeId %4d, identity %16x, counted nlink %2d, existing nlink %2d\n",
							path,
							inodeId,
							inode.identity,
							nlinkCount,
							inode.nlink));
					inodeCounts.put(inodeId, nlinkCount);
					if(inodeId > maxInodeId.longValue()) {
						maxInodeId.setValue(inodeId);
					}
				});
			}
			
			// walk backwards through inode table, handling any 0 nlink inodes as we go
			for(long inodeId = maxInodeId.longValue(); inodeId >= 0; inodeId--) {
				Inode inode = inodeWithId(inodeId);
				int nlink = inodeCounts.getOrDefault(inodeId, 0);
				if(inode.nlink != nlink) {
					if(nlink == 0) {
						inode.nlink = 0;
						if((inode.flags & Inode.FLAG_RETAIN) == 0) {
							sb.append(String.format("\tfree orphaned inodeId %3d, identity %16x, previous nlink %02d, type %02x, changedfrom %s\n",
									inode.getStat().getInodeId(),
									inode.identity,
									inode.nlink,
									inode.getStat().getType(),
									Util.formatRevisionTag(inode.changedFrom)));
							unlink(inodeId); // found an orphaned inode, free it up
						}
					} else {
						sb.append(String.format("\tupdate nlink for inodeId %3d, identity %16x, previous nlink %02d, new nlink %02d, type %02x, changedfrom %s\n",
								inode.getStat().getInodeId(),
								inode.identity,
								inode.nlink,
								nlink,
								inode.getStat().getType(),
								Util.formatRevisionTag(inode.changedFrom)));
						inode.nlink = nlink;
					}
				} else if(nlink == 0 && (inode.flags & Inode.FLAG_RETAIN) == 0) {
					/* we cleared the free list, so add the inode ID back in
					 * (freelist is a LIFO, so we want lowest inode IDs on top of the stack, hence why we
					 * traverse the inode table from end to start) */
					sb.append(String.format("\tadd to freelist previously freed inode inodeId %3d, identity %16x, previous nlink %02d, type %02x, changedfrom %s\n",
							inode.getStat().getInodeId(),
							inode.identity,
							inode.nlink,
							inode.getStat().getType(),
							Util.formatRevisionTag(inode.changedFrom)));
					inode.markDeleted();
					if(inodeId >= USER_INODE_ID_START) {
						freelist.freeInodeId(inodeId);
					}
				}
			}
			
			this.nextInodeId = -1;
			Util.debugLog(sb.toString());
			dirty = true;
			
			return null;
		});
	}
	
	/** array of all inodes stored at a given page number of the inode table */
	protected Inode[] inodesForPage(long pageNum) throws IOException {
		if(pageNum == 0) return inodesForFirstPage();
		Inode[] list = new Inode[numInodesForPage(pageNum)];
		
		try {
			seek(pageNum*zkfs.archive.config.pageSize, SEEK_SET);
		} catch(IllegalArgumentException exc) {
			throw new IOException();
		}
		
		ByteBuffer buf = ByteBuffer.wrap(read((int) zkfs.archive.config.pageSize));
		byte[] serialized = new byte[inodeSize()];
		int i = 0;
		
		while(buf.remaining() >= serialized.length && buf.hasRemaining()) {
			buf.get(serialized);
			list[i++] = new Inode(zkfs, serialized);
		}
		
		while(i < list.length) list[i++] = new Inode(zkfs);
		
		return list;
	}
	
	protected Inode[] inodesForFirstPage() throws IOException {
		Inode[] list = new Inode[numInodesForPage(0)];
		try {
			// the first part of the first page is our RevisionInfo
			seek(RevisionInfo.FIXED_SIZE, SEEK_SET);
		} catch(IllegalArgumentException exc) {
			throw new IOException();
		}
		
		// Read the remainder of the first page
		ByteBuffer buf = ByteBuffer.wrap(read((int) zkfs.archive.config.pageSize - RevisionInfo.FIXED_SIZE));
		byte[] serialized = new byte[inodeSize()];
		int i = 0;
		
		while(buf.remaining() >= serialized.length && buf.hasRemaining()) {
			buf.get(serialized);
			list[i++] = new Inode(zkfs, serialized);
		}
		
		while(i < list.length) list[i++] = new Inode(zkfs);
		list[0] = inode; // inject our own inode in at index 0
		
		return list;
	}
	
	protected RevisionInfo readRevisionInfo() throws IOException {
		seek(0, SEEK_SET);
		byte[] serializedRevInfo = read(RevisionInfo.FIXED_SIZE);
		if(serializedRevInfo.length < RevisionInfo.FIXED_SIZE) {
			throw new NonexistentPageException(inode.refTag, 0);
		}
		
		return new RevisionInfo(this, serializedRevInfo);
	}
	
	/** write an array of inodes to a given page number */
	protected void commitInodePage(long pageNum, Inode[] inodes) throws IOException {
		logger.debug("ZKFS {} {}: Committing inode page {}, {} inodes",
				Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(zkfs.baseRevision),
				pageNum,
				inodes.length);
		if(pageNum == 0) {
			commitFirstInodePage(inodes);
			return;
		}
		
		seek(pageNum*zkfs.archive.config.pageSize, SEEK_SET);
		for(Inode inode : inodes) write(inode.serialize());
		if(offset % zkfs.archive.config.pageSize != 0) {
			// pad out the rest of the page if needed
			write(new byte[(int) (zkfs.archive.config.pageSize - (offset % zkfs.archive.config.pageSize))]);
		}
	}
	
	protected void commitFirstInodePage(Inode[] inodes) throws IOException {
		seek(0, SEEK_SET);
		write(revision.serialize());
		for(Inode inode : inodes) {
			logger.trace("ZKFS {} {}: Commit inode {} {} size {} at offset {}",
					Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
					Util.formatRevisionTag(zkfs.getBaseRevision()),
					inode.stat.getInodeId(),
					inode.stat.getSize(),
					Util.formatRefTag(inode.getRefTag()),
					this.pos());
			write(inode.serialize());
		}
		if(offset < zkfs.archive.config.pageSize) {
			write(new byte[(int) (zkfs.archive.config.pageSize - (offset % zkfs.archive.config.pageSize))]);
		}
	}
	
	/** calculate the page number for a given inode id */
	protected long pageNumForInodeId(long inodeId) {
		if(inodeId < numInodesForPage(0)) {
			return 0;
		} else {
			// all pages except 0 have same number of inodes
			return 1 + (inodeId - numInodesForPage(0))/numInodesForPage(1);
		}
	}
	
	/** calculate the index into a page for a given inode id */
	protected int pageOffsetForInodeId(long inodeId) {
		if(inodeId < numInodesForPage(0)) {
			return (int) inodeId;
		} else {
			return (int) ((inodeId - numInodesForPage(0)) % numInodesForPage(1));
		}
	}
	
	protected int numInodesForPage(long pageNum) {
		if(pageNum == 0) {
			return (zkfs.archive.config.pageSize - RevisionInfo.FIXED_SIZE)/inodeSize();
		} else {
			return zkfs.archive.config.pageSize/inodeSize();
		}
	}
	
	public long nextInodeId() throws IOException {
		if(nextInodeId < 0) {
			nextInodeId = lookupNextInodeId();
		}
		
		return nextInodeId;
	}
	
	/** place an inode into the table, overwriting what's already there (assumes inode id is properly set) */
	protected synchronized void setInode(Inode inode) throws IOException {
		/* if we're closing, we're not creating a new revision; and if we're not making a new revision, we
		 * don't need to be making inode table changes... (dodges some ClosedException issues)
		 */
		if(closing) return;
		
		Inode existing = inodeWithId(inode.getStat().getInodeId());
		if(existing == inode) return; // inode is already set
		logger.trace("ZKFS {} {}: Replacing contents for inode {}, identity {} -> {}",
				Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(zkfs.baseRevision),
				inode.getStat().getInodeId(),
				existing.getIdentity(),
				inode.getIdentity());
		existing.deserialize(inode.serialize());
		zkfs.markDirty();
	}
	
	/** initialize an empty root directory */
	private void makeRootDir() throws IOException {
		Inode rootDir = new Inode(zkfs);
		long now = Util.currentTimeNanos();
		rootDir.setIdentity(0);
		rootDir.getStat().setCtime(now);
		rootDir.getStat().setAtime(now);
		rootDir.getStat().setMtime(now);
		rootDir.getStat().setInodeId(INODE_ID_ROOT_DIRECTORY);
		rootDir.getStat().makeDirectory();
		rootDir.getStat().setMode(0777);
		rootDir.getStat().setUid(0);
		rootDir.getStat().setGid(0);
		rootDir.setRefTag(RefTag.blank(zkfs.archive));
		rootDir.setFlags(Inode.FLAG_RETAIN);
		setInode(rootDir);
	}
	
	/** initialize a blank top-level revision (i.e. one that has no ancestors) */
	private void makeEmptyRevision() throws IOException {
		ArrayList<RevisionTag> parents = new ArrayList<>();
		revision = new RevisionInfo(this, parents, 0, zkfs.getArchive().getConfig().getDescription());
	}
	
	/** initialize a blank freelist */
	private void makeEmptyFreelist() throws IOException {
		Inode freelistInode = issueInode(INODE_ID_FREELIST);
		freelistInode.setFlags(Inode.FLAG_RETAIN);
		this.freelist = new FreeList(freelistInode);
		setInode(freelistInode);
	}
	
	/** initialize a blank inode table, including references to blank root directroy, revision info and freelist */
	private void initialize() throws IOException {
		this.inode = Inode.defaultRootInode(zkfs);
		this.setInode(this.inode);
		this.tree = new PageTree(RefTag.blank(zkfs.archive));
		this.nextInodeId = USER_INODE_ID_START;

		makeRootDir();
		makeEmptyRevision();
		makeEmptyFreelist();
	}
	
	/** initialize from existing inode table data, identified by a reftag */
	private void readExisting(RevisionTag tag) throws IOException {
		logger.debug("ZKFS {} {}: Loading existing inode table from {}",
				Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(tag),
				Util.formatRefTag(tag.getRefTag()));
		this.tree = new PageTree(tag.getRefTag());
		this.inode = new Inode(zkfs);
		this.inode.setRefTag(tag.getRefTag());
		this.inode.setFlags(Inode.FLAG_RETAIN);
		this.inode.stat.setSize(zkfs.archive.config.pageSize * tag.getRefTag().numPages);
		this.revision = readRevisionInfo();
		this.freelist = new FreeList(inodeWithId(INODE_ID_FREELIST)); // doesn't actually read anything yet
		nextInodeId = -1; // causes nextInodeId() to read from table on next invocation
		// zkfs.archive.config.revisionTree.addParentsForTag(tag, revision.parents);
	}
	
	/** apply a resolution to an inode conflict from an InodeDiff */
	public void replaceInode(InodeDiff inodeDiff) throws IOException {
		assert(inodeDiff.isResolved());
		if(inodeDiff.getResolution() == null) {
			inodeWithId(inodeDiff.getInodeId()).markDeleted();
		} else {
			Inode existing = inodeWithId(inodeDiff.getInodeId());
			Inode duplicated = inodeDiff.getResolution().clone(zkfs);
			
			/* anything to do with path structure, we can ignore. that means: nlink for all inodes, and refTag/size
			 * for directories. Directory structure is recalculated during merge, altering directory contents and
			 * inode nlinks.
			 */
			if(existing != null) {
				duplicated.nlink = existing.nlink;
				if(duplicated.stat.isDirectory() && existing.identity == inodeDiff.getResolution().identity) {
					duplicated.refTag = existing.refTag;
					duplicated.stat.setSize(existing.stat.getSize());
				}
			} else {
				// TODO API: (coverage) branch
				duplicated.nlink = 0;
				if(duplicated.stat.isDirectory()) {
					duplicated.refTag = RefTag.blank(zkfs.archive);
					duplicated.stat.setSize(0);
				}
			}
			
			// make sure we retain existing instances, to keep caches square
			setInode(duplicated);
		}
	}
	
	public String dumpInodes() throws IOException {
		String s = "";
		s += "Inode table dump for " + Util.formatRevisionTag(zkfs.baseRevision) + ", nextInodeId=" + nextInodeId() + ", dirty=" + dirty + "\n";
		for(int i = 0; i < nextInodeId(); i++) {
			Inode inode = inodeWithId(i);
			s += String.format("\tinodeId %4d: identity %16x, %s, size %7d, nlink %02d, type %02x, changedfrom %s\n",
					inode.stat.getInodeId(),
					inode.identity,
					Util.formatRefTag(inode.refTag),
					inode.getStat().getSize(),
					inode.nlink,
					inode.getStat().getType(),
					Util.formatRevisionTag(inode.changedFrom));
		}
		
		return s;
	}
	
	/** Iteraable for all inodes in the table */
	public Iterable<Inode> values() {
		@SuppressWarnings("resource")
		InodeTable self = this;
		return () -> {
			return new Iterator<Inode>() {
				long nextId = 0;
				
				@Override
				public boolean hasNext() {
					try {
						return nextId < self.nextInodeId();
					} catch(IOException exc) {
						return false;
					}
				}
				
				@Override
				public Inode next() {
					Inode next;
					try {
						next = inodeWithId(nextId);
						advance();
					} catch (IOException e) {
						throw new RuntimeException("Error iterating over inodes");
					}
					
					return next;
				}
				
				private void advance() throws IOException {
					while(hasNext()) {
						if(!inodeWithId(++nextId).isDeleted()) return;
					}
				}
			};
		};
	}

	public synchronized void uncache() throws IOException {
		logger.info("ZKFS {} {}: Purging inode table cache, has {} pages",
				Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(zkfs.getBaseRevision()),
				inodesByPage.cachedSize());
		inodesByPage.removeAll();
	}
}
