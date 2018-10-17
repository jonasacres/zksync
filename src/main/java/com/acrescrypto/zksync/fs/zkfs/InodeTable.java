package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import com.acrescrypto.zksync.crypto.HashContext;
import com.acrescrypto.zksync.exceptions.EMLINKException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
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
		this.trusted = false;
		this.path = INODE_TABLE_PATH;
		this.mode = O_RDWR;
		this.inodesByPage = new HashCache<Long,Inode[]>(16, (Long pageNum) -> {
			return inodesForPage(pageNum);
		}, (Long pageNum, Inode[] inodes) -> {
			commitInodePage(pageNum, inodes);
		});
		
		if(tag.refTag.isBlank()) initialize();
		else readExisting(tag);
	}
	
	/** calculate the next inode ID to be issued (by scanning for the largest issued inode ID) */
	protected long lookupNextInodeId() throws IOException {
		Inode[] inodes = inodesByPage.get(inode.refTag.numPages-1);
		
		long maxInodeId = USER_INODE_ID_START-1;
		for(Inode inode : inodes) {
			if(inode.isDeleted()) continue;
			maxInodeId = Math.max(maxInodeId, inode.stat.getInodeId());
		}
		
		return maxInodeId+1;
	}
	
	/** write inode table to filesystem, creating a new revision
	 * 
	 * @return RefTag for the newly created revision
	 *  */
	public RevisionTag commitWithTimestamp(RevisionTag[] additionalParents, long timestamp) throws IOException {
		// TODO: Objective merge logic breaks down if we have additional parents AND we had changes to the filesystem. Throw an exception if someone tries to do that.
		// TODO: I regret doing these as arrays instead of collections. Refactor.
		freelist.commit();
		ArrayList<RevisionTag> parents = makeParentList(additionalParents);
		updateRevisionInfo(parents);
		long parentHash = makeParentHash(parents);
		
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
		updateList(parents);
		
		long baseHeight = zkfs.baseRevision.height;
		RevisionTag revTag = new RevisionTag(inode.refTag, parentHash, 1+baseHeight);
		zkfs.archive.config.revisionTree.addParentsForTag(revTag, parents);
		
		return revTag;
	}
	
	/** clear the old revision info and replace with a new one */
	protected void updateRevisionInfo(Collection<RevisionTag> parents) throws IOException {
		 // TODO: figure out if we need to add the current revision tag here
		RevisionInfo newRevision = new RevisionInfo(this, parents, revision.generation+1);
		revision = newRevision;
	}
	
	/** write out all cached inodes */
	protected void syncInodes() throws IOException {
		for(Long pageNum : inodesByPage.cachedKeys()) {
			commitInodePage(pageNum, inodesByPage.get(pageNum));
		}
		flush();
	}
	
	/** add our new commit to the list of branch tips, and remove our ancestors */
	protected void updateList(ArrayList<RevisionTag> parents) throws IOException {
		long parentHash = makeParentHash(parents);
		long height = 1 + zkfs.baseRevision.height;
		RevisionTag tag = new RevisionTag(inode.getRefTag(), parentHash, height);	
		RevisionList list = zkfs.archive.config.getRevisionList();
		RevisionTree tree = zkfs.archive.config.getRevisionTree();
		tree.addParentsForTag(tag, parents);		
		list.addBranchTip(tag);
		list.consolidate(tag);
		
		list.write();
		zkfs.archive.config.swarm.announceTips();
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
	
	protected long makeParentHash(ArrayList<RevisionTag> parents) {
		HashContext ctx = zkfs.archive.crypto.startHash();
		parents.sort(null);
		for(RevisionTag parent : parents) {
			ctx.update(parent.getBytes());
		}
		
		return Util.shortTag(ctx.finish());
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
		
		// TODO: check to ensure we are not already deleted before adding to freelist?
		inode.markDeleted();
		freelist.freeInodeId(inodeId);
	}
	
	/** return an inode with a given ID */
	public Inode inodeWithId(long inodeId) throws IOException {
		return inodesByPage.get(pageNumForInodeId(inodeId))[pageOffsetForInodeId(inodeId)];
	}
	
	/** test if table contains an inode with the given ID 
	 * @throws IOException */
	public boolean hasInodeWithId(long inodeId) throws IOException {
		// TODO: consider checking freelist?
		return inodeId <= nextInodeId();
	}
	
	/** issue next inode ID (draw from freelist if available, or issue next sequential ID if freelist is empty) */
	public long issueInodeId() throws IOException {
		try {
			return freelist.issueInodeId();
		} catch(FreeListExhaustedException exc) {
			nextInodeId(); // ensure we have nextInodeId loaded
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
		inode.setIdentity(ByteBuffer.wrap(zkfs.archive.crypto.rng(8)).getLong()); // TODO: the new last gasp of non-determinism...
		inode.getStat().setInodeId(inodeId);
		inode.getStat().setCtime(now);
		inode.getStat().setAtime(now);
		inode.getStat().setMtime(now);
		inode.getStat().setMode(zkfs.archive.localConfig.getFileMode());
		inode.getStat().setUser(zkfs.archive.localConfig.getUser());
		inode.getStat().setUid(zkfs.archive.localConfig.getUid());
		inode.getStat().setGroup(zkfs.archive.localConfig.getGroup());
		inode.getStat().setGid(zkfs.archive.localConfig.getGid());
		inode.setRefTag(RefTag.blank(zkfs.archive));
		return inode;
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
		return new RevisionInfo(this, serializedRevInfo);
	}
	
	/** write an array of inodes to a given page number */
	protected void commitInodePage(long pageNum, Inode[] inodes) throws IOException {
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
		for(Inode inode : inodes) write(inode.serialize());
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
	protected void setInode(Inode inode) throws IOException {
		Inode existing = inodeWithId(inode.getStat().getInodeId());
		if(existing == inode) return; // inode is already set
		existing.deserialize(inode.serialize());
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
		revision = new RevisionInfo(this, parents, 0);
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
		this.tree = new PageTree(tag.refTag);
		this.inode = new Inode(zkfs);
		this.inode.setRefTag(tag.refTag);
		this.inode.setFlags(Inode.FLAG_RETAIN);
		this.inode.stat.setSize(zkfs.archive.config.pageSize * tag.refTag.numPages);
		this.revision = readRevisionInfo();
		this.freelist = new FreeList(inodeWithId(INODE_ID_FREELIST)); // doesn't actually read anything yet
		nextInodeId = -1; // causes nextInodeId() to read from table on next invocation
		zkfs.archive.config.revisionTree.addParentsForTag(tag, revision.parents);
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
				if(duplicated.stat.isDirectory()) {
					duplicated.refTag = existing.refTag;
					duplicated.stat.setSize(existing.stat.getSize());
				}
			} else {
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
	
	public void dumpInodes() throws IOException {
		System.out.println("Inode table for " + zkfs.baseRevision + ", nextInodeId=" + nextInodeId());
		for(int i = 0; i < nextInodeId(); i++) {
			Inode inode = inodeWithId(i);
			System.out.printf("\tInode %d: tag=%s... refType=%d identity=%x hash=%s\n\t\t%s\n",
					i,
					Util.bytesToHex(inode.refTag.getLiteral(), 4),
					inode.refTag.refType,
					inode.identity,
					Util.bytesToHex(zkfs.archive.crypto.hash(inode.serialize()), 4),
					inode.toString());
		}
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
}
