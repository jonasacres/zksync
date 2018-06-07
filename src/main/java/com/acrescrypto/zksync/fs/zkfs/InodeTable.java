package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

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
	
	/** metadata about the current revision such as immediate ancestors and generation number */
	public final static long INODE_ID_REVISION_INFO = 2;
	
	/** freelist of avaialble inode IDs */
	public final static long INODE_ID_FREELIST = 3;

	/** first inode ID issued to actual files */
	public final static long USER_INODE_ID_START = 16;
	
	/** fake value used to fill path field */
	public final static String INODE_TABLE_PATH = "(inode table)";
	
	protected RevisionInfo revision; /** current revision metadata */
	protected FreeList freelist;
	public long nextInodeId; /** next inode ID to be issued when freelist is exhausted */
	
	protected HashCache<Long,Inode[]> inodesByPage; /** in-memory cache of inode data */
	
	/** serialized size of an inode for a given archive, in bytes */
	public static int inodeSize(ZKArchive archive) {
		return Stat.STAT_SIZE + 2*8 + 1*4 + 1 + 2*(archive.refTagSize());		
	}
	
	/** initialize inode table for FS.
	 * 
	 * @param fs filesystem to init inode table for
	 * @param tag tag for revision to be loaded. if blank tag supplied, a blank inode table is created.
	 * @throws IOException
	 */
	public InodeTable(ZKFS fs, RefTag tag) throws IOException {
		super(fs);
		this.path = INODE_TABLE_PATH;
		this.mode = O_RDWR;
		this.inodesByPage = new HashCache<Long,Inode[]>(16, (Long pageNum) -> {
			return inodesForPage(pageNum);
		}, (Long pageNum, Inode[] inodes) -> {
			commitInodePage(pageNum, inodes);
		});
		
		if(tag.isBlank()) initialize();
		else readExisting(tag);
	}
	
	/** calculate the next inode ID to be issued (by scanning for the largest issued inode ID) */
	protected long lookupNextInodeId() throws IOException {
		Inode[] inodes = inodesByPage.get(inode.refTag.numPages-1);
		
		long maxInodeId = USER_INODE_ID_START;
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
	public RefTag commit(RefTag[] additionalParents) throws IOException {
		freelist.commit();
		updateRevisionInfo(additionalParents);
		syncInodes();
		updateTree(additionalParents);
		return inode.refTag;
	}
	
	/** clear the old revision info and replace with a new one */
	protected void updateRevisionInfo(RefTag[] additionalParents) throws IOException {
		 // TODO: figure out if we need to add the current revision tag here
		RevisionInfo newRevision = new RevisionInfo(zkfs);
		newRevision.reset();
		for(RefTag parent : additionalParents) newRevision.addParent(parent);
		newRevision.commit();
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
	protected void updateTree(RefTag[] additionalParents) throws IOException {
		RefTag tag = inode.getRefTag();
		RevisionTree tree = zkfs.archive.getRevisionTree();
		tree.addBranchTip(tag);
		tree.removeBranchTip(zkfs.baseRevision);
		for(RefTag parent : additionalParents) tree.removeBranchTip(parent);
		tree.write();
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
		
		inode.markDeleted();
		freelist.freeInodeId(inodeId);
	}
	
	/** return an inode with a given ID */
	public Inode inodeWithId(long inodeId) throws IOException {
		return inodesByPage.get(pageNumForInodeId(inodeId))[pageOffsetForInodeId(inodeId)];
	}
	
	/** test if table contains an inode with the given ID */
	public boolean hasInodeWithId(long inodeId) {
		// TODO: consider checking freelist?
		return inodeId <= nextInodeId;
	}
	
	/** issue next inode ID (draw from freelist if available, or issue next sequential ID if freelist is empty) */
	public long issueInodeId() throws IOException {
		try {
			return freelist.issueInodeId();
		} catch(FreeListExhaustedException exc) {
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
		Inode[] list = new Inode[inodesPerPage()];
		seek(pageNum*zkfs.archive.config.pageSize, SEEK_SET);
		ByteBuffer buf = ByteBuffer.wrap(read((int) zkfs.archive.config.pageSize));
		byte[] serialized = new byte[inodeSize()];
		int i = 0;
		
		while(buf.remaining() >= serialized.length && buf.hasRemaining()) {
			buf.get(serialized);
			list[i++] = new Inode(zkfs, serialized);
		}
		
		while(i < list.length) list[i++] = new Inode(zkfs);
		if(pageNum == 0) list[0] = inode;
		
		return list;
	}
	
	/** write an array of inodes to a given page number */
	protected void commitInodePage(long pageNum, Inode[] inodes) throws IOException {
		seek(pageNum*zkfs.archive.config.pageSize, SEEK_SET);
		for(Inode inode : inodes) write(inode.serialize());
		write(new byte[(int) (zkfs.archive.config.pageSize - (offset % zkfs.archive.config.pageSize))]);
	}
	
	/** calculate the page number for a given inode id */
	protected long pageNumForInodeId(long inodeId) {
		return inodeId/inodesPerPage();
	}
	
	/** calculate the index into a page for a given inode id */
	protected int pageOffsetForInodeId(long inodeId) {
		return (int) (inodeId % inodesPerPage());
	}
	
	protected int inodesPerPage() {
		return zkfs.archive.config.pageSize/inodeSize();
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
		Inode revfile = issueInode(INODE_ID_REVISION_INFO);
		revfile.setFlags(Inode.FLAG_RETAIN);
		setInode(revfile);
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
		this.merkle = new PageMerkle(RefTag.blank(zkfs.archive));
		this.nextInodeId = USER_INODE_ID_START;

		makeRootDir();
		makeEmptyRevision();
		makeEmptyFreelist();
	}
	
	/** initialize from existing inode table data, identified by a reftag */
	private void readExisting(RefTag tag) throws IOException {
		this.merkle = new PageMerkle(tag);
		this.merkle.assertExists();
		this.inode = new Inode(zkfs);
		this.inode.setRefTag(tag);
		this.inode.stat.setSize(zkfs.archive.config.pageSize * tag.numPages);
		this.revision = new RevisionInfo(inodeWithId(INODE_ID_REVISION_INFO));
		this.freelist = new FreeList(inodeWithId(INODE_ID_FREELIST));
		nextInodeId = lookupNextInodeId();
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
	
	/** Iteraable for all inodes in the table */
	public Iterable<Inode> values() {
		@SuppressWarnings("resource")
		InodeTable self = this;
		return () -> {
			return new Iterator<Inode>() {
				long nextId = 0;
				
				@Override
				public boolean hasNext() {
					return nextId < self.nextInodeId;
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
