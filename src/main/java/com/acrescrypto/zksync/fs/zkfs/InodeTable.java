package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.acrescrypto.zksync.HashCache;
import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.exceptions.EMLINKException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.resolver.InodeDiff;

// represents inode table for ZKFS instance. 
public class InodeTable extends ZKFile {
	public final static long INODE_ID_INODE_TABLE = 0;
	public final static long INODE_ID_ROOT_DIRECTORY = 1;
	public static final long INODE_ID_REVISION_INFO = 2;

	public final static long USER_INODE_ID_START = 16;
	
	public final static String INODE_TABLE_PATH = "(inode table)";
	
	protected RevisionInfo revision;
	public long nextInodeId;
	
	protected HashCache<Long,Inode[]> inodesByPage;
	
	public static int inodeSize(ZKArchive archive) {
		return Stat.STAT_SIZE + 2*8 + 1*4 + 1 + 2*(archive.refTagSize());		
	}
	
	public InodeTable(ZKFS fs, RefTag tag) throws IOException {
		this.fs = fs;
		this.path = INODE_TABLE_PATH;
		this.mode = O_RDWR;
		this.inodesByPage = new HashCache<Long,Inode[]>(16, (Long pageNum) -> {
			return inodesForPage(pageNum);
		}, (Long pageNum, Inode[] inodes) -> {
			commitInodePage(pageNum, inodes);
		});
		
		if(tag.isBlank()) {
			initialize();
		} else {
			this.merkel = new PageMerkel(tag);
			this.inode = new Inode(fs);
			this.inode.setRefTag(tag);
			inferSize(tag); // TODO: we may not need this; size will always be numPages*pageSize
			nextInodeId = lookupNextInodeId();
		}
	}
	
	protected long lookupNextInodeId() throws IOException {
		Inode[] inodes = inodesByPage.get(inode.refTag.numPages-1);
		
		long maxInodeId = USER_INODE_ID_START;
		for(Inode inode : inodes) {
			if(inode.isDeleted()) continue;
			maxInodeId = Math.max(maxInodeId, inode.stat.getInodeId());
		}
		
		return maxInodeId+1;
	}

	public RefTag commit(RefTag[] additionalParents) throws IOException {
		updateRevisionInfo(additionalParents);
		syncInodes();
		updateTree(additionalParents);
		return inode.refTag;
	}
	
	protected void updateRevisionInfo(RefTag[] additionalParents) throws IOException {
		RevisionInfo newRevision = new RevisionInfo(fs);
		newRevision.reset();
		for(RefTag parent : additionalParents) newRevision.addParent(parent);
		newRevision.commit();
		revision = newRevision;
	}
	
	protected void syncInodes() throws IOException {
		for(Long pageNum : inodesByPage.cachedKeys()) {
			commitInodePage(pageNum, inodesByPage.get(pageNum));
		}
		flush();
	}
	
	protected void updateTree(RefTag[] additionalParents) throws IOException {
		RefTag tag = inode.getRefTag();
		RevisionTree tree = fs.archive.getRevisionTree();
		tree.addBranchTip(tag);
		tree.removeBranchTip(fs.baseRevision);
		for(RefTag parent : additionalParents) tree.removeBranchTip(parent);
		tree.write();
	}
	
	public int inodeSize() {
		return inodeSize(fs.archive);
	}
	
	public void unlink(long inodeId) throws IOException {
		if(!hasInodeWithId(inodeId)) throw new ENOENTException(String.format("inode %d", inodeId));
		if(inodeId <= 1) throw new IllegalArgumentException();

		Inode inode = inodeWithId(inodeId);
		if(inode.nlink > 0) {
			throw new EMLINKException(String.format("inode %d", inodeId));
		}
		
		inode.markDeleted();
		// TODO: add inode id to freelist
	}
	
	public Inode inodeWithId(long inodeId) throws IOException {
		return inodesByPage.get(pageNumForInodeId(inodeId))[pageOffsetForInodeId(inodeId)];
	}
	
	public boolean hasInodeWithId(long inodeId) {
		return inodeId <= nextInodeId;
	}
	
	public long issueInodeId() {
		// TODO: pull inode id from freelist
		return nextInodeId++;
	}
	
	public Inode issueInode() throws IOException {
		return issueInode(issueInodeId());
	}
	
	public Inode issueInode(long inodeId) throws IOException {
		Inode inode = inodeWithId(inodeId);
		long now = fs.currentTime();
		inode.setIdentity(ByteBuffer.wrap(fs.archive.crypto.rng(8)).getLong()); // TODO: the new last gasp of non-determinism...
		inode.getStat().setInodeId(inodeId);
		inode.getStat().setCtime(now);
		inode.getStat().setAtime(now);
		inode.getStat().setMtime(now);
		inode.getStat().setMode(fs.archive.localConfig.getFileMode());
		inode.getStat().setUser(fs.archive.localConfig.getUser());
		inode.getStat().setUid(fs.archive.localConfig.getUid());
		inode.getStat().setGroup(fs.archive.localConfig.getGroup());
		inode.getStat().setGid(fs.archive.localConfig.getGid());
		inode.setRefTag(RefTag.blank(fs.archive));
		return inode;
	}
	
	protected Inode[] inodesForPage(long pageNum) throws IOException {
		Inode[] list = new Inode[inodesPerPage()];
		seek(pageNum*fs.archive.privConfig.getPageSize(), SEEK_SET);
		ByteBuffer buf = ByteBuffer.wrap(read(fs.archive.privConfig.getPageSize()));
		byte[] serialized = new byte[inodeSize()];
		int i = 0;
		
		while(buf.remaining() >= serialized.length) {
			buf.get(serialized);
			list[i++] = new Inode(fs, serialized);
		}
		
		while(i < list.length) list[i++] = new Inode(fs);
		if(pageNum == 0) list[0] = inode;
		
		return list;
	}
	
	protected void commitInodePage(long pageNum, Inode[] inodes) throws IOException {
		seek(pageNum*fs.archive.privConfig.getPageSize(), SEEK_SET);
		for(Inode inode : inodes) write(inode.serialize());
	}
	
	protected long pageNumForInodeId(long inodeId) {
		return inodeId/inodesPerPage();
	}
	
	protected int pageOffsetForInodeId(long inodeId) {
		return (int) (inodeId % inodesPerPage());
	}
	
	protected int inodesPerPage() {
		return fs.archive.privConfig.getPageSize()/inodeSize();
	}
	
	protected void setInode(Inode inode) throws IOException {
		Inode existing = inodeWithId(inode.getStat().getInodeId());
		existing.deserialize(inode.serialize());
	}
	
	private void makeRootDir() throws IOException {
		Inode rootDir = new Inode(fs);
		long now = fs.currentTime();
		rootDir.setIdentity(0);
		rootDir.getStat().setCtime(now);
		rootDir.getStat().setAtime(now);
		rootDir.getStat().setMtime(now);
		rootDir.getStat().setInodeId(INODE_ID_ROOT_DIRECTORY);
		rootDir.getStat().makeDirectory();
		rootDir.getStat().setMode(0777);
		rootDir.getStat().setUid(0);
		rootDir.getStat().setGid(0);
		rootDir.setRefTag(RefTag.blank(fs.archive));
		rootDir.setFlags(Inode.FLAG_RETAIN);
		setInode(rootDir);
	}
	
	private void makeEmptyRevision() throws IOException {
		Inode revfile = issueInode(INODE_ID_REVISION_INFO);
		setInode(revfile);
	}
	
	private void initialize() throws IOException {
		this.inode = Inode.defaultRootInode(fs);
		this.setInode(this.inode);
		this.merkel = new PageMerkel(RefTag.blank(fs.archive));
		this.nextInodeId = USER_INODE_ID_START;

		makeRootDir();
		makeEmptyRevision();
	}

	public void replaceInode(InodeDiff inodeDiff) throws IOException {
		assert(inodeDiff.isResolved());
		if(inodeDiff.getResolution() == null) {
			inodeWithId(inodeDiff.getInodeId()).markDeleted();
		} else {
			Inode existing = inodeWithId(inodeDiff.getInodeId());
			Inode duplicated = inodeDiff.getResolution().clone(fs);
			
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
					duplicated.refTag = RefTag.blank(fs.archive);
					duplicated.stat.setSize(0);
				}
			}
			
			// make sure we retain existing instances, to keep caches square
			setInode(duplicated);
		}
	}
	
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
