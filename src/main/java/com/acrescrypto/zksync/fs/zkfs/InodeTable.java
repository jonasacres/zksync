package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Hashtable;

import com.acrescrypto.zksync.exceptions.EMLINKException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.resolver.InodeDiff;

// represents inode table for ZKFS instance. 
public class InodeTable extends ZKFile {
	public final static long INODE_ID_INODE_TABLE = 0;
	public final static long INODE_ID_ROOT_DIRECTORY = 1;
	public static final long INODE_ID_REVISION_INFO = 2;

	public final static long USER_INODE_ID_START = 16;
	
	public final static String INODE_TABLE_PATH = "(inode table)";
	
	// TODO: fixed-length inode serialization to support partial inode table reads (might be a good next thing to look at...)
	
	protected Hashtable<Long,Inode> inodes;
	protected RevisionInfo revision;
	protected long nextInodeId;
	
	public InodeTable(ZKFS fs, RefTag tag) throws IOException {
		this.fs = fs;
		this.path = INODE_TABLE_PATH;
		this.mode = O_RDWR;
		this.inodes = new Hashtable<Long,Inode>();
		
		if(tag.isBlank()) {
			initialize();
		} else {
			this.merkel = new PageMerkel(tag);
			this.inode = new Inode(fs);
			this.inode.setRefTag(tag);
			inferSize(tag);
			readTable();
		}
	}

	public RefTag commit(RefTag[] additionalParents) throws IOException {
		rewind();
		truncate(0);
		
		RevisionInfo newRevision = new RevisionInfo(fs);
		newRevision.reset();
		for(RefTag parent : additionalParents) newRevision.addParent(parent);
		newRevision.commit();

		for(Inode inode : inodes.values()) {
			if(inode.getStat().getInodeId() == InodeTable.INODE_ID_INODE_TABLE) continue;
			write(inode.serialize());
		}
		
		flush();
		revision = newRevision;
		RefTag tag = inode.getRefTag();
		RevisionTree tree = fs.archive.getRevisionTree();
		tree.addBranchTip(tag);
		tree.removeBranchTip(fs.baseRevision);
		for(RefTag parent : additionalParents) tree.removeBranchTip(parent);
		tree.write();
		return tag;
	}
	
	public int inodeSize() {
		// TODO: consider leaving some space to grow...
		return Stat.STAT_SIZE + 2*8 + 1*4 + 1 + 2*(fs.archive.refTagSize());
	}
	
	public void readTable() throws IOException {
		rewind();
		ByteBuffer buf = ByteBuffer.allocate(inodeSize());
		while(hasData()) {
			buf.clear();
			
			int readLen = read(buf.array(), 0, inodeSize());
			if(readLen < inodeSize()) throw new InvalidArchiveException("Inode table ended prematurely");
			
			Inode inode = new Inode(fs, buf.array());
			long inodeId = inode.getStat().getInodeId();
			if(inodeId >= nextInodeId) nextInodeId = inodeId+1;
			inodes.put(inodeId, inode);
		}
	}
	
	public void unlink(long inodeId) throws ENOENTException, EMLINKException {
		if(!inodes.containsKey(inodeId)) throw new ENOENTException(String.format("inode %d", inodeId));
		
		if(inodeId <= 1) throw new IllegalArgumentException();		
		if(inodes.get(inodeId).getNlink() > 0) {
			throw new EMLINKException(String.format("inode %d", inodeId));
		}
		
		inodes.remove(inodeId);
	}
	
	public Inode inodeWithId(long inodeId) throws ENOENTException {
		if(!inodes.containsKey(inodeId)) {
			throw new ENOENTException(String.format("inode %d", inodeId));
		}
		return inodes.get(inodeId);
	}
	
	public boolean hasInodeWithId(long inodeId) {
		return inodes.containsKey(inodeId);
	}
	
	public long issueInodeId() {
		return nextInodeId++;
	}
	
	public Inode issueInode() {
		Inode inode = new Inode(fs);
		long now = 1000l*1000l*System.currentTimeMillis();
		inode.setIdentity(ByteBuffer.wrap(fs.archive.crypto.rng(8)).getLong()); // TODO: the new last gasp of non-determinism...
		inode.getStat().setInodeId(issueInodeId());
		inode.getStat().setCtime(now);
		inode.getStat().setAtime(now);
		inode.getStat().setMtime(now);
		inode.getStat().setMode(fs.archive.localConfig.getFileMode());
		inode.getStat().setUser(fs.archive.localConfig.getUser());
		inode.getStat().setUid(fs.archive.localConfig.getUid());
		inode.getStat().setGroup(fs.archive.localConfig.getGroup());
		inode.getStat().setGid(fs.archive.localConfig.getGid());
		inode.setRefTag(RefTag.blank(fs.archive));
		inodes.put(inode.getStat().getInodeId(), inode);
		return inode;
	}
	
	private void makeRootDir() {
		Inode rootDir = new Inode(fs);
		long now = 1000l*1000l*System.currentTimeMillis();
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
		inodes.put(rootDir.getStat().getInodeId(), rootDir);
	}
	
	private void makeEmptyRevision() {
		Inode revfile = issueInode();
		revfile.getStat().setInodeId(INODE_ID_REVISION_INFO);
		inodes.put(revfile.getStat().getInodeId(), revfile);
	}
	
	private void initialize() throws IOException {
		this.inode = Inode.defaultRootInode(fs);
		this.inodes.put(INODE_ID_INODE_TABLE, this.inode);
		this.merkel = new PageMerkel(RefTag.blank(fs.archive));
		this.nextInodeId = USER_INODE_ID_START;

		makeRootDir();
		makeEmptyRevision();
	}

	public void replaceInode(InodeDiff inodeDiff) {
		assert(inodeDiff.isResolved());
		if(inodeDiff.getResolution() == null) {
			inodes.remove(inodeDiff.getInodeId());
		} else {
			Inode existing = inodes.getOrDefault(inodeDiff.getInodeId(), null);
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
			inodes.putIfAbsent(inodeDiff.getInodeId(), new Inode(fs));
			inodes.get(inodeDiff.getInodeId()).deserialize(duplicated.serialize());
		}
	}
	
	public Hashtable<Long,Inode> getInodes() {
		return inodes;
	}
}
