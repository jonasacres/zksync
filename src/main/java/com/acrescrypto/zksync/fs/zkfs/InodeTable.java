package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import com.acrescrypto.zksync.HashCache;
import com.acrescrypto.zksync.exceptions.EMLINKException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.zkfs.resolver.InodeDiff;

// represents inode table for ZKFS instance. 
public class InodeTable extends ZKFile {
	public final static long INODE_ID_INODE_TABLE = 0;
	public final static long INODE_ID_ROOT_DIRECTORY = 1;
	public static final long INODE_ID_REVISION_INFO = 2;

	public final static long USER_INODE_ID_START = 16;
	
	public final static String INODE_TABLE_PATH = "(inode table)";
	
	protected HashCache<Integer,Inode[]> pages;
	protected RevisionInfo revision;
	protected long nextInodeId;
	
	public InodeTable(ZKFS fs, RefTag tag) throws IOException {
		this.fs = fs;
		this.path = INODE_TABLE_PATH;
		this.mode = O_RDWR;
		this.pages = new HashCache<Integer,Inode[]>(64, (Integer pageNum) -> { // TODO: put capacity in localconfig
			Inode[] inodes = new Inode[inodesPerPage()];

			int offset = pageNum*fs.archive.getPrivConfig().getPageSize();
			if(offset > inode.getStat().getSize()) throw new ENOENTException("inode table page " + pageNum);
			if(offset == inode.getStat().getSize()) {
				blankInodeList(pageNum, inodes);
				return inodes;
			}
			
			seek(offset, ZKFile.SEEK_SET);
			ByteBuffer buf = ByteBuffer.wrap(read(fs.archive.getPrivConfig().getPageSize()));
			int i = 0;
			byte[] serialized = new byte[inodeSize()];
			
			while(buf.remaining() >= inodeSize()) {
				buf.get(serialized);
				inodes[i++] = new Inode(this.fs, serialized);
			}
			
			return inodes;
		}, (Integer pageNum, Inode[] inodes) -> {
			ByteBuffer buf = ByteBuffer.allocate(fs.archive.getPrivConfig().getPageSize());
			for(Inode inode : inodes) buf.put(inode.serialize());
			seek(pageNum*fs.archive.getPrivConfig().getPageSize(), ZKFile.SEEK_SET);
			write(buf.array());
		});
		
		if(tag.isBlank()) {
			initialize();
		} else {
			this.merkel = new PageMerkel(tag);
			this.inode = new Inode(fs);
			this.inode.setRefTag(tag);
			inferSize(tag);
		}
	}

	private void blankInodeList(Integer pageNum, Inode[] inodes) {
		for(int i = 0; i < inodes.length; i++) {
			inodes[i] = new Inode(fs);
			inodes[i].stat.setInodeId(pageNum*fs.archive.privConfig.getPageSize()/inodeSize()+i);
			inodes[i].markDeleted();
		}
	}

	public RefTag commit(RefTag[] additionalParents) throws IOException {
		pages.reset();
		flush();
		revision = writeRevision(additionalParents);
		return updateBranchTips(additionalParents);
	}
	
	protected RevisionInfo writeRevision(RefTag[] additionalParents) throws IOException {
		RevisionInfo newRevision = new RevisionInfo(fs);
		newRevision.reset();
		for(RefTag parent : additionalParents) newRevision.addParent(parent);
		newRevision.commit();
		return newRevision;
	}
	
	protected RefTag updateBranchTips(RefTag[] additionalParents) throws IOException {
		RefTag tag = inode.getRefTag();
		RevisionTree tree = fs.archive.getRevisionTree();
		tree.addBranchTip(tag);
		tree.removeBranchTip(fs.baseRevision);
		for(RefTag parent : additionalParents) tree.removeBranchTip(parent);
		tree.write();
		return tag;
	}
	
	public int inodeSize() {
		return Inode.sizeForFs(fs);
	}
	
	public void unlink(long inodeId) throws IOException {
		if(inodeId <= 1) throw new IllegalArgumentException();		

		Inode inode = inodeWithId(inodeId);
		if(inode.getNlink() > 0) throw new EMLINKException(String.format("inode %d", inodeId));
		// TODO: this is where we're going to add to the freelist when we add that
		// TODO: also, truncate the file if this is our last inode
	}
	
	public Inode inodeWithId(long inodeId) throws IOException {
		Inode inode = pages.get(pageNumForInodeId(inodeId))[offsetForInodeId(inodeId)];
		if(inode.isDeleted()) throw new ENOENTException("inode " + inodeId);
		return inode;
	}
	
	public int size() {
		return (int) (inode.getStat().getSize()/inodeSize());
	}
	
	public Inode[] pageForInodeId(long inodeId) throws IOException {
		return pages.get(pageNumForInodeId(inodeId));
	}
	
	protected int inodesPerPage() {
		return fs.archive.privConfig.getPageSize()/inodeSize();
	}
	
	protected int pageNumForInodeId(long inodeId) {
		return (int) (inodeId-1) / inodesPerPage();
	}
	
	protected int offsetForInodeId(long inodeId) {
		return (int) (inodeId-1) % inodesPerPage();
	}
	
	public boolean hasInodeWithId(long inodeId) throws IOException {
		try {
			inodeWithId(inodeId);
			return true;
		} catch(ENOENTException exc) {
			return false;
		}
	}
	
	public long issueInodeId() {
		// TODO: draw from the freelist here once we add that
		return nextInodeId++;
	}
	
	public Inode issueInode() throws IOException {
		return issueInode(issueInodeId());
	}
	
	public Inode issueInode(long inodeId) throws IOException {
		Inode inode = pages.get(pageNumForInodeId(inodeId))[offsetForInodeId(inodeId)];
		assert(inode.isDeleted());
		long now = 1000l*1000l*System.currentTimeMillis();
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
	
	private void makeRootDir() throws IOException {
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
		setInode(INODE_ID_ROOT_DIRECTORY, rootDir);
	}
	
	private void makeEmptyRevision() throws IOException {
		Inode revfile = issueInode();
		revfile.getStat().setInodeId(INODE_ID_REVISION_INFO);
		setInode(revfile.getStat().getInodeId(), revfile);
	}
	
	private void initialize() throws IOException {
		this.inode = Inode.defaultRootInode(fs);
		this.merkel = new PageMerkel(RefTag.blank(fs.archive));
		this.nextInodeId = USER_INODE_ID_START;

		makeRootDir();
		makeEmptyRevision();
	}

	public void replaceInode(InodeDiff inodeDiff) throws IOException {
		assert(inodeDiff.isResolved());
		if(inodeDiff.getResolution() == null) {
			unlink(inodeDiff.getInodeId()); // TODO: we may need an unlinkUnsafe that doesn't do nlink check
		} else {
			Inode existing;
			try {
				existing = inodeWithId(inodeDiff.getInodeId());
			} catch(ENOENTException exc) { existing = null; }
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
			
			setInode(inodeDiff.getInodeId(), duplicated);
		}
	}
	
	protected void setInode(long inodeId, Inode inode) throws IOException {
		Inode[] page = pageForInodeId(inodeId);
		page[offsetForInodeId(inodeId)].deserialize(inode.serialize()); // maintain existing reference for caches
	}
	
	public Iterable<Inode> values() {
		@SuppressWarnings("resource")
		InodeTable table = this;
		return () -> {
			return new Iterator<Inode>() {
				int currentInodeId = 1; // inode 0 (inode table) is not included in serialization
				
				@Override
				public boolean hasNext() {
					return currentInodeId < table.size();
				}
				
				@Override
				public Inode next() {
					try {
						return table.inodeWithId(currentInodeId++);
					} catch (IOException e) {
						throw new RuntimeException("unable to get inode " + (currentInodeId-1));
					}
				}
			};
		};
	}
}
