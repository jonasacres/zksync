package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Hashtable;

import com.acrescrypto.zksync.exceptions.EMLINKException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;

// represents inode table for ZKFS instance. 
public class InodeTable extends ZKFile {
	public final static long INODE_ID_INODE_TABLE = 0;
	public final static long INODE_ID_ROOT_DIRECTORY = 1;
	public final static long USER_INODE_ID_START = 10000;
	
	public final static String INODE_TABLE_PATH = "(inode table)";
	
	// TODO: fixed-length inode serialization to support partial inode table reads
	
	protected Hashtable<Long,Inode> inodes;
	protected Revision revision;
	protected long nextInodeId;
	
	public InodeTable(ZKFS fs, Revision revision) throws IOException {
		this.fs = fs;
		this.path = INODE_TABLE_PATH;
		this.mode = O_RDWR;
		
		if(revision != null) {
			this.inode = revision.getSupernode().clone();
			this.merkel = new PageMerkel(fs, this.inode);

		}
		
		this.revision = revision;
		this.inodes = new Hashtable<Long,Inode>();
		
		if(revision == null) {
			initialize();
			return;
		}
		
		readTable();
	}

	public Revision commit() throws IOException {
		rewind();
		truncate(0);
		
		for(Inode inode : inodes.values()) {
			if(inode.getStat().getInodeId() == InodeTable.INODE_ID_INODE_TABLE) continue;
			write(inode.serialize());
		}
		
		flush();
		
		Revision newRevision = new Revision(this);
		if(getRevision() != null && getRevision().getRevTag() != null) newRevision.addParent(getRevision().getRevTag());
		newRevision.write();
		revision = newRevision;
		return newRevision;
	}
	
	public void readTable() throws IOException {
		rewind();
		ByteBuffer buf = ByteBuffer.allocate(1024);
		while(hasData()) {
			buf.clear();
			
			read(buf.array(), 0, 4);
			int size = buf.getInt();
			if(4+size > buf.capacity()) {
				buf = ByteBuffer.allocate(4+size);
				buf.putInt(size);
			}
			
			int readLen = read(buf.array(), buf.position(), size);
			if(readLen < size) throw new InvalidArchiveException("Inode table ended prematurely");
			
			Inode inode = new Inode(fs, buf.array());
			long inodeId = inode.getStat().getInodeId();
			if(inodeId >= nextInodeId) nextInodeId = inodeId; 
			inodes.put(inodeId, inode);
		}
	}
	
	public void unlink(long inodeId) throws ENOENTException, EMLINKException {
		if(!inodes.containsKey(inodeId)) throw new ENOENTException(String.format("inode %d", inodeId));
		
		if(inodeId <= 1) throw new IllegalArgumentException();		
		if(inodes.get(inodeId).getNlink() > 0) throw new EMLINKException(String.format("inode %d", inodeId));
		
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
	
	public Inode issueInode() {
		Inode inode = new Inode(fs);
		long now = 1000l*1000l*System.currentTimeMillis();
		inode.getStat().setInodeId(nextInodeId++);
		inode.getStat().setCtime(now);
		inode.getStat().setAtime(now);
		inode.getStat().setMtime(now);
		inode.getStat().setMode(0640); // TODO: default mode, uid, gid
		inode.setRefTag(new byte[] {});
		inode.setRefType(Inode.REF_TYPE_IMMEDIATE);
		inodes.put(inode.getStat().getInodeId(), inode);
		return inode;
	}
	
	private void makeRootDir() {
		Inode rootDir = new Inode(fs);
		long now = 1000l*1000l*System.currentTimeMillis();
		rootDir.getStat().setInodeId(nextInodeId++);
		rootDir.getStat().setCtime(now);
		rootDir.getStat().setAtime(now);
		rootDir.getStat().setMtime(now);
		rootDir.getStat().setInodeId(INODE_ID_ROOT_DIRECTORY);
		rootDir.getStat().makeDirectory();
		rootDir.getStat().setMode(0777);
		rootDir.getStat().setUid(0);
		rootDir.getStat().setGid(0);
		inodes.put(rootDir.getStat().getInodeId(), rootDir);
	}
	
	private void initialize() throws InaccessibleStorageException {
		this.inode = Inode.blankRootInode(fs);
		this.inodes.put(INODE_ID_INODE_TABLE, this.inode);
		this.merkel = new PageMerkel(fs, this.inode);

		makeRootDir();
		nextInodeId = USER_INODE_ID_START;
	}

	public Revision getRevision() {
		return revision;
	}
}
