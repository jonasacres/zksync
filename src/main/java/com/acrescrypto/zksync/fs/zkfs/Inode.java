package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.fs.Stat;

public class Inode {
	protected Stat stat;
	protected int nlink;
	protected byte flags;
	protected RefTag refTag;
	protected RefTag changedFrom; // last revision reftag with previous version
	protected ZKFS fs;
	
	public static final byte FLAG_RETAIN = 1 << 0;
	
	public static Inode blankInode(ZKFS fs) {
		Inode blank = new Inode(fs);
		blank.setStat(new Stat());
		blank.setRefTag(RefTag.blank(fs.archive));
		return blank;
	}

	public static Inode defaultRootInode(ZKFS fs) {
		Inode blank = new Inode(fs);
		Stat stat = new Stat();
		long now = System.currentTimeMillis()*1000l*1000l;
		stat.setAtime(now);
		stat.setMtime(now);
		stat.setCtime(now);
		stat.setUser("zksync");
		stat.setGroup("zksync");
		
		blank.setStat(stat);
		blank.setFlags(FLAG_RETAIN);
		blank.setRefTag(RefTag.blank(fs.archive));
		return blank;
	}
	
	public Inode(ZKFS fs) {
		this.fs = fs;
		this.stat = new Stat();
		this.changedFrom = fs.baseRevision;
	}
	
	public Inode(ZKFS fs, Stat stat) {
		this.fs = fs;
		this.stat = stat;
	}
	
	public Inode(ZKFS fs, byte[] serialized) {
		this.fs = fs;
		deserialize(serialized);
	}
	
	public void setStat(Stat stat) {
		this.stat = stat;
	}
	
	public Stat getStat() {
		return stat;
	}
	
	public int getNlink() {
		return nlink;
	}
	
	public void setNlink(int nlink) {
		this.nlink = nlink;
	}
	
	public int getFlags() {
		return flags;
	}
	
	public void setFlags(byte flags) {
		this.flags = flags;
	}

	public RefTag getRefTag() {
		return refTag;
	}

	public void setRefTag(RefTag refTag) {
		this.refTag = refTag;
	}
	
	public RefTag getChangedFrom() {
		return changedFrom;
	}

	public void setChangedFrom(RefTag changedFrom) {
		this.changedFrom = changedFrom;
	}
	
	public byte[] serialize() {
		int size = stat.getStorageSize() + 2*4 + 1 + 2*(RefTag.REFTAG_EXTRA_DATA_SIZE+fs.archive.crypto.hashLength());
		ByteBuffer buf = ByteBuffer.allocate(size);
		buf.putInt(size-4);
		buf.put(stat.serialize());
		buf.putInt(nlink);
		buf.put(flags);
		buf.put(refTag.getBytes());
		buf.put(changedFrom.getBytes());
		assert(!buf.hasRemaining());
		
		return buf.array();
	}
	
	public void deserialize(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		buf.getInt(); // inode size; skip
		int statLen = buf.getInt();
		
		buf.position(buf.position()-4); // Stat constructor expects to see statLen at start
		byte[] serializedStat = new byte[statLen];
		buf.get(serializedStat, 0, statLen);
		this.stat = new Stat(serializedStat);
		
		this.nlink = buf.getInt();
		this.flags = buf.get();
		
		byte[] refTagBytes = new byte[fs.archive.crypto.hashLength()];
		buf.get(refTagBytes, 0, fs.archive.crypto.hashLength());
		this.refTag = new RefTag(fs.archive, refTagBytes);
		
		buf.get(refTagBytes, 0, fs.archive.crypto.hashLength());
		this.changedFrom = new RefTag(fs.archive, refTagBytes);
		
		assert(!buf.hasRemaining());
	}
	
	public void addLink() {
		nlink++;
	}
	
	public void removeLink() {
		nlink--;
		if((flags & FLAG_RETAIN) == 0 && nlink == 0) {
			try {
				fs.getInodeTable().unlink(stat.getInodeId());
			} catch (IOException e) {
				throw new IllegalStateException();
			}
		}
	}
	
	public Inode clone() {
		Inode clone = new Inode(fs);
		clone.flags = flags;
		clone.nlink = nlink;
		clone.refTag = refTag;
		clone.changedFrom = changedFrom;
		clone.stat = stat.clone();
		return clone;
	}
	
	public int hashCode() {
		return ByteBuffer.wrap(fs.archive.crypto.hash(serialize())).getInt(); // TODO: super slow, reconsider
	}
	
	public boolean equals(Object other) {
		if(!other.getClass().equals(this.getClass())) return false;
		Inode __other = (Inode) other;
		return Arrays.equals(serialize(), __other.serialize());
	}
	
	public String toString() {
		return String.format("%d (%08x) - %d %d bytes, %d %d", stat.getInodeId(), hashCode(), nlink, stat.getSize(), stat.getMtime(), stat.getAtime());
	}
}
