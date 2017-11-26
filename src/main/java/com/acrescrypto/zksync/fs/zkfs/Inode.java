package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.bouncycastle.util.Arrays;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.fs.Stat;

public class Inode implements Comparable<Inode> {
	protected Stat stat;
	protected int nlink;
	protected byte flags;
	protected long modifiedTime; // last time we modified inode or file data. can't use stat.ctime, since users own that.
	protected RefTag refTag;
	protected RefTag changedFrom; // last revision reftag with previous version
	protected ZKFS fs;
	
	// TODO: modifying metadata, like nlink or stat, should update modifiedTime. test for this.
	
	/* TODO: ctime semantics
	 * Right now, ctime is treated as a "creation time," as in NTFS. Instead, it should be "change time" for inode
	 * data. This is important for diff merges, where we need to be abel to differentiate between changes to a
	 * directory's structure, and to its metadata.
	 */
	
	public static final byte FLAG_RETAIN = 1 << 0;
	
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
		blank.setModifiedTime(now);
		return blank;
	}
	
	public Inode(ZKFS fs) {
		this.fs = fs;
		this.stat = new Stat();
		this.changedFrom = fs.baseRevision;
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
	
	public long getModifiedTime() {
		return modifiedTime;
	}
	
	public void setModifiedTime(long modifiedTime) {
		this.modifiedTime = modifiedTime;
	}
	
	public byte[] serialize() {
		int size = stat.getStorageSize() + 8 + 2*4 + 1 + 2*(RefTag.REFTAG_EXTRA_DATA_SIZE+fs.archive.crypto.hashLength());
		ByteBuffer buf = ByteBuffer.allocate(size);
		buf.putInt(size-4);
		buf.put(stat.serialize());
		buf.putLong(modifiedTime);
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
		
		this.modifiedTime = buf.getLong();
		this.nlink = buf.getInt();
		this.flags = buf.get();
		
		byte[] refTagBytes = new byte[fs.archive.crypto.hashLength() + RefTag.REFTAG_EXTRA_DATA_SIZE];
		buf.get(refTagBytes);
		this.refTag = new RefTag(fs.archive, refTagBytes);
		
		buf.get(refTagBytes);
		this.changedFrom = new RefTag(fs.archive, refTagBytes);
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
	
	public int hashCode() {
		return ByteBuffer.wrap(refTag.getBytes()).getInt();
	}
	
	public boolean equals(Object other) {
		if(!other.getClass().equals(this.getClass())) return false;
		Inode __other = (Inode) other;
		if(!refTag.equals(__other.refTag)) return false;
		if(!changedFrom.equals(__other.changedFrom)) return false;
		if(flags != __other.flags) return false;
		if(nlink != __other.nlink) return false;
		if(!stat.equals(__other.stat)) return false;
		// intentionally exclude modifiedTime, since otherwise we won't notice when two revisions are the same in a merge
		return true;
	}
	
	public String toString() {
		return String.format("%d (%08x) - %d %d bytes, %d %d %s", stat.getInodeId(), hashCode(), nlink, stat.getSize(), stat.getMtime(), stat.getAtime(), Util.bytesToHex(refTag.tag));
	}

	@Override
	public int compareTo(Inode o) {
		int c;
		if(modifiedTime != o.modifiedTime) return modifiedTime < o.modifiedTime ? -1 : 1;
		if((c = Arrays.compareUnsigned(changedFrom.getBytes(), o.changedFrom.getBytes())) != 0) return c;
		return Arrays.compareUnsigned(serialize(), o.serialize());
	}
	
	public Inode clone() {
		return new Inode(this.fs, serialize());
	}
}
