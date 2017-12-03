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
	protected long identity; // assigned on inode creation, remains constant
	protected RefTag refTag;
	protected RefTag changedFrom; // last revision reftag with previous version
	protected ZKFS fs;
	
	public static final byte FLAG_RETAIN = 1 << 0;
	
	public static int sizeForFs(ZKFS fs) {
		// TODO: consider leaving some space to grow...
		return Stat.STAT_SIZE + 2*8 + 1*4 + 1 + 2*(fs.archive.refTagSize());
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
	
	public long getIdentity() {
		return identity;
	}
	
	public void setIdentity(long newIdentity) {
		this.identity = newIdentity;
	}
	
	public byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(sizeForFs(fs));
		buf.put(stat.serialize());
		buf.putLong(modifiedTime);
		buf.putLong(identity);
		buf.putInt(nlink);
		buf.put(flags);
		buf.put(refTag.getBytes());
		buf.put(changedFrom.getBytes());
		
		return buf.array();
	}
	
	public void deserialize(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		
		byte[] serializedStat = new byte[Stat.STAT_SIZE];
		buf.get(serializedStat);
		this.stat = new Stat(serializedStat);
		
		this.modifiedTime = buf.getLong();
		this.identity = buf.getLong();
		this.nlink = buf.getInt();
		this.flags = buf.get();
		
		byte[] refTagBytes = new byte[fs.archive.refTagSize()];
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
	
	public void markDeleted() {
		long inodeId = stat.getInodeId();
		stat = new Stat();
		stat.setInodeId(inodeId);
		nlink = 0;
		flags = 0;
		modifiedTime = 0;
		identity = 0;
		refTag = RefTag.blank(fs.archive);
		changedFrom = RefTag.blank(fs.archive);
	}
	
	public boolean isDeleted() {
		return nlink <= 0 && (flags & FLAG_RETAIN) == 0;
	}
	
	public boolean equals(Object other) {
		if(!other.getClass().equals(this.getClass())) return false;
		Inode __other = (Inode) other;
		
		if(identity != __other.identity) return false;
		if(!refTag.equals(__other.refTag)) return false;
		if(!changedFrom.equals(__other.changedFrom)) return false;
		if(flags != __other.flags) return false;
		if(!stat.equals(__other.stat)) return false;
		// intentionally exclude modifiedTime, since otherwise we won't notice when two revisions are the same in a merge
		// also skip nlink, since that confuses diffs involving hardlinks
		return true;
	}
	
	public String toString() {
		return String.format("%d (%08x) - %d %d bytes, %d %d %s", stat.getInodeId(), hashCode(), nlink, stat.getSize(), stat.getMtime(), stat.getAtime(), Util.bytesToHex(refTag.tag));
	}

	@Override
	public int compareTo(Inode o) {
		/* Note that compareTo is very different from equals. equals might be true while compareTo != 0.
		 * equals leaves out modifiedTime to allow detection of identical revisions; compareTo uses modifiedTime as
		 * its first key, to allow time-wise sorting.
		 */
		int c;
		if(modifiedTime != o.modifiedTime) return modifiedTime < o.modifiedTime ? -1 : 1;
		if((c = Arrays.compareUnsigned(changedFrom.getBytes(), o.changedFrom.getBytes())) != 0) return c;
		return Arrays.compareUnsigned(serialize(), o.serialize());
	}
	
	public Inode clone(ZKFS fs) {
		return new Inode(fs, serialize());
	}
}
