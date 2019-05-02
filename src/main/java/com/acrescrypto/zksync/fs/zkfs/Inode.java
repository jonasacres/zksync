package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.bouncycastle.util.Arrays;

import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.utility.Util;

public class Inode implements Comparable<Inode> {
	protected ZKFS fs; /** filesystem containing this inode */

	// the following fields are serialized:
	protected Stat stat; /** stat object, similar to POSIX struct stat */
	protected int nlink; /** number of references (eg paths) to this inode */
	protected byte flags; /** bitmask flag field */
	protected long modifiedTime; /** last time we modified inode or file data. can't use stat.ctime, since users own that. */
	protected long identity; /** assigned on inode creation, remains constant, used to distinguish new files vs. modified files on merge */
	protected RefTag refTag; /** reference to file contents */
	protected RevisionTag changedFrom; /** latest revision tag containing previous version */
	
	// for use in flags field
	public static final byte FLAG_RETAIN = 1 << 0;
	
	/** initialize a root inode (refers to the inode table itself) */
	public static Inode defaultRootInode(ZKFS fs) {
		Inode blank = new Inode(fs);
		Stat stat = new Stat();
		long now = Util.currentTimeNanos();
		stat.setAtime(now);
		stat.setMtime(now);
		stat.setCtime(now);
		stat.setUser("zksync");
		stat.setGroup("zksync");
		stat.setInodeId(InodeTable.INODE_ID_INODE_TABLE);
		
		blank.setStat(stat);
		blank.setFlags(FLAG_RETAIN);
		blank.setRefTag(RefTag.blank(fs.archive));
		blank.setModifiedTime(now);
		return blank;
	}
	
	/** initialize a blank inode for a given fs */
	public Inode(ZKFS fs) {
		this.fs = fs;
		this.stat = new Stat();
		this.changedFrom = fs.baseRevision;
		this.refTag = RefTag.blank(fs.archive);
	}
	
	/** deserialize an inode belonging to a given fs */
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
	
	public RevisionTag getChangedFrom() {
		return changedFrom;
	}

	public void setChangedFrom(RevisionTag changedFrom) {
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
	
	/** serialize to plaintext byte array */
	public byte[] serialize() {
		// force revtag serialization if needed
		if(changedFrom.getBytes() == null) changedFrom.serialize();
		
		ByteBuffer buf = ByteBuffer.allocate(InodeTable.inodeSize(fs.archive));
		buf.put(stat.serialize());
		buf.putLong(modifiedTime);
		buf.putLong(identity);
		buf.putInt(nlink);
		buf.put(flags);
		buf.put(refTag.getBytes());
		buf.put(changedFrom.getBytes());
		
		return buf.array();
	}
	
	/** deserialize from plaintext byte array */
	public void deserialize(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		
		byte[] serializedStat = new byte[Stat.STAT_SIZE];
		buf.get(serializedStat);
		this.stat = new Stat(serializedStat);
		
		this.modifiedTime = buf.getLong();
		this.identity = buf.getLong();
		this.nlink = buf.getInt();
		this.flags = buf.get();
		
		byte[] refTagBytes = new byte[fs.archive.config.refTagSize()];
		buf.get(refTagBytes);
		this.refTag = new RefTag(fs.archive, refTagBytes);
		
		byte[] revisionTagBytes = new byte[RevisionTag.sizeForConfig(fs.archive.config)];
		buf.get(revisionTagBytes);
		this.changedFrom = new RevisionTag(fs.archive.config, revisionTagBytes, false);
	}
	
	/** increment link count */
	public void addLink() {
		this.changedFrom = fs.baseRevision;
		nlink++;
	}
	
	/** decrement link count. automatically unlink if nlink becomes == 0 and FLAG_RETAIN not set. 
	 * @throws IOException */ 
	public void removeLink() throws IOException {
		nlink--;
		if(isDeleted()) {
			fs.getInodeTable().unlink(stat.getInodeId());
		}
	}
	
	/** hash code based on content reftag */
	public int hashCode() {
		return ByteBuffer.wrap(refTag.getBytes()).getInt();
	}
	
	/**
	 * return true if equal to other inode (matches identity, refTag, changedFrom, falgs, stat).
	 * WARNING: equals == true NOT the same as compareTo == 0 for Inode!
	 * @see compareTo
	 */
	public boolean equals(Object other) {
		if(other == null) return false;
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
	
	/** printable string summary for debug */
	public String toString() {
		return String.format("%d (%08x) - %d %d bytes, %d %d %s", stat.getInodeId(), hashCode(), nlink, stat.getSize(), stat.getMtime(), stat.getAtime(), Util.bytesToHex(refTag.tag, 4));
	}

	@Override
	/**
	 * Compares on modifiedTime, changedFrom then serialized inode contents.
	 * WARNING: compareTo == 0 NOT the same as equals == true! equals leaves out modifiedTime to detect identical changes.
	 * compareTo includes modifiedTime as its first key to allow time-wise sorting.
	 */
	public int compareTo(Inode o) {
		int c;
		if(modifiedTime != o.modifiedTime) return modifiedTime < o.modifiedTime ? -1 : 1;
		if((c = Arrays.compareUnsigned(changedFrom.getBytes(), o.changedFrom.getBytes())) != 0) return c;
		return Arrays.compareUnsigned(serialize(), o.serialize());
	}
	
	/** true <=> nlink == 0 and FLAG_RETAIN not set */
	public boolean isDeleted() {
		return nlink == 0 && (flags & FLAG_RETAIN) == 0;
	}
	
	/** clear inode contents. */
	public void markDeleted() {
		long oldId = stat.getInodeId();
		stat = new Stat();
		stat.setInodeId(oldId);
		refTag = RefTag.blank(fs.archive);
		changedFrom = RevisionTag.blank(fs.archive.config);
		nlink = 0;
		modifiedTime = 0;
		flags = 0;
		identity = 0;
	}
	
	/** deep copy of this inode */
	public Inode clone(ZKFS fs) {
		return new Inode(fs, serialize());
	}
	
	public String dump() {
		String s = "Inode " + stat.getInodeId() + " size: " + stat.getSize() + "\n";
		s += "\tMode: " + String.format("0%03o", stat.getMode()) + "\n";
		s += "\tUID: " + stat.getUid() + " " + stat.getUser() + "\n";
		s += "\tGID: " + stat.getGid() + " " + stat.getGroup() + "\n";
		s += "\tType: " + stat.getType() + " major: " + stat.getDevMajor() + " minor: " + stat.getDevMinor() + "\n";
		s += "\tMtime: " + stat.getMtime() + " Ctime: " + stat.getCtime() + " Atime: " + stat.getAtime() + "\n";
		s += "\tNlink: " + nlink + " Flags: " + String.format("0x%02x", flags) + "\n";
		s += "\tModTime: " + modifiedTime + "\n";
		s += "\tIdentity: " + identity + "\n";
		s += "\trefTag: " + Util.formatRefTag(refTag) + "\n";
		s += "\tchangedFrom: " + Util.formatRevisionTag(changedFrom) + "\n";
		return s;
	}
}
