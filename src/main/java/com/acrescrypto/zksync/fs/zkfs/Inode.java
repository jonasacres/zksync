package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.bouncycastle.util.Arrays;

import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.utility.Util;

public class Inode implements Comparable<Inode> {
	protected ZKFS fs; /** filesystem containing this inode */

	// the following fields are serialized:
	private Stat stat; /** stat object, similar to POSIX struct stat */
	private int nlink; /** number of references (eg paths) to this inode */
	private byte flags; /** bitmask flag field */
	private long modifiedTime; /** last time we modified inode or file data. can't use stat.ctime, since users own that. */
	private long identity; /** assigned on inode creation, remains constant, used to distinguish new files vs. modified files on merge */
	private RefTag refTag; /** reference to file contents */
	private RevisionTag changedFrom; /** latest revision tag containing previous version */
	private long previousInodeId; /** inodeId in revision indicated in changedFrom; -1 if inode did not exist in that revision */
	private boolean dirty;
	
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
		if(!fs.archive.config.isReadOnly()) {
			this.changedFrom = RevisionTag.blank(fs.archive.config);
		}
		this.refTag = RefTag.blank(fs.archive);
	}
	
	/** deserialize an inode belonging to a given fs */
	public Inode(ZKFS fs, byte[] serialized) {
		this.fs = fs;
		deserialize(serialized);
	}
	
	public Stat getStat() {
		return stat;
	}
	
	public void setStat(Stat stat) {
		if(this.stat == stat) return;
		setDirty(true);
		this.stat = stat;
	}
	
	public int getNlink() {
		return nlink;
	}
	
	public void setNlink(int nlink) {
		if(nlink == this.nlink) return;
		setDirty(true);
		this.nlink = nlink;
	}
	
	public byte getFlags() {
		return flags;
	}
	
	public void setFlags(byte flags) {
		if(flags == this.flags) return;
		setDirty(true);
		this.flags = flags;
	}

	public RefTag getRefTag() {
		return refTag;
	}

	public void setRefTag(RefTag refTag) {
		if(refTag == this.refTag) return;
		setDirty(true);
		this.refTag = refTag;
	}
	
	public RevisionTag getChangedFrom() {
		return changedFrom;
	}

	public void setChangedFrom(RevisionTag changedFrom) {
		if(changedFrom == this.changedFrom) return;
		setDirty(true);
		this.changedFrom = changedFrom;
	}
	
	public long getPreviousInodeId() {
		return previousInodeId;
	}
	
	public void setPreviousInodeId(long previousInodeId) {
		if(previousInodeId == this.previousInodeId) return;
		setDirty(true);
		this.previousInodeId = previousInodeId;
	}
	
	public long getModifiedTime() {
		return modifiedTime;
	}
	
	public void setModifiedTime(long modifiedTime) {
		if(modifiedTime == this.modifiedTime) return;
		setDirty(true);
		this.modifiedTime = modifiedTime;
	}
	
	public long getIdentity() {
		return identity;
	}
	
	public void setIdentity(long identity) {
		if(identity == this.identity) return;
		setDirty(true);
		this.identity = identity;
	}
	
	/** serialize to plaintext byte array 
	 * @throws IOException */
	public byte[] serialize() throws IOException {
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
		buf.putLong(previousInodeId);
		
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
		
		this.previousInodeId = buf.getLong();
		this.dirty = false;
	}
	
	/** increment link count */
	public void addLink() {
		setDirty(true);
		nlink++;
	}
	
	/** decrement link count. automatically unlink if nlink becomes == 0 and FLAG_RETAIN not set. 
	 * @throws IOException */ 
	public void removeLink() throws IOException {
		/* InodeTable needs to have the next inode ID cached BEFORE we unlink. Otherwise, if it needs to
		 * scan when we unlink and if the unlinked inode is at the end of the table, it will see our inode
		 * has nlink 0 and consider it already deleted, and throw an error since we're attempting to unlink
		 * an inode presumed to be beyond the end of the table. Triggering the scan here is a bit ugly,
		 * but provides the necessary guarantee that the scan happens before nlink is decremented.
		 */
		fs.getInodeTable().nextInodeId();
		setDirty(true);
		nlink--;
		if(isDeleted()) {
			fs.getInodeTable().unlink(stat.getInodeId());
		}
	}
	
	/** hash code based on content reftag */
	public int hashCode() {
		return refTag.hashCode();
	}
	
	/**
	 * return true if equal to other inode (matches identity, refTag, changedFrom, flags, stat).
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
		if(getFlags() != __other.getFlags()) return false;
		if(!stat.equals(__other.stat)) return false;
		// intentionally exclude modifiedTime, since otherwise we won't notice when two revisions are the same in a merge
		// also skip nlink, since that confuses diffs involving hardlinks
		return true;
	}
	
	/** printable string summary for debug */
	public String toString() {
		return String.format("%d (%08x) - %d %d bytes, %d %d %s",
				stat.getInodeId(),
				hashCode(),
				nlink,
				stat.getSize(),
				stat.getMtime(),
				stat.getAtime(),
				refTag
		);
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
		if(identity != o.identity) Long.compareUnsigned(identity, o.identity);
		if(refTag != o.refTag) return refTag.compareTo(o.refTag);
		if(!stat.equals(o.stat)) return Arrays.compareUnsigned(stat.serialize(), o.stat.serialize());
		if(nlink != o.nlink) return Integer.compare(nlink, o.nlink);
		if(flags != o.flags) return Byte.compare(flags, o.flags);
		if(previousInodeId != o.previousInodeId) return Long.compare(previousInodeId, o.previousInodeId);
		return 0;
	}
	
	/** is this inode in a deleted state? true <=> nlink == 0 and FLAG_RETAIN not set */
	public boolean isDeleted() {
		return nlink == 0 && (getFlags() & FLAG_RETAIN) == 0;
	}
	
	/** has the deletion of this inode been processed? (ie. its contents are cleared) */
	public boolean isMarkedDeleted() {
		return nlink == 0 && identity == 0 && modifiedTime == 0;
	}
	
	/** clear inode contents. */
	public void markDeleted() {
		long oldId = stat.getInodeId();
		setDirty(true);
		stat = new Stat();
		stat.setInodeId(oldId);
		refTag = RefTag.blank(fs.archive);
		changedFrom = RevisionTag.blank(fs.archive.config);
		previousInodeId = 0;
		nlink = 0;
		modifiedTime = 0;
		flags = 0;
		identity = 0;
	}
	
	/** deep copy of this inode */
	public Inode clone() {
		return clone(fs);
	}
	
	/** deep copy of this inode with new fs field */
	public Inode clone(ZKFS fs) {
		Inode newInode = new Inode(fs);
		newInode.setStat(stat.clone());
		newInode.setNlink(nlink);
		newInode.setFlags(flags);
		newInode.setModifiedTime(modifiedTime);
		newInode.setIdentity(identity);
		newInode.setRefTag(refTag);
		newInode.setChangedFrom(changedFrom);
		newInode.setPreviousInodeId(previousInodeId);
		newInode.setDirty(dirty);
		
		return newInode;
	}
	
	public void copyValuesFrom(Inode inode, boolean preserveSizeAndReftag) {
		long oldSize = this.stat.getSize();
		RefTag oldRefTag = this.getRefTag();
		
		this.setStat(inode.stat.clone());
		this.setNlink(inode.getNlink());
		this.setFlags(inode.getFlags());
		this.setModifiedTime(inode.getModifiedTime());
		this.setIdentity(inode.getIdentity());
		this.setRefTag(inode.getRefTag());
		this.setChangedFrom(inode.getChangedFrom());
		
		if(preserveSizeAndReftag) {
			this.stat.setSize(oldSize);
			this.setRefTag(oldRefTag);
		}
	}
	
	public String dump() {
		String s = "Inode " + stat.getInodeId() + " size: " + stat.getSize() + "\n";
		s += "\tMode: " + String.format("0%03o", stat.getMode()) + "\n";
		s += "\tUID: " + stat.getUid() + " " + stat.getUser() + "\n";
		s += "\tGID: " + stat.getGid() + " " + stat.getGroup() + "\n";
		s += "\tType: " + stat.getType() + " major: " + stat.getDevMajor() + " minor: " + stat.getDevMinor() + "\n";
		s += "\tMtime: " + stat.getMtime() + " Ctime: " + stat.getCtime() + " Atime: " + stat.getAtime() + "\n";
		s += "\tNlink: " + nlink + " Flags: " + String.format("0x%02x", getFlags()) + "\n";
		s += "\tModTime: " + modifiedTime + "\n";
		s += "\tIdentity: " + identity + "\n";
		s += "\trefTag: " + Util.formatRefTag(refTag) + "\n";
		s += "\tchangedFrom: " + Util.formatRevisionTag(changedFrom) + "\n";
		s += "\tpreviousInodeId: " + previousInodeId + "\n";
		return s;
	}

	public boolean isDirty() {
		return dirty;
	}

	public void setDirty(boolean dirty) {
		if(dirty == this.dirty) return;		
		this.dirty = dirty;
		
		if(dirty && fs != null && fs.getInodeTable() != null) {
			fs.getInodeTable().setDirty(true);
		}
	}
}
