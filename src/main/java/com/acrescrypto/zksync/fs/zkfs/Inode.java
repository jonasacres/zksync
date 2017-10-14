package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.fs.Stat;

public class Inode {
	private Stat stat;
	private int nlink;
	private byte flags, refType;
	private byte[] refId;
	private ZKFS fs;
	
	public static final byte FLAG_RETAIN = 1 << 0;
	
	public static final byte REF_TYPE_IMMEDIATE = 0;
	public static final byte REF_TYPE_INDIRECT = 1;
	public static final byte REF_TYPE_2INDIRECT = 2;
	
	public static Inode blankRootInode(ZKFS fs) {
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
		return blank;
	}
	
	public Inode(ZKFS fs) {
		this.fs = fs;
		this.stat = new Stat();
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

	public int getRefType() {
		return refType;
	}

	public void setRefType(byte refType) {
		this.refType = refType;
	}

	public byte[] getRefTag() {
		return refId;
	}

	public void setRefTag(byte[] refId) {
		this.refId = refId;
	}
	
	public byte[] serialize() {
		int size = stat.getStorageSize() + 2*4 + 2*1 + fs.getCrypto().hashLength();
		ByteBuffer buf = ByteBuffer.allocate(size);
		buf.putInt(size-4);
		buf.put(stat.serialize());
		buf.putInt(nlink);
		buf.put(flags);
		buf.put(refType);
		buf.put(refId);
		
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
		this.refType = buf.get();
		buf.get(this.refId, 0, fs.getCrypto().hashLength());
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
		clone.refType = refType;
		clone.refId = refId.clone();
		clone.stat = stat.clone();
		return clone;
	}
}
