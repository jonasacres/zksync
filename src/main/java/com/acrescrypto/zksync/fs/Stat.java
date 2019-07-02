package com.acrescrypto.zksync.fs;

import java.nio.ByteBuffer;

import org.bouncycastle.util.Arrays;

public class Stat {
	int gid, uid, mode;
	private int type;
	private int devMinor, devMajor;
	String group, user;
	long atime, mtime, ctime, size, inodeId;
	
	public final static int TYPE_REGULAR_FILE = 0;
	public final static int TYPE_DIRECTORY = 1;
	public final static int TYPE_SYMLINK = 2;
	public final static int TYPE_BLOCK_DEVICE = 3;
	public final static int TYPE_CHARACTER_DEVICE = 4;
	public final static int TYPE_FIFO = 5;
	
	// int gid, uid, mode, typeFlags, devMajor, devMinor:      6*4  =  24
	// String group, user:                                    2*32  =  64
	// long atime, mtime, ctime, size, inodeId;                5*8  =  40
	// total:                                                         128
	public final static int STAT_SIZE = 128; // size of serialized inode in bytes
	public final static int MAX_GROUP_LEN = 32;
	public final static int MAX_USER_LEN = 32;
	
	public Stat() {
		group = "";
		user = "";
	}
	
	public Stat(byte[] serialized) {
		deserialize(serialized);
	}
	
	public boolean isSymlink() {
		return getType() == TYPE_SYMLINK;
	}
	
	public boolean isRegularFile() {
		return getType() == TYPE_REGULAR_FILE;
	}
	
	public boolean isDirectory() {
		return getType() == TYPE_DIRECTORY;
	}
	
	public boolean isDevice() {
		return isBlockDevice() || isCharacterDevice();
	}
	
	public boolean isBlockDevice() {
		return getType() == TYPE_BLOCK_DEVICE;
	}
	
	public boolean isCharacterDevice() {
		return getType() == TYPE_CHARACTER_DEVICE;
	}
	
	public boolean isFifo() {
		return getType() == TYPE_FIFO;
	}
	
	public String getGroup() {
		return group;
	}
	
	public String getUser() {
		return user;
	}
	
	public int getGid() {
		return this.gid;
	}
	
	public String setUser() {
		return user;
	}
	
	public int getUid() {
		return this.uid;
	}
	
	public int getMode() {
		return this.mode;
	}
	
	public long getAtime() {
		return this.atime;
	}
	
	public long getCtime() {
		return this.ctime;
	}
	
	public long getMtime() {
		return this.mtime;
	}
	
	public long getSize() {
		return this.size;
	}
	
	public long getInodeId() {
		return this.inodeId;
	}
	
	public void setGroup(String group) {
		this.group = group;
	}
	
	public void setGid(int gid) {
		this.gid = gid;
	}
	
	public void setUser(String user) {
		this.user = user;
	}
	
	public void setUid(int uid) {
		this.uid = uid;
	}
	
	public void setMode(int mode) {
		this.mode = mode;
	}
	
	public void setAtime(long atime) {
		this.atime = atime;
	}

	public void setCtime(long ctime) {
		this.ctime = ctime;
	}

	public void setMtime(long mtime) {
		this.mtime = mtime;
	}

	public void setSize(long size) {
		this.size = size;
	}
	
	public void setInodeId(long inodeId) {
		this.inodeId = inodeId;
	}
	
	public byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(STAT_SIZE);
		buf.putLong(inodeId);
		buf.putLong(size);
		buf.putLong(ctime);
		buf.putLong(mtime);
		buf.putLong(atime);
		
		buf.putInt(gid);
		buf.putInt(uid);
		buf.putInt(mode);
		buf.putInt(getType());
		buf.putInt(getDevMajor());
		buf.putInt(getDevMinor());
		
		buf.put(user.getBytes());
		buf.put(new byte[MAX_USER_LEN-user.getBytes().length]);
		buf.put(group.getBytes());
		buf.put(new byte[MAX_GROUP_LEN-group.getBytes().length]);
		
		return buf.array();
	}
	
	public void deserialize(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		this.inodeId = buf.getLong();
		this.size = buf.getLong();
		this.ctime = buf.getLong();
		this.mtime = buf.getLong();
		this.atime = buf.getLong();
		
		this.gid = buf.getInt();
		this.uid = buf.getInt();
		this.mode = buf.getInt();
		this.setType(buf.getInt());
		this.setDevMajor(buf.getInt());
		this.setDevMinor(buf.getInt());
		
		byte[] userBuf = new byte[MAX_USER_LEN];
		buf.get(userBuf);
		this.user = unpad(userBuf);
		
		byte[] groupBuf = new byte[MAX_GROUP_LEN];
		buf.get(groupBuf);
		this.group = unpad(groupBuf);
		assert(!buf.hasRemaining());
	}
	
	protected String unpad(byte[] buf) {
		int length = 0;
		while(length < buf.length && buf[length] != 0) {
			length++;
		}
		
		return new String(buf, 0, length);
	}
	
	public Stat clone() {
		Stat clone = new Stat();
		clone.gid = gid;
		clone.uid = uid;
		clone.mode = mode;
		clone.setType(type);
		clone.setDevMajor(devMajor);
		clone.setDevMinor(devMinor);
		clone.group = group == null ? null : new String(group);
		clone.user = user == null ? null : new String(user);
		clone.atime = atime;
		clone.mtime = mtime;
		clone.ctime = ctime;
		clone.size = size;
		clone.inodeId = inodeId;
		
		return clone;
	}
	
	public void makeRegularFile() {
		setType(TYPE_REGULAR_FILE);
	}

	public void makeSymlink() {
		setType(TYPE_SYMLINK);
	}
	
	public void makeDirectory() {
		setType(TYPE_DIRECTORY);
	}
	
	public void makeCharacterDevice(int major, int minor) {
		setType(TYPE_CHARACTER_DEVICE);
		setDevMajor(major);
		setDevMinor(minor);
	}
	
	public void makeBlockDevice(int major, int minor) {
		setType(TYPE_BLOCK_DEVICE);
		setDevMajor(major);
		setDevMinor(minor);
	}
	
	public void makeFifo() {
		setType(TYPE_FIFO);
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getDevMajor() {
		return devMajor;
	}

	public void setDevMajor(int devMajor) {
		this.devMajor = devMajor;
	}

	public int getDevMinor() {
		return devMinor;
	}

	public void setDevMinor(int devMinor) {
		this.devMinor = devMinor;
	}
	
	public boolean equals(Object other) {
		if(other == null) return false;
		if(!other.getClass().equals(this.getClass())) return false;
		return Arrays.areEqual(serialize(), ((Stat) other).serialize());
	}
	
	public String toString() {
		return String.format("Inode %016x, type=%d, size=%d, mode=0%s",
				inodeId,
				type,
				size,
				Integer.toOctalString(mode));
	}
}
