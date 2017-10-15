package com.acrescrypto.zksync.fs;

import java.nio.ByteBuffer;

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
	
	public Stat() { }
	
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
	
	public int getStorageSize() {
		// int size
		// int gid, uid, mode, typeFlags, devMajor, devMinor: 5*5 = 25
		// String group, user: 4 + variable + 4 + variable = 8 + 2*variable
		// long atime, mtime, ctime, size, inodeId; 5*8 = 40
		// total: 74 + 2*variable
		
		return 78 + group.length() + user.length();
	}
	
	public byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(getStorageSize());
		buf.putInt(getStorageSize());
		buf.putInt(gid);
		buf.putInt(uid);
		buf.putInt(mode);
		buf.putInt(getType());
		buf.putInt(getDevMajor());
		buf.putInt(getDevMinor());
		
		buf.putLong(ctime);
		buf.putLong(mtime);
		buf.putLong(atime);
		
		buf.putInt(user.length());
		buf.put(user.getBytes());
		buf.putInt(group.length());
		buf.put(group.getBytes());
		
		return buf.array();
	}
	
	public void deserialize(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		buf.getInt(); // skip storage size
		this.gid = buf.getInt();
		this.uid = buf.getInt();
		this.mode = buf.getInt();
		this.setType(buf.getInt());
		this.setDevMajor(buf.getInt());
		this.setDevMinor(buf.getInt());
		
		this.ctime = buf.getLong();
		this.mtime = buf.getLong();
		this.atime = buf.getLong();
		
		int userLen = buf.getInt();
		byte[] userBuf = new byte[userLen];
		buf.get(userBuf);
		this.user = new String(userBuf);
		
		int groupLen = buf.getInt();
		byte[] groupBuf = new byte[groupLen];
		buf.get(groupBuf);
		this.group = new String(groupBuf);
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
}
