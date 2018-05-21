package com.acrescrypto.zksync.fs.ramfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.HashMap;

import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.exceptions.EISNOTDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class RAMFS extends FS {
	static HashMap<String,RAMFS> volumes = new HashMap<String,RAMFS>();
	
	interface InodeMaker { void make(Inode inode); }
	String scope;
	HashMap<String,Inode> inodesByPath = new HashMap<String,Inode>();
	long nextInodeId = 0;
	
	public long maxFileSize() {
		return 1024*1024*1024*1;
	}
	
	public static RAMFS volumeWithName(String name) {
		volumes.putIfAbsent(name, new RAMFS());
		return volumes.get(name);
	}
	
	public static RAMFS removeVolume(String name) {
		return volumes.remove(name);
	}
	
	protected class Inode {
		Stat stat = new Stat();
		byte[] data;
		
		public Inode() {
			data = new byte[0];
			long now = System.currentTimeMillis() * (1000l * 1000l);
			stat.setInodeId(nextInodeId++);
			stat.setMode(0640);
			stat.setUser("root");
			stat.setGroup("root");
			stat.setMtime(now);
			stat.setCtime(now);
			stat.setAtime(now);
		}
		
		public void setData(byte[] data) {
			this.data = data;
			this.stat.setSize(data.length);
		}
	}

	public RAMFS() {
		this.scope = "";
		mkdir("/");
	}
	
	public RAMFS(String scope, RAMFS parent) {
		this.scope = scope;
		this.inodesByPath = parent.inodesByPath;
	}
	
	@Override
	public Stat stat(String path) throws IOException {
		return lookup(path).stat;
	}

	@Override
	public Stat lstat(String path) throws IOException {
		return llookup(path).stat;
	}

	@Override
	public RAMDirectory opendir(String path) throws IOException {
		if(!lookup(path).stat.isDirectory()) throw new EISNOTDIRException(path);
		return new RAMDirectory(this, path);
	}

	@Override
	public void mkdir(String path) {
		makeInode(path, (inode)->inode.stat.makeDirectory());
	}

	@Override
	public void mkdirp(String path) throws IOException {
		if(!dirname(path).equals(path) && !exists(dirname(path))) {
			mkdirp(dirname(path));
		}
		
		mkdir(path);
	}

	@Override
	public void rmdir(String path) throws IOException {
		clearInode(path);
	}

	@Override
	public void unlink(String path) throws IOException {
		clearInode(path);
	}

	@Override
	public void link(String target, String link) throws IOException {
		if(exists(link)) throw new EEXISTSException(link);
		setInode(link, lookup(target));
	}

	@Override
	public void symlink(String target, String link) throws IOException {
		makeInode(link, (inode)->{
			inode.stat.makeSymlink();
			inode.data = new String(target).getBytes();
		});
	}

	@Override
	public String readlink(String link) throws IOException {
		return new String(llookup(link).data);
	}

	@Override
	public void mknod(String path, int type, int major, int minor) throws IOException {
		makeInode(path, (inode)->{
			if(type == Stat.TYPE_BLOCK_DEVICE) inode.stat.makeBlockDevice(major, minor);
			else if(type == Stat.TYPE_CHARACTER_DEVICE) inode.stat.makeCharacterDevice(major, minor);
		});
	}

	@Override
	public void mkfifo(String path) throws IOException {
		makeInode(path, (inode)->{
			inode.stat.makeFifo();
		});
	}

	@Override
	public void chmod(String path, int mode) throws IOException {
		lookup(path).stat.setMode(mode);
	}

	@Override
	public void chown(String path, int uid) throws IOException {
		lookup(path).stat.setUid(uid);
	}

	@Override
	public void chown(String path, String user) throws IOException {
		lookup(path).stat.setUser(user);
	}

	@Override
	public void chgrp(String path, int gid) throws IOException {
		lookup(path).stat.setGid(gid);
	}

	@Override
	public void chgrp(String path, String group) throws IOException {
		lookup(path).stat.setGroup(group);
	}

	@Override
	public void setMtime(String path, long mtime) throws IOException {
		lookup(path).stat.setMtime(mtime);
	}

	@Override
	public void setCtime(String path, long ctime) throws IOException {
		lookup(path).stat.setCtime(ctime);
	}

	@Override
	public void setAtime(String path, long atime) throws IOException {
		lookup(path).stat.setAtime(atime);
	}
	
	@Override
	public byte[] read(String path) throws IOException {
		return lookup(path).data.clone();
	}

	@Override
	public void write(String path, byte[] contents, int offset, int length) throws IOException {
		mkdirp(dirname(path));
		try {
			Inode inode = lookup(path);
			inode.data = new byte[length];
			System.arraycopy(contents, offset, inode.data, 0, length);
			inode.stat.setSize(length);
		} catch(ENOENTException exc) {
			makeInode(path, (inode)->{ inode.data = contents.clone(); });
		}
	}

	@Override
	public File open(String path, int mode) throws IOException {
		return new RAMFile(this, path, mode);
	}

	@Override
	public void truncate(String path, long size) throws IOException {
		ByteBuffer newData = ByteBuffer.allocate((int) size);
		Inode inode = lookup(path);
		inode.data = newData.put(inode.data, 0, Math.min(inode.data.length,	(int) size)).array();
		inode.stat.setSize(inode.data.length);
	}

	@Override
	public FS scopedFS(String path) throws IOException {
		return new RAMFS(Paths.get(scope, path).toString(), this);
	}
	
	protected Inode lookup(String path) throws ENOENTException {
		Inode inode = llookup(path);
		if(inode.stat.isSymlink()) {
			return lookup(new String(inode.data));
		}
		
		return inode;
	}
	
	protected Inode llookup(String path) throws ENOENTException {
		if(path.equals("")) path = "/";
		Inode inode = inodesByPath.getOrDefault(unscopedPath(path), null);
		if(inode == null) throw new ENOENTException(path);
		return inode;
	}
	
	protected void setInode(String path, Inode inode) {
		inodesByPath.put(unscopedPath(path), inode);
	}
	
	protected void clearInode(String path) {
		if(unscopedPath(path).equals("/")) return;
		inodesByPath.remove(unscopedPath(path));
	}
	
	protected Inode makeInode(String path, InodeMaker lambda) {
		Inode inode = new Inode();
		lambda.make(inode);
		inode.stat.setSize(inode.data.length);
		setInode(path, inode);
		return inode;
	}
	
	protected String scopedPath(String path) {
		String suffix = scope;
		if(!scope.endsWith("/")) suffix += "/";
		String scoped = path.substring(suffix.length());
		if(scoped.startsWith("/")) scoped = scoped.substring(1);
		return scoped;
	}
	
	protected String unscopedPath(String path) {
		if(path.equals(".")) path = "/";
		return Paths.get("/", scope, path).toString();
	}
	
	public String getRoot() {
		return scope;
	}
}
