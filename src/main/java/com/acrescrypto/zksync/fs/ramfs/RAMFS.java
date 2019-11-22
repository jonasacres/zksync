package com.acrescrypto.zksync.fs.ramfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.exceptions.EISDIRException;
import com.acrescrypto.zksync.exceptions.EISNOTDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.ENOTEMPTYException;
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
		this.scope = "/";
		try {
			mkdir("/");
		} catch(IOException exc) {
			throw new RuntimeException("unexpected failure to create RAMFS");
		}
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
		return opendir(path, stat(path));
	}
	
	@Override
	public RAMDirectory opendir(String path, Stat stat) throws IOException {
		if(!stat.isDirectory()) throw new EISNOTDIRException(path);
		return new RAMDirectory(this, path);
	}

	@Override
	public void mkdir(String path) throws IOException {
		if(exists(path)) throw new EEXISTSException(path);
		makeInode(path, (inode)->inode.stat.makeDirectory());
	}

	@Override
	public synchronized void mkdirp(String path) throws IOException {
		if(!dirname(path).equals(path) && !exists(dirname(path))) {
			mkdirp(dirname(path));
		}
		
		try {
			mkdir(path);
		} catch(EEXISTSException exc) {}
	}

	@Override
	public void rmdir(String path) throws IOException {
		if(!stat(path).isDirectory()) throw new EISNOTDIRException(path);
		clearInode(path);
	}
	
	@Override
	public void mv(String oldPath, String newPath) throws IOException {
		try {
			super.mv(oldPath, newPath);
		} catch(EISDIRException exc) {
			// adapted from ZKFS version
			Stat oldStat = stat(oldPath);
			String actualTargetPath;
			try {
				Stat destStat = stat(newPath);
				if(destStat.isDirectory()) {
					actualTargetPath = Paths.get(newPath, basename(oldPath)).toString();
					
					try {
						destStat = stat(actualTargetPath);
						if(destStat.isDirectory() && oldStat.isDirectory()) {
							try(RAMDirectory dir = opendir(actualTargetPath)) {
								if(dir.list().size() > 0) {
									throw new ENOTEMPTYException(actualTargetPath);
								}
								
								rmdir(actualTargetPath);
							}
						} else {
							throw new EEXISTSException(actualTargetPath);
						}
					} catch(ENOENTException exc3) {}
				} else {
					throw new EEXISTSException(newPath);
				}
			} catch(ENOENTException exc2) {
				actualTargetPath = newPath;
			}
			
			try(RAMDirectory dir = opendir(dirname(actualTargetPath))) {
				dir.link(oldPath, basename(actualTargetPath));
			}
			
			try(RAMDirectory existingDir = opendir(oldPath)) {
				for(String subpath : existingDir.list()) {
					String oldSubPath = Paths.get(oldPath, subpath).toString();
					String newSubPath = Paths.get(actualTargetPath, subpath).toString();
					Inode inode = llookup(oldSubPath);
					setInode(newSubPath, inode);
				}
			}

			try(RAMDirectory existingDir = opendir(oldPath)) {
				for(String subpath : existingDir.list()) {
					unlink(Paths.get(oldPath, subpath).toString());
				}
			}
			
			try(RAMDirectory dir = opendir(dirname(oldPath))) {
				dir.unlink(basename(oldPath));
			}
		}
	}

	@Override
	public void unlink(String path) throws IOException {
		if(!exists(path, false)) throw new ENOENTException(path);
		clearInode(path);
	}

	@Override
	public void link(String target, String link) throws IOException {
		if(exists(link)) throw new EEXISTSException(link);
		setInode(link, llookup(target));
	}

	@Override
	public void symlink(String target, String link) throws IOException {
		unscopedPath(target); // trigger exception if outside scope
		symlink_unsafe(target, link);
	}
	
	@Override
	/** unsafe because there is no scope check */
	public void symlink_unsafe(String target, String link) throws IOException {
		if(exists(link)) throw new EEXISTSException(link);
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
	public String readlink_unsafe(String link) throws IOException {
		return new String(llookup(link).data);
	}

	@Override
	public void mknod(String path, int type, int major, int minor) throws IOException {
		if(exists(path)) throw new EEXISTSException(path);
		makeInode(path, (inode)->{
			if(type == Stat.TYPE_BLOCK_DEVICE) inode.stat.makeBlockDevice(major, minor);
			else if(type == Stat.TYPE_CHARACTER_DEVICE) inode.stat.makeCharacterDevice(major, minor);
		});
	}

	@Override
	public void mkfifo(String path) throws IOException {
		if(exists(path)) throw new EEXISTSException(path);
		makeInode(path, (inode)->{
			inode.stat.makeFifo();
		});
	}

	@Override
	public void chmod(String path, int mode, boolean followSymlinks) throws IOException {
		lookup(path, followSymlinks).stat.setMode(mode);
	}

	@Override
	public void chown(String path, int uid, boolean followSymlinks) throws IOException {
		lookup(path, followSymlinks).stat.setUid(uid);
	}

	@Override
	public void chown(String path, String user, boolean followSymlinks) throws IOException {
		lookup(path, followSymlinks).stat.setUser(user);
	}

	@Override
	public void chgrp(String path, int gid, boolean followSymlinks) throws IOException {
		lookup(path, followSymlinks).stat.setGid(gid);
	}

	@Override
	public void chgrp(String path, String group, boolean followSymlinks) throws IOException {
		lookup(path, followSymlinks).stat.setGroup(group);
	}

	@Override
	public void setMtime(String path, long mtime, boolean followSymlinks) throws IOException {
		lookup(path, followSymlinks).stat.setMtime(mtime);
	}

	@Override
	public void setCtime(String path, long ctime, boolean followSymlinks) throws IOException {
		lookup(path, followSymlinks).stat.setCtime(ctime);
	}

	@Override
	public void setAtime(String path, long atime, boolean followSymlinks) throws IOException {
		lookup(path, followSymlinks).stat.setAtime(atime);
	}
	
	@Override
	public byte[] read(String path) throws IOException {
		return lookup(path).data.clone();
	}

	@Override
	public void write(String path, byte[] contents, int offset, int length) throws IOException {
		byte[] data = new byte[length];
		System.arraycopy(contents, offset, data, 0, length);
		if(!exists(dirname(path))) mkdirp(dirname(path));
		
		try {
			Inode inode = lookup(path);
			if(inode.stat.isDirectory()) throw new EISDIRException(path);
			inode.data = data;
			inode.stat.setSize(length);
		} catch(ENOENTException exc) {
			makeInode(path, (inode)->{ inode.data = data; });
		}
	}

	@Override
	public File open(String path, int mode) throws IOException {
		return new RAMFile(this, path, mode);
	}

	@Override
	public void truncate(String path, long size) throws IOException {
		ByteBuffer newData = ByteBuffer.allocate((int) size);
		Inode inode;
		try {
			inode = lookup(path);
		} catch(ENOENTException exc) {
			inode = makeInode(path, (inode_)->{});
		}
		
		inode.data = newData.put(inode.data, 0, Math.min(inode.data.length,	(int) size)).array();
		inode.stat.setSize(inode.data.length);
	}

	@Override
	public FS scopedFS(String path) throws IOException {
		if(!exists(path)) mkdirp(path);
		return new RAMFS(unscopedPath(path), this);
	}
	
	@Override
	public RAMFS unscopedFS() throws IOException {
		return new RAMFS("/", this);
	}
	
	protected Inode lookup(String path, boolean followSymlinks) throws IOException {
		return followSymlinks ? lookup(path) : llookup(path);
	}
	
	protected Inode lookup(String path) throws IOException {
		Inode inode = llookup(path);
		if(inode.stat.isSymlink()) {
			return lookup(new String(inode.data));
		}
		
		return inode;
	}
	
	protected Inode llookup(String path) throws IOException {
		if(path.equals("")) path = "/";
		synchronized(inodesByPath) {
			Inode inode = inodesByPath.getOrDefault(unscopedPath(path), null);
			if(inode == null) {
				String parent = path;
				do {
					parent = dirname(parent);
					Inode parentInode = inodesByPath.getOrDefault(unscopedPath(parent), null); 
					if(parentInode != null) {
						if(parentInode.stat.isSymlink()) {
							String target = readlink(parent);
							String rewrittenPath = target + "/" + path.substring(parent.length());
							return llookup(rewrittenPath);
						} else {
							break;
						}
					}
				} while(!parent.equals("/"));
				throw new ENOENTException(path);
			}
			return inode;
		}
	}
	
	protected String canonicalPath(String path) throws IOException {
		String unscoped = unscopedPath(path);

		if(unscoped.length() == 0 || unscoped.equals("/")) {
			return "/";
		}
		
		if(unscoped.charAt(0) != '/') unscoped = "/" + unscoped;
		String[] comps = unscoped.split("/");
		String[] partials = new String[comps.length];

		for(int i =  0; i < comps.length; i++) {
			String p;
			if(i == 0) {
				p = "/";
			} else if(i == 1) {
				p = partials[i-1] + comps[i];
			} else {
				p = partials[i-1] + "/" + comps[i];
			}
			
			Inode inode = inodesByPath.getOrDefault(p, null);
			if(inode != null && inode.stat.isSymlink()) {
				String target = readlink(p);
				if(target.startsWith("/")) {
					partials[i] = target;
				} else if(i == 0) {
					partials[i] = Paths.get("/", target).normalize().toString();
				} else {
					partials[i] = Paths.get(partials[i-1], target).normalize().toString();
				}
			} else {
				partials[i] = p;
			}
		}
		
		String result = partials[partials.length - 1];
		if(result.charAt(0) != '/') result = "/" + result;
		return result;
	}
	
	protected String canonicalSubpath(String path) throws IOException {
		return Paths.get(canonicalPath(dirname(path)), basename(path)).toString();
	}
	
	protected synchronized void setInode(String path, Inode inode) throws IOException {
		synchronized(inodesByPath) {
			inodesByPath.put(canonicalSubpath(path), inode);
		}
	}
	
	protected void clearInode(String path) throws IOException {
		try {
			if(unscopedPath(path).equals("/")) return;
			synchronized(inodesByPath) {
				inodesByPath.remove(canonicalSubpath(path));
			}
		} catch(ENOENTException exc) {}
	}
	
	protected Inode makeInode(String path, InodeMaker lambda) throws IOException {
		Inode inode = new Inode();
		lambda.make(inode);
		inode.stat.setSize(inode.data.length);
		setInode(path, inode);
		return inode;
	}
	
	protected String scopedPath(String path) throws ENOENTException {
		Path p = Paths.get(path).normalize();
		if(!p.startsWith(scope)) throw new ENOENTException(path);
		String suffix = scope;
		if(!scope.endsWith("/")) suffix += "/";
		String scoped = path.substring(suffix.length());
		if(scoped.startsWith("/")) scoped = scoped.substring(1);
		return scoped;
	}
	
	protected String unscopedPath(String path) throws ENOENTException {
		if(path.equals(".")) path = "/";
		Path p = Paths.get("/", scope, path).normalize();
		if(!p.startsWith(scope)) throw new ENOENTException(path);
		if(!p.startsWith("/")) return "/" + p.toString();
		return p.toString();
	}
	
	public String getRoot() {
		return scope;
	}
	
	public String toString() {
		return this.getClass().getSimpleName() + " " + this.getRoot();
	}
}
