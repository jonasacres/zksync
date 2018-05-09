package com.acrescrypto.zksync.fs.backedfs;

import java.io.IOException;
import java.util.HashSet;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class BackedFS extends FS {
	protected FS cacheFS; // We check this first, and if something isn't here, we try to download it and write it here.
	protected FS backupFS; // This is where we go to look for data we don't have.

	HashSet<String> pendingPaths = new HashSet<String>();

	public BackedFS(FS cacheFS, FS backupFS) {
		this.cacheFS = cacheFS;
		this.backupFS = backupFS;
	}

	@Override
	public Stat stat(String path) throws IOException {
		try {
			return cacheFS.stat(path);
		} catch(ENOENTException exc) {
			return backupFS.stat(path);
		}
	}

	@Override
	public Stat lstat(String path) throws IOException {
		try {
			return cacheFS.lstat(path);
		} catch(ENOENTException exc) {
			return backupFS.stat(path);
		}
	}

	@Override
	public Directory opendir(String path) throws IOException {
		ensurePresent(path);
		return cacheFS.opendir(path);
	}

	@Override
	public void mkdir(String path) throws IOException {
		ensureParentPresent(path);
		cacheFS.mkdir(path);
	}

	@Override
	public void mkdirp(String path) throws IOException {
		cacheFS.mkdirp(path);
	}

	@Override
	public void rmdir(String path) throws IOException {
		ensureParentPresent(path);
		cacheFS.rmdir(path);
	}

	@Override
	public void unlink(String path) throws IOException {
		ensureParentPresent(path);
		cacheFS.unlink(path);
	}

	@Override
	public void link(String target, String link) throws IOException {
		ensurePresent(target);
		cacheFS.link(target, link);
	}

	@Override
	public void symlink(String target, String link) throws IOException {
		ensureParentPresent(target);
		cacheFS.symlink(target, link);
	}

	@Override
	public String readlink(String link) throws IOException {
		ensurePresent(link);
		return cacheFS.readlink(link);
	}

	@Override
	public void mknod(String path, int type, int major, int minor) throws IOException {
		ensureParentPresent(path);
		cacheFS.mknod(path, type, major, minor);
	}

	@Override
	public void mkfifo(String path) throws IOException {
		ensureParentPresent(path);
		cacheFS.mkfifo(path);
	}

	@Override
	public void chmod(String path, int mode) throws IOException {
		ensurePresent(path);
		cacheFS.chmod(path, mode);
	}

	@Override
	public void chown(String path, int uid) throws IOException {
		ensurePresent(path);
		cacheFS.chown(path, uid);
	}

	@Override
	public void chown(String path, String user) throws IOException {
		ensurePresent(path);
		cacheFS.chown(path, user);
	}

	@Override
	public void chgrp(String path, int gid) throws IOException {
		ensurePresent(path);
		cacheFS.chgrp(path, gid);
	}

	@Override
	public void chgrp(String path, String group) throws IOException {
		ensurePresent(path);
		cacheFS.chgrp(path, group);
	}

	@Override
	public void setMtime(String path, long mtime) throws IOException {
		ensurePresent(path);
		cacheFS.setMtime(path, mtime);
	}

	@Override
	public void setCtime(String path, long ctime) throws IOException {
		ensurePresent(path);
		cacheFS.setCtime(path, ctime);
	}

	@Override
	public void setAtime(String path, long atime) throws IOException {
		ensurePresent(path);
		cacheFS.setAtime(path, atime);
	}

	@Override
	public void write(String path, byte[] contents) throws IOException {
		ensureParentPresent(path);
		cacheFS.write(path, contents);
	}

	@Override
	public byte[] read(String path) throws IOException {
		try {
			return cacheFS.read(path);
		} catch(ENOENTException exc) {
			return backupFS.read(path);
		}
	}

	@Override
	public File open(String path, int mode) throws IOException {
		try {
			return cacheFS.open(path, mode);
		} catch(ENOENTException exc) {
			return backupFS.open(path, mode);
		}
	}

	@Override
	public void truncate(String path, long size) throws IOException {
		ensurePresent(path);
		cacheFS.truncate(path, size);
	}
	
	/** Acquire a page from our supplementary sources and write to our backing FS, blocking until we've done so.
	 * Throw an ENOENTException if no one has the file.
	 * 
	 * @param path
	 * @throws IOException
	 */
	protected void ensurePresent(String path) throws IOException {
		synchronized(pendingPaths) {
			while(pendingPaths.contains(path)) {
				try {
					pendingPaths.wait();
				} catch (InterruptedException e) {}
			}
			
			pendingPaths.add(path);
		}
		
		try {
			if(!cacheFS.exists(path)) {
				read(path);
			}
		} finally {			
			synchronized(pendingPaths) {
				pendingPaths.remove(path);
			}
		}
	}
	
	protected void ensureParentPresent(String path) throws IOException {
		/* We COULD go do an ensurePresent on parent and make sure it's supposed to be a directory, but we probably
		 * already have a good reason to believe that it is, and skipping that check saves us a lot of network time.
		 */
		String parent = dirname(path);
		mkdirp(parent);
	}
	
	@Override
	public BackedFS scopedFS(String subpath) throws IOException {
		return new BackedFS(cacheFS.scopedFS(subpath), backupFS.scopedFS(subpath));
	}
}
