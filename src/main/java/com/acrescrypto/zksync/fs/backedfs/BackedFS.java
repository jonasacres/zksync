package com.acrescrypto.zksync.fs.backedfs;

import java.io.IOException;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.SwarmTimeoutException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.TimedReader;
import com.acrescrypto.zksync.fs.swarmfs.SwarmFS;
import com.acrescrypto.zksync.utility.Util;

/** This is written, and partially tested, as a generic FS. In truth, there are some MAJOR gaps.
 * Because right now BackedFS only has one use case, and that's to pair a LocalFS to a SwarmFS to retrieve pages,
 * there's not much use of devices/symlinks/hardlinks/etc. Therefore, it has some odd semantics.
 * 
 * 1. The behavior of accessing anything except a directory or regular file is untested and probably broken.
 *    This includes symlinks.
 *    
 * 2. Calls that create filesystem entries (e.g. link, symlink, write, mkdir) implicitly mkdirp the parent of
 *    the path you supply, on the theory that saving the latency of stat'ing every directory in the path from
 *    the backupFS (which might be across a network connection) is of greater value than preserving consistency
 *    in behavior in this regard.
 * 
 * None of the above should be relied upon, as it may be change later if BackedFS winds up having other uses.
 */
public class BackedFS extends FS {
	protected FS cacheFS; // We check this first, and if something isn't here, we try to download it and write it here.
	protected FS backupFS; // This is where we go to look for data we don't have.
	protected final Logger logger = LoggerFactory.getLogger(BackedFS.class);

	HashSet<String> pendingPaths = new HashSet<String>();

	public BackedFS(FS cacheFS, FS backupFS) {
		this.cacheFS  = cacheFS;
		this.backupFS = backupFS;
	}
	
	@Override
	public long storageSize() throws IOException {
	    return cacheFS.storageSize();
	}
	
	@Override
	public boolean exists(String path) {
		if(path.equals("/")) return cacheFS.exists(path);
		return super.exists(path);
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
			return backupFS.lstat(path);
		}
	}

	@Override
	/** This will almost certainly end in tears, since we can't guarantee that the directory
	 * actually has correct contents. But support is provided.
	 */
	public Directory opendir(String path, Stat stat) throws IOException {
		if(!cacheFS.exists(path)) {
			if(backupFS.stat(path).isDirectory()) {
				mkdirp(path);
			}
		}
		return cacheFS.opendir(path);
	}
	
	@Override
	public void mv(String source, String dest) throws IOException {
		ensureParentPresent(dest);
		cacheFS.mv(source, dest);
	}
	
	@Override
	public void cp(String source, String dest) throws IOException {
		ensureParentPresent(dest);
		cacheFS.cp(source, dest);
	}

	@Override
	public void mkdir(String path) throws IOException {
		if(dirname(path).equals(path)) {
			if(exists(path)) throw new EEXISTSException(path);
			cacheFS.mkdir(path);
		} else {
			ensureParentPresent(path);
			cacheFS.mkdir(path);
		}
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
		ensurePresent(target, -1);
		ensureParentPresent(link);
		cacheFS.link(target, link);
	}

	@Override
	public void symlink(String target, String link) throws IOException {
		ensureParentPresent(link);
		cacheFS.symlink(target, link);
	}
	
	@Override
	public void symlink_unsafe(String target, String link) throws IOException {
		ensureParentPresent(link);
		cacheFS.symlink_unsafe(target, link);
	}

	@Override
	public String readlink(String link) throws IOException {
		ensurePresent(link, -1);
		return cacheFS.readlink(link);
	}
	
	@Override
	public String readlink_unsafe(String link) throws IOException {
		ensurePresent(link, -1);
		return cacheFS.readlink_unsafe(link);
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
	public void chmod(String path, int mode, boolean followSymlinks) throws IOException {
		ensurePresent(path, -1);
		cacheFS.chmod(path, mode, followSymlinks);
	}

	@Override
	public void chown(String path, int uid, boolean followSymlinks) throws IOException {
		ensurePresent(path, -1);
		cacheFS.chown(path, uid, followSymlinks);
	}

	@Override
	public void chown(String path, String user, boolean followSymlinks) throws IOException {
		ensurePresent(path, -1);
		cacheFS.chown(path, user, followSymlinks);
	}

	@Override
	public void chgrp(String path, int gid, boolean followSymlinks) throws IOException {
		ensurePresent(path, -1);
		cacheFS.chgrp(path, gid, followSymlinks);
	}

	@Override
	public void chgrp(String path, String group, boolean followSymlinks) throws IOException {
		ensurePresent(path, -1);
		cacheFS.chgrp(path, group, followSymlinks);
	}

	@Override
	public void setMtime(String path, long mtime, boolean followSymlinks) throws IOException {
		ensurePresent(path, -1);
		cacheFS.setMtime(path, mtime, followSymlinks);
	}

	@Override
	public void setCtime(String path, long ctime, boolean followSymlinks) throws IOException {
		ensurePresent(path, -1);
		cacheFS.setCtime(path, ctime, followSymlinks);
	}

	@Override
	public void setAtime(String path, long atime, boolean followSymlinks) throws IOException {
		ensurePresent(path, -1);
		cacheFS.setAtime(path, atime, followSymlinks);
	}

	@Override
	public void write(String path, byte[] contents, int offset, int length) throws IOException {
		ensureParentPresent(path);
		cacheFS.write(path, contents, offset, length);
	}

	@Override
	public byte[] read(String path) throws IOException {
		ensurePresent(path, -1);
		return cacheFS.read(path);
	}

	@Override
	public File open(String path, int mode) throws IOException {
		if((mode & File.O_TRUNC) == 0) {
			try {			
				ensurePresent(path, -1);
			} catch(ENOENTException exc) {
				if((mode & File.O_CREAT) == 0) throw exc;
			}
		}
		
		if((mode & File.O_CREAT) != 0)  {
			String dn = dirname(path);
			if(!cacheFS.exists(dn)) mkdirp(dn);
		}
		return cacheFS.open(path, mode);
	}

	@Override
	public void truncate(String path, long size) throws IOException {
		if(size != 0) {
			ensurePresent(path, -1);
		} else {
			cacheFS.write(path, new byte[0]);
			return;
		}
		
		cacheFS.truncate(path, size);
	}
	
	/** Acquire a page from our supplementary sources and write to our backing FS, blocking until we've done so.
	 * Wait up to timeoutMs milliseconds. 
	 * Throw an ENOENTException if no one has the file.
	 * 
	 * @param path
	 * @throws IOException
	 */
	public void ensurePresent(String path, long timeoutMs) throws IOException {
		if(!pendingPaths.contains(path) && cacheFS.exists(path, false)) return;
		long deadline = System.currentTimeMillis() + timeoutMs;
		synchronized(this) {
			while(pendingPaths.contains(path)) {
				try {
					long timeoutRemainingMs;
					if(timeoutMs >= 0) {
						timeoutRemainingMs = Math.max(deadline - System.currentTimeMillis(), 0);
						if(timeoutRemainingMs <= 0) {
							throw new SwarmTimeoutException(path);
						}
					} else {
						timeoutRemainingMs = Long.MAX_VALUE;
					}
					
					if(timeoutRemainingMs > 0) {
						this.wait(timeoutRemainingMs);
					} else if(timeoutMs == 0) {
						throw new SwarmTimeoutException(path);
					} else {
						this.wait();
					}
				} catch (InterruptedException e) {}
			}
			
			pendingPaths.add(path);
		}
		
		try {
			if(!cacheFS.exists(path)) {
				Stat stat = backupFS.stat(path);
				long timeoutRemainingMs;
				if(timeoutMs >= 0) {
					timeoutRemainingMs = Math.max(deadline - System.currentTimeMillis(), 0);
				} else {
					timeoutRemainingMs = Long.MAX_VALUE;
				}
				byte[] data;
				
				if(backupFS instanceof TimedReader) {
					data = ((TimedReader) backupFS).read(path, timeoutRemainingMs);
				} else {
					data = backupFS.read(path);
				}
				
				if(!cacheFS.exists(path) || cacheFS.stat(path).getSize() != stat.getSize()) {
					if(!(backupFS instanceof SwarmFS)) {
						// hacky, but SwarmFS already writes our data, and this becomes redundant, and potentially race-inducing
						cacheFS.write(path, data);
						cacheFS.applyStat(path, stat);
					}
					
					boolean settled = Util.waitUntil(1000,
							()->{
								try {
									return cacheFS.stat(path).getSize() == stat.getSize();
								} catch(IOException exc) {
									logger.error("BackedFS: Caught exception syncing path {}", path, exc);
									return false;
								}
							});
					if(!settled) {
						logger.error("BackedFS: Unable to sync path {}, expected size {}, got {}",
								path,
								stat.getSize(),
								cacheFS.stat(path).getSize());
					}
				}
			}
		} finally {			
			synchronized(this) {
				pendingPaths.remove(path);
				this.notifyAll();
			}
		}
	}
	
	protected void ensureParentPresent(String path) throws IOException {
		/* We COULD go do an ensurePresent on parent and make sure it's supposed to be a directory, but we probably
		 * already have a good reason to believe that it is, and skipping that check saves us a lot of network time.
		 */
		String parent = dirname(path);
		if(!cacheFS.exists(parent)) {
			mkdirp(parent);
		}
	}
	
	@Override
	public BackedFS scopedFS(String subpath) throws IOException {
		return new BackedFS(cacheFS.scopedFS(subpath), backupFS.scopedFS(subpath));
	}
	
	@Override
	public BackedFS unscopedFS() throws IOException {
		return new BackedFS(cacheFS.unscopedFS(), backupFS.unscopedFS());
	}

	public FS getCacheFS() {
		return cacheFS;
	}
	
	public FS getBackupFS() {
		return backupFS;
	}
	
	public String toString() {
		return this.getClass().getSimpleName() + " cache: (" + cacheFS + ") backup: (" + backupFS + ")";
	}
}
