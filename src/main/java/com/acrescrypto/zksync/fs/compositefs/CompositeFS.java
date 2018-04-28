package com.acrescrypto.zksync.fs.compositefs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import com.acrescrypto.zksync.TaskPool;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.compositefs.CompositeReadOperation.IncomingDataValidator;
import com.acrescrypto.zksync.fs.compositefs.CompositeReadOperation.IncomingStatValidator;

/** Pulls data from a variety of "supplementary" sources, storing them as they are acquired to a permanent "backing fs."
 * e.g.  a bunch of supplementary network filesystems, backing to a LocalFS instance.
 * 
 * This is useful for ZKArchive storage, where all the files are immutable and each peer either has the file or it does
 * not.
 *
 */
public class CompositeFS extends FS {
	protected FS backingFS; // We check this first, and if something isn't here, we try to download it and write it here.
	protected IncomingDataValidator dataValidator = (p, d)->true;
	protected IncomingStatValidator statValidator = (p, s)->true;
	ArrayList<FS> supplementaries = new ArrayList<FS>(); // This is were we look for stuff that's not in our backingFS.
	HashSet<String> pendingPaths = new HashSet<String>();
	TaskPool<String,Object> expectationPool; // thread pool for acquiring expected paths
	
	int maxSimultaneousExpectDownloads = 16; // number of expected paths we can download simultaneously
	
	public class SupplementaryFSFailureException extends IOException {
		private static final long serialVersionUID = 1L;
		public IOException exception;
		
		public SupplementaryFSFailureException(IOException exception) {
			this.exception = exception;
		}
	}
	
	public class SupplementaryFSTimeoutException extends IOException {
		private static final long serialVersionUID = 1L;
	}
	
	public CompositeFS(FS backingFS) {
		this.backingFS = backingFS;
	}
	
	public CompositeFS setDataValidator(IncomingDataValidator dataValidator) {
		this.dataValidator = dataValidator;
		return this;
	}
	
	public CompositeFS setStatValidator(IncomingStatValidator statValidator) {
		this.statValidator = statValidator;
		return this;
	}
	
	public synchronized void addSupplementaryFS(FS supplementaryFS) {
		supplementaries.add(supplementaryFS);
	}

	@Override
	public Stat stat(String path) throws IOException {
		try {
			return backingFS.stat(path);
		} catch(ENOENTException exc) {
			return (new CompositeReadOperation(this, path, statValidator, dataValidator, CompositeReadOperation.MODE_STAT)).waitForStat().getStat();
		}
	}

	@Override
	public Stat lstat(String path) throws IOException {
		try {
			return backingFS.lstat(path);
		} catch(ENOENTException exc) {
			return (new CompositeReadOperation(this, path, statValidator, dataValidator, CompositeReadOperation.MODE_LSTAT)).waitForStat().getStat();
		}
	}

	@Override
	public Directory opendir(String path) throws IOException {
		ensurePresent(path);
		return backingFS.opendir(path);
	}

	@Override
	public void mkdir(String path) throws IOException {
		ensureParentPresent(path);
		backingFS.mkdir(path);
	}

	@Override
	public void mkdirp(String path) throws IOException {
		backingFS.mkdirp(path);
	}

	@Override
	public void rmdir(String path) throws IOException {
		ensureParentPresent(path);
		backingFS.rmdir(path);
	}

	@Override
	public void unlink(String path) throws IOException {
		ensureParentPresent(path);
		backingFS.unlink(path);
	}

	@Override
	public void link(String target, String link) throws IOException {
		ensurePresent(target);
		backingFS.link(target, link);
	}

	@Override
	public void symlink(String target, String link) throws IOException {
		ensureParentPresent(target);
		backingFS.symlink(target, link);
	}

	@Override
	public String readlink(String link) throws IOException {
		ensurePresent(link);
		return backingFS.readlink(link);
	}

	@Override
	public void mknod(String path, int type, int major, int minor) throws IOException {
		ensureParentPresent(path);
		backingFS.mknod(path, type, major, minor);
	}

	@Override
	public void mkfifo(String path) throws IOException {
		ensureParentPresent(path);
		backingFS.mkfifo(path);
	}

	@Override
	public void chmod(String path, int mode) throws IOException {
		ensurePresent(path);
		backingFS.chmod(path, mode);
	}

	@Override
	public void chown(String path, int uid) throws IOException {
		ensurePresent(path);
		backingFS.chown(path, uid);
	}

	@Override
	public void chown(String path, String user) throws IOException {
		ensurePresent(path);
		backingFS.chown(path, user);
	}

	@Override
	public void chgrp(String path, int gid) throws IOException {
		ensurePresent(path);
		backingFS.chgrp(path, gid);
	}

	@Override
	public void chgrp(String path, String group) throws IOException {
		ensurePresent(path);
		backingFS.chgrp(path, group);
	}

	@Override
	public void setMtime(String path, long mtime) throws IOException {
		ensurePresent(path);
		backingFS.setMtime(path, mtime);
	}

	@Override
	public void setCtime(String path, long ctime) throws IOException {
		ensurePresent(path);
		backingFS.setCtime(path, ctime);
	}

	@Override
	public void setAtime(String path, long atime) throws IOException {
		ensurePresent(path);
		backingFS.setAtime(path, atime);
	}

	@Override
	public void write(String path, byte[] contents) throws IOException {
		ensureParentPresent(path);
		backingFS.write(path, contents);
	}

	@Override
	public byte[] _read(String path) throws IOException {
		ensurePresent(path);
		return backingFS._read(path);
	}

	@Override
	public File open(String path, int mode) throws IOException {
		ensurePresent(path);
		return backingFS.open(path, mode);
	}

	@Override
	public void truncate(String path, long size) throws IOException {
		ensurePresent(path);
		backingFS.truncate(path, size);
	}
	
	@Override
	public synchronized void expect(String path) {
		expectationPool.add(path);
	}
	
	public Collection<FS> getSupplementaries() {
		return supplementaries;
	}
	
	protected void startExpectationPool() {
		expectationPool = new TaskPool<String,Object>(maxSimultaneousExpectDownloads, null)
				.setTask((String path) -> {
					try {
						ensurePresent(path);
					} catch (IOException e) {
					}
					return null;
				})
				.holdOpen()
				.launch();
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
			if(!backingFS.exists(path)) {
				acquire(path);
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
	
	protected void acquire(String path) throws IOException {
		CompositeReadOperation op = new CompositeReadOperation(this, path, statValidator, dataValidator, CompositeReadOperation.MODE_DOWNLOAD);
		op.waitToFinish();
	}
	
	protected synchronized void failSupplementaryFS(FS fs, boolean evil) {
		supplementaries.remove(fs);
		if(evil) {
			fs.blacklist();
		}
		
		try {
			fs.close();
		} catch(IOException exc) {}
	}
	
	@Override
	public CompositeFS scopedFS(String subpath) throws IOException {
		CompositeFS scoped = new CompositeFS(backingFS.scopedFS(subpath));
		for(FS supplementary : supplementaries) {
			scoped.addSupplementaryFS(supplementary.scopedFS(subpath));
		}
		
		return scoped;
	}
}
