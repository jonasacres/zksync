package com.acrescrypto.zksync.fs.compositefs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

import com.acrescrypto.zksync.TaskPool;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

/** Pulls data from a variety of "supplementary" sources, storing them as they are acquired to a permanent "backing fs."
 * e.g.  a bunch of supplementary network filesystems, backing to a LocalFS instance.
 *
 */
public class CompositeFS extends FS {
	protected FS backingFS; // We check this first, and if something isn't here, we try to download it and write it here.
	ArrayList<FS> supplementaries = new ArrayList<FS>(); // This is were we look for stuff that's not in our backingFS.
	HashSet<String> pendingPaths = new HashSet<String>();
	TaskPool<String,FS> expectationPool; // thread pool for acquiring expected paths
	
	/** TODO: Might be nice to set a max downloads per supplementary instead of just a global max. */
	int maxSimultaneousExpectDownloads = 16; // number of expected paths we can download simultaneously
	int maxSimultaneousSupplementQueries = 8; // number of concurrent stat requests we will make to supplementaries
	
	/** TODO: scale these parameters at runtime according to observed performance. Carrier pigeons and datacenters have different expectations...
	 * maybe even select supplementaries based on observed performance.
	 */
	int minThroughput = 56600; // expected minimum throughput bits/second; used to identify FS performing too slowly
	int expectedLatency = 100; // expected ms rtt; used to identify FS performing too slowly
	
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
	
	public synchronized void addSupplementaryFS(FS supplementaryFS) {
		supplementaries.add(supplementaryFS);
	}

	@Override
	public Stat stat(String path) throws IOException {
		try {
			return backingFS.stat(path);
		} catch(ENOENTException exc) {
			return statPool(path, true).waitForResult().getResult();
		}
	}

	@Override
	public Stat lstat(String path) throws IOException {
		try {
			return backingFS.lstat(path);
		} catch(ENOENTException exc) {
			return statPool(path, false).waitForResult().getResult();
		}
	}

	@Override
	public Directory opendir(String path) throws IOException {
		ensurePresent(path);
		return backingFS.opendir(path);
	}

	@Override
	public void mkdir(String path) throws IOException {
		ensurePresent(dirname(path));
		backingFS.mkdir(path);
	}

	@Override
	public void mkdirp(String path) throws IOException {
		backingFS.mkdirp(path);
	}

	@Override
	public void rmdir(String path) throws IOException {
		ensurePresent(dirname(path));
		backingFS.rmdir(path);
	}

	@Override
	public void unlink(String path) throws IOException {
		ensurePresent(dirname(path));
		backingFS.unlink(path);
	}

	@Override
	public void link(String target, String link) throws IOException {
		ensurePresent(target);
		backingFS.link(target, link);
	}

	@Override
	public void symlink(String target, String link) throws IOException {
		ensurePresent(dirname(link));
		backingFS.symlink(target, link);
	}

	@Override
	public String readlink(String link) throws IOException {
		ensurePresent(link);
		return backingFS.readlink(link);
	}

	@Override
	public void mknod(String path, int type, int major, int minor) throws IOException {
		ensurePresent(dirname(path));
		backingFS.mknod(path, type, major, minor);
	}

	@Override
	public void mkfifo(String path) throws IOException {
		ensurePresent(dirname(path));
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
		ensurePresent(dirname(path));
		backingFS.write(path, contents);
	}

	@Override
	public byte[] read(String path) throws IOException {
		ensurePresent(path);
		return backingFS.read(path);
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
	
	protected void startExpectationPool() {
		expectationPool = new TaskPool<String,FS>(maxSimultaneousExpectDownloads, null)
				.setTask((String path) -> {
					try {
						return ensurePresent(path);
					} catch (IOException e) {
						return null;
					}
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
	protected FS ensurePresent(String path) throws IOException {
		synchronized(pendingPaths) {
			while(pendingPaths.contains(path)) {
				try {
					pendingPaths.wait();
				} catch (InterruptedException e) {}
			}
			
			pendingPaths.add(path);
		}
		
		FS result;
		try {
			result = backingFS.exists(path) ? backingFS : acquire(path);
		} finally {			
			synchronized(pendingPaths) {
				pendingPaths.remove(path);
			}
		}
		
		return result;
	}
	
	protected TaskPool<FS,Stat> statPool(String path, boolean followLinks) throws IOException {
		ArrayList<FS> shuffled;
		synchronized(this) {
			if(supplementaries.isEmpty()) throw new ENOENTException(path);
			shuffled = new ArrayList<FS>(supplementaries);
		}
		Collections.shuffle(shuffled);
		return new TaskPool<FS,Stat>(maxSimultaneousSupplementQueries, shuffled)
				.setTask((FS fs) -> {
					try {
						return followLinks ? fs.stat(path) : fs.lstat(path);
					} catch (IOException e) {
						return null;
					}
				})
				.done();
	}
	
	protected FS acquire(String path) throws IOException {
		while(true) {
			FS remoteFS = null;
			try {
				TaskPool<FS,Stat> pool = statPool(path, false).launch().waitForResult(2*expectedLatency);
				
				remoteFS = pool.getResultInput();
				Stat stat = pool.getResult();
				if(remoteFS == null) throw new SupplementaryFSTimeoutException();
				
				acquireSpecific(path, stat, remoteFS);				
				backingFS.applyStat(path, stat);
				
				return remoteFS;
			} catch(SupplementaryFSFailureException | SupplementaryFSTimeoutException exc) {
				failSupplementaryFS(remoteFS);
			}
		}
	}
	
	protected void acquireSpecific(String path, Stat stat, FS remoteFS) throws IOException {
		if(remoteFS == null) throw new ENOENTException(path);
		else if(stat.isSymlink()) acquireSymlink(path, stat, remoteFS);
		else if(stat.isDevice()) acquireDevice(path, stat, remoteFS);
		else if(stat.isFifo()) acquireFifo(path, stat, remoteFS);
		else if(stat.isDirectory()) acquireDirectory(path, stat, remoteFS);
		else if(stat.isRegularFile()) acquireRegularFile(path, stat, remoteFS);
		else throw new EINVALException(path);
	}
	
	protected void acquireSymlink(String path, Stat stat, FS remoteFS) throws IOException {
		Object r = TaskPool.oneoff(2*expectedLatency, () -> {
			try {
				return remoteFS.readlink(path);
			} catch(IOException exc) {
				return exc;
			}
		});
		
		if(r == null) {
			throw new SupplementaryFSTimeoutException();
		} else if(IOException.class.isInstance(r)) {
			throw (IOException) r;
		} else if(String.class.isInstance(r)) {
			String target = (String) r;
			try {
				ensurePresent(target);
			} catch(ENOENTException exc) {} // squelch these to allow cloning of dead symlinks
			
			backingFS.symlink(target, path);
		} else {
			throw new RuntimeException();
		}
	}
	
	protected void failSupplementaryFS(FS failedFS) {
		synchronized(this) {
			supplementaries.remove(failedFS);
		}
		
		// TODO: tell whoever feeds us these connections that we just lost one
	}

	private void acquireDevice(String path, Stat stat, FS remoteFS) throws IOException {
		if(stat.isBlockDevice()) {
			backingFS.mknod(path, Stat.TYPE_BLOCK_DEVICE, stat.getDevMajor(), stat.getDevMinor());
		}
		
		if(stat.isCharacterDevice()) {
			backingFS.mknod(path, Stat.TYPE_CHARACTER_DEVICE, stat.getDevMajor(), stat.getDevMinor());
		}
	}
	
	private void acquireFifo(String path, Stat stat, FS remoteFS) throws IOException {
		backingFS.mkfifo(path);
	}
	
	private void acquireDirectory(String path, Stat stat, FS remoteFS) throws IOException {
		backingFS.mkdir(path);
	}
	
	private void acquireRegularFile(String path, Stat stat, FS remoteFS) throws IOException {
		// Need to keep exceptions in the supplementary FS distinct so we can try an alternative FS if needed
		final int readSize = 65536;
		long expectedMaxTime = 8*readSize / minThroughput + expectedLatency;
		
		File inFile, outFile;
		outFile = backingFS.open(path, File.O_WRONLY|File.O_CREAT|File.O_TRUNC);
		
		try {
			inFile = remoteFS.open(path, File.O_RDONLY);
		} catch(IOException exc) {
			throw new SupplementaryFSFailureException(exc);
		}
		
		/* TODO: consider a way to tweak the timeout so that we allow intermittent bursts of huge delays, as long as
		 * overall throughput is acceptable.
		 */
		while(inFile.hasData()) {
			Object r = TaskPool.oneoff(expectedMaxTime, () -> {
				byte[] data = new byte[readSize];
				try {
					return inFile.read(data, 0, data.length);
				} catch(IOException exc) {
					return exc;
				}
			});
			
			byte[] b = new byte[0];
			if(r == null) {
				throw new SupplementaryFSTimeoutException();
			} if(b.getClass().isInstance(r)) {
				outFile.write((byte[]) r);
			} else if(IOException.class.isInstance(r)) {
				throw (IOException) r;
			} else {
				throw new RuntimeException();
			}
		}
		
		try {
			inFile.close();
		} catch(IOException exc) {
			throw new SupplementaryFSFailureException(exc);
		}
		
		outFile.close();
	}
}
