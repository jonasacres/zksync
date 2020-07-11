package com.acrescrypto.zksync.fs.zkfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.CommandFailedException;
import com.acrescrypto.zksync.exceptions.EISDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.FSPath;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;

import static java.nio.file.StandardWatchEventKinds.*;

public class FSMirror {
	static int numActive;
	ZKFS zkfs;
	FS target;
	RevisionTag lastRev;
	Thread watchThread;
	boolean suppressWatch;

	Logger logger = LoggerFactory.getLogger(FSMirror.class);
	MutableBoolean watchFlag = new MutableBoolean();
	
	/* We don't want a situation where syncing the archive to the target triggers our
	 * filesystem watch, which triggers a write to the archive, which triggers a write back
	 * to the target, which triggers a write to the archive, and so on. So we "squelch"
	 * writes for a configurable time (fs.settings.mirror.pathSquelchPeriodMs, default 100ms
	 * as of this writing).
	 * 
	 * This means we're blind to changes to the target FS for this interval after a sync.
	 */
	protected class SquelchedPath {
		String path;
		FS fs;
		long timestamp;
		
		public SquelchedPath(String path, FS fs) {
			this.path = path;
			this.fs = fs;
			this.timestamp = System.currentTimeMillis();
		}
		
		public boolean matches(String path, FS fs) {
			if(!path.equals(this.path)) return false;
			if(fs != this.fs) return false; 
			return true;
		}
		
		public boolean isExpired() {
			long timeout = zkfs.getArchive().getMaster().getGlobalConfig().getLong("fs.settings.mirror.pathSquelchPeriodMs");
			return System.currentTimeMillis() - timestamp > timeout;
		}
		
		@Override
		public int hashCode() {
			return path.hashCode();
		}
	}
	
	SnoozeThread archiveToTargetSyncThread;
	ConcurrentHashMap<String, Boolean> queuedArchivePaths = new ConcurrentHashMap<>();
	HashMap<String, SquelchedPath> squelchedPaths = new HashMap<>();
	
	public static int numActive() {
		return numActive;
	}
	
	private synchronized static void incrementActive() {
		numActive++;
	}
	
	private synchronized static void decrementActive() {
		numActive--;
	}

	public interface SyncOperation {
		void op() throws IOException;
	}

	public FSMirror(ZKFS zkfs, FS target) {
		this.zkfs = zkfs;
		this.target = target;
		this.lastRev = zkfs.baseRevision;
		
		ConfigFile globalConfig = zkfs.getArchive().getMaster().getGlobalConfig();
		SubscriptionService subService = globalConfig.getSubsciptionService();
		
		subService.subscribe("fs.settings.mirror.zkfsToHostSyncDelayMs").asLong((delayMs)->{
			updateSyncThreadProperties(delayMs, globalConfig.getLong("fs.settings.mirror.zkfsToHostSyncMaxDelayMs"));
		});
		
		subService.subscribe("fs.settings.mirror.zkfsToHostSyncMaxDelayMs").asLong((maxDelayMs)->{
			updateSyncThreadProperties(globalConfig.getLong("fs.settings.mirror.zkfsToHostSyncMaxDelayMs"), maxDelayMs);
		});
	}
	
	public FS getTarget() {
		return target;
	}
	
	public ZKFS getZKFS() {
		return zkfs;
	}
	
	/** Determine if a given path refers to a file in the ZKFS that can be safely mirrored
	 * to the target filesystem.
	 */
	public boolean canMirror(String path) throws IOException {
		/* ext3, ext4, support everything just fine. It's FAT32/NTFS/etc that are the problem.
		 * It's possible that, for instance, a Linux system might have an NTFS mount, in
		 * which case this logic fails. Similarly, it is also conceivable that a Windows
		 * system will have an ext4 mount.
		 * 
		 *  TODO: detect limited-capability filesystems on non-Windows systems
		 */
		if(!Util.isWindows()) return true;
		
		String std = FSPath.standardize(path);
		Stat stat;  
		try {
			stat   = zkfs.lstat(std);
		} catch(ENOENTException exc) {
			return true;
		}
		
		if(stat.isRegularFile()) return true;
		if(stat.isDirectory())   return true;
		return false;
	}

	public boolean isWatching() {
		return watchFlag.isTrue();
	}

	public void startWatch() throws IOException {
		if(watchFlag.isTrue()) return;
		MutableBoolean flag;
		flag = watchFlag = new MutableBoolean();
		watchFlag.setTrue();

		if(!(target instanceof LocalFS)) throw new UnsupportedOperationException("Cannot watch this kind of filesystem");

		LocalFS localTarget = (LocalFS) target;
		Path dir = localTarget.getRoot().toNativePath();
		WatchService watcher = dir.getFileSystem().newWatchService();
		HashMap<WatchKey, Path> pathsByKey = new HashMap<>();
		incrementActive();
		
		startArchiveToTargetSyncThread();
		
		suppressWatch = true;
		
		try {
			logger.info("FS {}: FSMirror starting watch of {}, {} active",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					localTarget.getRoot(),
					numActive());
			watchDirectory(dir, watcher, pathsByKey);
			suppressWatch = false;
	
			watchThread = new Thread( () -> watchThread(flag, watcher, pathsByKey) );
			watchThread.start();
		} catch(Throwable exc) {
			decrementActive();
			throw exc;
		}
	}

	public void stopWatch() {
		this.queuedArchivePaths.clear();
		stopArchiveToTargetSyncThread();
		
		if(watchThread == null) return;
		Thread stoppedThread = watchThread;
		logger.info("FS {}: FSMirror stopping watch of {}, {} active",
				Util.formatArchiveId(zkfs.archive.config.archiveId),
				((LocalFS) target).getRoot(),
				numActive());
		watchFlag.setFalse();
		Util.blockOnPoll(()->stoppedThread.isAlive());
		logger.info("FS {}: FSMirror stopped watch of {}, {} watches active",
				Util.formatArchiveId(zkfs.archive.config.archiveId),
				((LocalFS) target).getRoot(),
				numActive());
	}
	
	protected synchronized void startArchiveToTargetSyncThread() {
		this.queuedArchivePaths.clear();
		this.archiveToTargetSyncThread = new SnoozeThread(
				zkfs.getArchive().getMaster().getGlobalConfig().getInt("fs.settings.mirror.zkfsToHostSyncDelayMs"),
				zkfs.getArchive().getMaster().getGlobalConfig().getInt("fs.settings.mirror.zkfsToHostSyncMaxDelayMs"),
				false,
				()->{
					synchronized(this) {
						syncQueuedArchivePaths();
						startArchiveToTargetSyncThread();
					}
				});
	}
	
	protected synchronized void stopArchiveToTargetSyncThread() {
		if(this.archiveToTargetSyncThread == null) return;
		this.archiveToTargetSyncThread.cancel();
		this.archiveToTargetSyncThread = null;
	}
	
	protected void updateSyncThreadProperties(long delayMs, long maxDelayMs) {
		if(archiveToTargetSyncThread != null && !archiveToTargetSyncThread.isCancelled()) {
			archiveToTargetSyncThread.setDelayMs(delayMs);
			archiveToTargetSyncThread.setMaxTimeMs(maxDelayMs);
		}
	}

	protected void watchDirectory(Path dir, WatchService watcher, HashMap<WatchKey, Path> pathsByKey) throws IOException {
		Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path subdir, BasicFileAttributes attrs) throws IOException {
				WatchKey key = subdir.register(
						watcher,
						ENTRY_CREATE,
						ENTRY_DELETE,
						ENTRY_MODIFY
					);
				pathsByKey.put(key, subdir);

				String realPath = ((LocalFS) target)
						.getRoot()
						.relativize(FSPath.with(subdir))
						.toString();
				
				if(!suppressWatch) {
					suspectedTargetPathChange(realPath);
				}

				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				String realPath = ((LocalFS) target)
					.getRoot()
					.relativize(FSPath.with(file))
					.toPosix();
				
				if(!suppressWatch) {
					suspectedTargetPathChange(realPath);
				}
				
				return FileVisitResult.CONTINUE;
			}
			
			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
				logger.error("FS {}: Caught exception attempting to visit {} in watchDirectory of {}",
						Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
						file.toString(),
						dir.toString(),
						exc);
				// we can have bad stuff like mode changes and unlinks happen underneath us, so just carry on
				return FileVisitResult.CONTINUE;
			}
		});
	}

	protected void watchThread(MutableBoolean flag, WatchService watcher, HashMap<WatchKey, Path> pathsByKey) {
		Thread.currentThread().setName("FSMirror watch thread");
		try {
			while(flag.booleanValue()) {
				boolean shouldContinue = watchThreadBody(flag, watcher, pathsByKey);
				if(!shouldContinue) break;
			}
		} catch(Throwable exc) {
			logger.error("FS {}: Watch thread caught exception", exc);
			exc.printStackTrace();
			throw exc;
		} finally {
			suppressWatch = false;
			try {
				watcher.close();
			} catch (IOException exc) {
				logger.error("FS {}: FSMirror unable to close watcher",
						Util.formatArchiveId(zkfs.archive.config.archiveId),
						exc);
			}
			decrementActive();
		}
	}
	
	protected boolean watchThreadBody(MutableBoolean flag, WatchService watcher, HashMap<WatchKey, Path> pathsByKey) {
		try {
			try {
				checkDirectories(pathsByKey);
			} catch (IOException exc) {
				logger.error("FS {}: FSMirror caught exception checking directories for changes",
						Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
						exc);
			}
			WatchKey key = watcher.poll(10, TimeUnit.MILLISECONDS);
			if(key == null || flag.isFalse()) {
				return true;
			}
			try {
				for(WatchEvent<?> event : key.pollEvents()) {
					WatchEvent.Kind<?> kind = event.kind();
					if(kind == OVERFLOW) {
						logger.warn("FS {}: FSMirror caught overflow; some local filesystem changes may not have made it into the archive.",
								Util.formatArchiveId(zkfs.archive.config.archiveId));
						continue;
					}

					if(!(event.context() instanceof Path)) continue;

					Path filename = (Path) event.context();
					Path fullPath = pathsByKey.get(key).resolve(filename);
					Path basePath = ((LocalFS) target).getRoot().toNativePath();
					String realPath = FSPath.with(basePath.relativize(fullPath).toString()).toPosix();

					try {
						Stat stat = getLstat(target, realPath);

						if(kind == ENTRY_CREATE && stat != null && stat.isDirectory()) {
							watchDirectory(fullPath, watcher, pathsByKey);
						}
						
						if(kind != ENTRY_CREATE || stat == null || !stat.isRegularFile() || stat.getSize() == 0) {
							/* we don't want ENTRY_CREATE for files, because that also generates ENTRY_MODIFY...
							 * but we DO want it if the file size is zero, because we DON'T get the MODIFY for empty files.
							 */
							observedTargetPathChange(realPath);
						}
					} catch(ENOENTException|CommandFailedException exc) {
						// ignore it; the file was deleted from underneath us
					} catch(IOException exc) {
						logger.error("FS {}: FSMirror caught exception mirroring local FS",
								Util.formatArchiveId(zkfs.archive.config.archiveId),
								exc);
					}
				}
			} catch(Exception exc) {
				logger.error("FS {}: FSMirror mirror thread caught exception",
						Util.formatArchiveId(zkfs.archive.config.archiveId),
						exc);
			} finally {
				key.reset();
			}
		} catch (InterruptedException e) {
		}
		
		return true;
	}
	
	public synchronized void checkDirectories(HashMap<WatchKey, Path> pathsByKey) throws IOException {
		/* We're not guaranteed to get a delete callback if a directory is unlinked.
		 * So periodically we have to check if all our directories are still there.
		 */
		LinkedList<String> suspected = new LinkedList<>();
		for(Path path : pathsByKey.values()) {
			Path basePath = ((LocalFS) target).getRoot().toNativePath();
			String realPath = basePath.relativize(path).toString();
			if(!target.exists(realPath.toString())) {
				if(realPath.length() > 0) {
					suspected.add(realPath.toString());
				}
			}
		}
		
		for(String path : suspected) {
			suspectedTargetPathChange(path);
		}
	}

	public synchronized void observedTargetPathChange(String path) throws IOException {
		logger.info("FS {}: FSMirror observed target change: {}",
				Util.formatArchiveId(zkfs.archive.config.archiveId),
				path);
		
		try {
			copy(target, zkfs, path);
		} catch(Exception exc) {
			logger.warn(String.format("FSMirror %s: %s Caught exception processing path: %s (%d bytes)",
					zkfs.getArchive().getMaster().getName(),
					Util.formatRevisionTag(zkfs.baseRevision),
					path,
					path.length()));
			throw exc;
		}
	}
	
	protected void observedArchivePathChange(String path, Stat stat) throws IOException {
		if(watchFlag.isFalse()) return;
		if(isSquelched(path, target)) return;
		
		try {
			if(archiveToTargetSyncThread != null && !archiveToTargetSyncThread.isCancelled()) {
				queuedArchivePaths.put(path, true);
				archiveToTargetSyncThread.snooze();
			}
		} catch(NullPointerException exc) {
			// we can't synchronize this method for fear of deadlocks with ZKFS operations
			// but someone could stop the watch while we're running, which will null out archiveToTargetSyncThread
			// just let it go...
		}
		
		logger.info("FS {}: FSMirror {} observed archive change: {}",
				Util.formatArchiveId(zkfs.archive.config.archiveId),
				System.identityHashCode(this),
				path,
				new Throwable());
	}
	
	protected synchronized void syncQueuedArchivePaths() {
		for(String path : queuedArchivePaths.keySet()) {
			try {
				copy(zkfs, target, path);
			} catch(Exception exc) {
				logger.error("Caught exception processing path: %s", path, exc);
			}
		}
	}

	protected synchronized void suspectedTargetPathChange(String path) throws IOException {
		if(!isChanged(null, path)) return;
		observedTargetPathChange(path);
	}
	
	protected boolean isChanged(ZKFS oldFs, String path) throws IOException {
		boolean changed = false;
		if(path.equals("") || path.equals("/")) return false;
		Inode inode = getInode(zkfs, path), oldInode = null;
		Stat tstat = getLstat(target, path), zstat = inode == null ? null : inode.getStat();
		if(oldFs != null) {
			oldInode = getInode(oldFs, path);
		}
		
		if(tstat == null && zstat == null) return false;
		if(tstat == null || zstat == null) {
			logger.trace("FS {}: FSMirror detects difference at {} due to existence (zkfs {}, target {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					zstat != null,
					tstat != null);
			changed = true;
		} else if(oldInode != null && !inode.getRefTag().equals(oldInode.getRefTag())) {
			logger.trace("FS {}: FSMirror detects difference at {} since reftag has changed (new {}, old {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					Util.formatRefTag(inode.getRefTag()),
					Util.formatRefTag(oldInode.getRefTag()));
			changed = true;
		} else if(tstat.getType() != zstat.getType()) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing type (zkfs {}, target {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					zstat.getType(),
					tstat.getType());
			changed = true;
		} else if(zstat.getType() == Stat.TYPE_REGULAR_FILE && tstat.getSize() != zstat.getSize()) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing size (zkfs {}, target {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					zstat.getSize(),
					tstat.getSize());
			changed = true;
		} else if(zstat.getType() == Stat.TYPE_REGULAR_FILE && tstat.getMtime() != zstat.getMtime()) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing mtime (zkfs {}, target {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					zstat.getMtime(),
					tstat.getMtime());
			changed = true;
		} else if(tstat.getMode() != zstat.getMode() && zstat.getType() != Stat.TYPE_SYMLINK) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing mode (zkfs 0{}, target 0{})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					Integer.toOctalString(zstat.getMode()),
					Integer.toOctalString(tstat.getMode()));
			changed = true;
		} else if(zstat.getType() == Stat.TYPE_SYMLINK && !target.readlink(path).equals(zkfs.readlink(path))) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing target (zkfs {}, target {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					zkfs.readlink(path),
					target.readlink(path));
			changed = true;
		}
		
		return changed; // nothing changed
	}

	public void syncArchiveToTarget() throws IOException {
		logger.info("FSMirror {} {}: Mirroring ZKFS to mirror target",
				Util.formatArchiveId(zkfs.archive.config.archiveId),
				Util.formatRevisionTag(zkfs.baseRevision));
		
		boolean wasWatching = isWatching();
		if(wasWatching) {
			stopWatch();
		}

		synchronized(this) {
			ZKFS oldFs = lastRev != null ? lastRev.readOnlyFS() : null;
			try {
				try(ZKDirectory dir = zkfs.opendir("/")) {
					dir.walk(Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS, (path, stat, isBrokenSymlink, parent)->{
						if(!isChanged(oldFs, path)) return;
						syncPathArchiveToTarget(oldFs, path);
					});
		
					pruneFs(target, zkfs);
					
					if(wasWatching && !isWatching()) {
						startWatch();
					}
				}
				
				lastRev = zkfs.baseRevision;
			} finally {
				if(oldFs != null) {
					oldFs.close();
				}
			}
		} 
	}

	public synchronized void syncTargetToArchive() throws IOException {
		Directory dir = null;
		try {
			target.opendir("/").walk(Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS, (path, stat, brokenSymlink, parent)->{
				copy(target, zkfs, path);
			});

			pruneFs(zkfs, target);
		} finally {
			if(dir != null) dir.close();
		}
	}

	protected void pruneFs(FS pruned, FS reference) throws IOException {
		LinkedList<String> toPrune = new LinkedList<>();
		
		try(Directory dir = pruned.opendir("/")) {
			dir.walk(Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS, (path, stat, isBrokenSymlink, parent)->{
				if(reference.exists(path, false)) return;
				toPrune.add(path);
			});
		}
		
		for(String path : toPrune) {
			try {
				remove(pruned, path, pruned.lstat(path));
			} catch(ENOENTException exc) {}
		}
	}

	protected void syncPathArchiveToTarget(ZKFS oldFs, String path) throws IOException {
		Inode incoming = getInode(zkfs, path);

		// TODO Someday: Consider a way to preserve local changes.

		if(incoming == null) {
			// null inode means the path is deleted, so unlink it locally 
			try {
				target.unlink(path);
			} catch(ENOENTException exc) {} // enoent => file wasn't on target FS either, no big deal
			
			return;
		}
		
		try {
			copy(zkfs, target, path);
		} catch(IOException exc) {
			logger.warn("FS {}: FSMirror caught exception copying path to target: {}",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					exc);
		} catch(UnsupportedOperationException exc) {
			logger.info("FS {}: FSMirror skipping path due to lack of local support (maybe we need superuser?): {}",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					exc);
		}
	}
	
	protected synchronized boolean acquireSquelch(String path, FS srcFs, FS destFs) {
		String abspath = destFs.root().join(path).normalize().toNative();
		if(isSquelched(abspath, destFs)) {
			logger.debug("FS {}: FSMirror {} path is squelched: {}",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					System.identityHashCode(this),
					path);
			return false;
		}
		
		logger.debug("FS {}: FSMirror {} squelching path: {} {}",
				Util.formatArchiveId(zkfs.archive.config.archiveId),
				System.identityHashCode(this),
				path,
				srcFs);

		SquelchedPath squelch = new SquelchedPath(abspath, srcFs);
		squelchedPaths.put(abspath, squelch);
		return true;
	}
	
	protected boolean isSquelched(String path, FS fs) {
		SquelchedPath squelch = squelchedPaths.get(path);
		if(squelch == null) {
			return false;
		}
		
		if(squelch.isExpired()) {
			pruneSquelches();
			return false;
		}
		
		if(!squelch.matches(path, fs)) {
			return false;
		}
		
		return true;
	}
	
	protected void pruneSquelches() {
		LinkedList<String> toRemove = new LinkedList<>();
		squelchedPaths.forEach((path, squelch)->{
			if(squelch.isExpired()) {
				logger.debug("FS {}: FSMirror {} canceling expired squelch for path: {} {}",
						Util.formatArchiveId(zkfs.archive.config.archiveId),
						System.identityHashCode(this),
						path,
						squelch.fs);
				toRemove.add(path);
			}
		});
		
		for(String path : toRemove) {
			squelchedPaths.remove(path);
		}
	}

	protected void copy(FS src, FS dest, String path) throws IOException {
		Stat          srcStat = null,
			         destStat = null;
		boolean keepSquelched = false;
		
		copyParentDirectories(src, dest, path);
		if(!canMirror(path)) return;
		
		try {
			try {
				destStat = dest.lstat(path);
			} catch(ENOENTException exc) {}
			
			srcStat = src.lstat(path);
			if(!acquireSquelch(path, src, dest)) {
				keepSquelched = true;
				return;
			}
			
			if(srcStat.isRegularFile()) {
				copyFile     (src, dest, path, srcStat, destStat);
			} else if(srcStat.isFifo()) {
				copyFifo     (src, dest, path, srcStat, destStat);
			} else if(srcStat.isDevice()) {
				copyDevice   (src, dest, path, srcStat, destStat);
			} else if(srcStat.isDirectory()) {
				copyDirectory(src, dest, path, srcStat, destStat);
			} else if(srcStat.isSymlink()) {
				copySymlink  (src, dest, path, srcStat, destStat);
			}

			applyStat(srcStat, dest, path);
			keepSquelched = true;
		} catch(ENOENTException exc) {
			try {
				dest.unlink(path);
			} catch(EISDIRException exc2) {
				dest.rmrf(path);
			} catch(ENOENTException exc2) {}
		} finally {
			if(!keepSquelched) {
				// if we fail to process a file, we cancel the squelch so that it can be re-handled
				squelchedPaths.remove(path);
			}
		}

		return;
	}

	protected void copyParentDirectories(FS src, FS dest, String path) throws IOException {
		if(path.equals("/")) return;
		
		tracelog(src, dest, path, "copy parent directories");
		String dir = src.dirname(path);
		if(!dest.exists(dir)) {
			copy(src, dest, dir);
		}
	}

	protected void copyFile(FS src, FS dest, String path, Stat srcStat, Stat destStat) throws IOException {
		tracelog(src, dest, path, "copy regular file");
		File srcFile = null, destFile = null;
		try {
			srcFile = src.open(path, File.O_RDONLY);

			if(destStat != null && !destStat.isRegularFile()) {
				remove(dest, path, destStat);
			}

			destFile = dest.open(path, File.O_WRONLY|File.O_CREAT|File.O_TRUNC);
			while(srcFile.hasData()) {
				byte[] chunk = srcFile.read(65536);
				logger.trace("FS {}: FSMirror write {}, offset {} of file size {}, chunk len {}",
						Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
						path,
						srcFile.seek(0, File.SEEK_CUR),
						srcFile.getSize(),
						chunk.length);
				destFile.write(chunk);
			}
			
			destFile.close();
			destFile = null;
		} catch(FileNotFoundException exc) {
			logger.warn("FS {}: FSMirror unable to copy file at " + path, exc);
		} finally {
			ensureClosed(srcFile);
			ensureClosed(destFile);
		}
	}

	protected void copyFifo(FS src, FS dest, String path, Stat srcStat, Stat destStat) throws IOException {
		if(destStat != null && destStat.isFifo()) return;
		tracelog(src, dest, path, "copy regular fifo");
		remove(dest, path, destStat);
		dest.mkfifo(path);
	}

	protected void copyDevice(FS src, FS dest, String path, Stat srcStat, Stat destStat) throws IOException {
		if(destStat != null && destStat.getType() == srcStat.getType()
				&& destStat.getDevMajor() == srcStat.getDevMajor()
				&& destStat.getDevMinor() == srcStat.getDevMinor())
		{
			return;
		}

		tracelog(src, dest, path, "copy device");
		remove(dest, path, destStat);
		dest.mknod(path, srcStat.getType(), srcStat.getDevMajor(), srcStat.getDevMinor());
	}

	protected void copyDirectory(FS src, FS dest, String path, Stat srcStat, Stat destStat) throws IOException {
		if(destStat != null && destStat.isDirectory()) {
			return;
		}

		tracelog(src, dest, path, "copy directory");
		remove(dest, path, destStat);
		dest.mkdir(path);
	}

	protected void copySymlink(FS src, FS dest, String path, Stat srcStat, Stat destStat) throws IOException {
		String link = src.readlink(path);
		if(destStat != null && destStat.isSymlink()
				&& link.equals(dest.readlink(path))) {
			return;
		}

		tracelog(src, dest, path, "copy symlink, link target is {}", link);
		remove(dest, path, destStat);
		String target = src.readlink(path);
		dest.symlink_unsafe(target, path);
	}

	protected Stat getLstat(FS fs, String path) {
		try {
			return fs.lstat(path);
		} catch(IOException exc) {
			return null;
		}
	}
	
	protected Inode getInode(ZKFS fs, String path) {
		try {
			return fs.inodeForPath(path, false);
		} catch(IOException exc) {
			return null;
		}
	}

	protected void applyStat(Stat stat, FS dest, String path) throws IOException {
		// Java's filesystem API does not behave as you'd expect with symlinks, so just leave their metadata alone.
		if(stat.isSymlink()) return;
		attempt(() -> dest.chmod(path, stat.getMode()));

		// we may not actually want this, even if permissions available
		// also we're doing string names second since we want the username ('steve') to override a uid (100)
		attempt(() -> dest.chown(path, stat.getUid()));
		attempt(() -> dest.chown(path, stat.getUser()));
		attempt(() -> dest.chgrp(path, stat.getGid()));
		attempt(() -> dest.chgrp(path, stat.getGroup()));

		attempt(() -> dest.setAtime(path, stat.getAtime()));
		attempt(() -> dest.setCtime(path, stat.getCtime()));
		attempt(() -> dest.setMtime(path, stat.getMtime()));
	}

	protected void ensureClosed(File file) {
		if(file == null) return;
		try {
			file.close();
		} catch(IOException exc) {
			logger.error("FS {}: FSMirror caught exception closing file {}",
					Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
					file.getPath());
		}
	}

	protected void remove(FS fs, String path, Stat stat) throws IOException {
		if(stat == null) return;
		if(stat.isDirectory()) {
			fs.rmrf(path);
			return;
		}

		fs.unlink(path);
	}

	protected void attempt(SyncOperation op) {
		try {
			op.op();
		} catch(IOException|UnsupportedOperationException exc) {
			logger.debug("FS {}: FSMirror could not complete operation due to {}",
					Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
					exc.getClass(),
					exc);
		}
	}
	
	protected void tracelog(FS src, FS dest, String path, String msg) {
		tracelog(src, dest, path, msg, null);
	}
	
	protected void tracelog(FS src, FS dest, String path, String msg, Object arg) {
		String direction = src == zkfs ? "zkfs -> target" : "target -> zkfs";
		String prefix = "FS " + Util.formatArchiveId(zkfs.archive.config.archiveId) +
				": FSMirror sync " +
				path +
				" " +
				direction + " ";
		logger.trace(prefix + msg, arg);
	}

	public void setTargetFs(FS fs) {
		assert(!isWatching());
		this.target = fs;
	}

	public void setZkfs(ZKFS zkfs) {
		assert(!isWatching());
		this.zkfs = zkfs;		
	}
}
