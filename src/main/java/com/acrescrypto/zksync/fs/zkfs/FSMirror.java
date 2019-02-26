package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.CommandFailedException;
import com.acrescrypto.zksync.exceptions.EISDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.utility.Util;

import static java.nio.file.StandardWatchEventKinds.*;

public class FSMirror {
	static int numActive;
	ZKFS zkfs;
	FS target;
	RevisionTag lastRev;
	Thread watchThread;

	Logger logger = LoggerFactory.getLogger(FSMirror.class);
	MutableBoolean watchFlag = new MutableBoolean();
	
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
		Path dir = Paths.get(localTarget.getRoot());
		WatchService watcher = dir.getFileSystem().newWatchService();
		HashMap<WatchKey, Path> pathsByKey = new HashMap<>();
		incrementActive();
		logger.info("FS {}: FSMirror starting watch of {}, {} active",
				Util.formatArchiveId(zkfs.archive.config.archiveId),
				localTarget.getRoot(),
				numActive());
		watchDirectory(dir, watcher, pathsByKey);

		watchThread = new Thread( () -> watchThread(flag, watcher, pathsByKey) );
		watchThread.start();
	}

	public void stopWatch() {
		if(watchThread == null) return;
		Thread stoppedThread = watchThread;
		logger.info("FS {}: FSMirror stopping watch of {}, {} active",
				Util.formatArchiveId(zkfs.archive.config.archiveId),
				((LocalFS) target).getRoot(),
				numActive());
		watchFlag.setFalse();
		Util.blockOn(()->stoppedThread.isAlive());
		logger.info("FS {}: FSMirror stopped watch of {}, {} watches active",
				Util.formatArchiveId(zkfs.archive.config.archiveId),
				((LocalFS) target).getRoot(),
				numActive());
	}

	protected void watchDirectory(Path dir, WatchService watcher, HashMap<WatchKey, Path> pathsByKey) throws IOException {
		Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult preVisitDirectory(Path subdir, BasicFileAttributes attrs) throws IOException {
				WatchKey key = subdir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
				pathsByKey.put(key, subdir);

				Path basePath = Paths.get(((LocalFS) target).getRoot());
				String realPath = basePath.relativize(subdir).toString();
				suspectedTargetPathChange(realPath);

				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
				Path basePath = Paths.get(((LocalFS) target).getRoot());
				String realPath = basePath.relativize(file).toString();
				suspectedTargetPathChange(realPath);
				return FileVisitResult.CONTINUE;
			}
		});
	}

	protected void watchThread(MutableBoolean flag, WatchService watcher, HashMap<WatchKey, Path> pathsByKey) {
		while(flag.booleanValue()) {
			if(!watchThreadBody(flag, watcher, pathsByKey)) {
				break;
			}
		}

		try {
			watcher.close();
			decrementActive();
		} catch (IOException exc) {
			logger.error("FS {}: FSMirror unable to close watcher",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					exc);
		}
	}
	
	protected boolean watchThreadBody(MutableBoolean flag, WatchService watcher, HashMap<WatchKey, Path> pathsByKey) {
		try {
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
					Path basePath = Paths.get(((LocalFS) target).getRoot());
					String realPath = basePath.relativize(fullPath).toString();

					try {
						Stat stat = getLstat(target, realPath);

						if(kind == ENTRY_CREATE && stat != null && stat.isDirectory()) {
							watchDirectory(fullPath, watcher, pathsByKey);
						}

						if(kind != ENTRY_CREATE || stat == null || !stat.isRegularFile()) {
							// we don't want ENTRY_CREATE for files, because that also generates ENTRY_MODIFY
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
				if(!key.reset()) return false;
			}
		} catch (InterruptedException e) {
		}
		
		return true;
	}

	public synchronized void observedTargetPathChange(String path) throws IOException {
		logger.info("FS {}: FSMirror observed change: {}",
				Util.formatArchiveId(zkfs.archive.config.archiveId),
				path);
		copy(target, zkfs, path);
	}

	protected synchronized void suspectedTargetPathChange(String path) throws IOException {
		if(path.equals("") || path.equals("/")) return;
		Stat tstat = getLstat(target, path), zstat = getLstat(zkfs, path);
		if(tstat == null && zstat == null) return;
		if(tstat == null || zstat == null) {
		} else if(tstat.getType() != zstat.getType()) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing type (zkfs {}, target {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					zstat.getType(),
					tstat.getType());
		} else if(zstat.getType() == Stat.TYPE_REGULAR_FILE && tstat.getSize() != zstat.getSize()) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing size (zkfs {}, target {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					zstat.getSize(),
					tstat.getSize());
		} else if(zstat.getType() == Stat.TYPE_REGULAR_FILE && tstat.getMtime() != zstat.getMtime()) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing mtime (zkfs {}, target {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					zstat.getMtime(),
					tstat.getMtime());
		} else if(tstat.getMode() != zstat.getMode()) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing mode (zkfs 0{}, target 0{})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					Integer.toOctalString(zstat.getMode()),
					Integer.toOctalString(tstat.getMode()));
		} else {
			return; // nothing was different
		}

		observedTargetPathChange(path);
	}

	public synchronized void syncArchiveToTarget() throws IOException {
		ZKFS oldFs = lastRev != null ? lastRev.getFS() : null;
		boolean wasWatching = isWatching();
		if(wasWatching) {
			stopWatch();
		}

		try {
			String[] list = zkfs.opendir("/").listRecursive();
			for(String path : list) {
				syncPathArchiveToTarget(oldFs, path);
			}

			pruneFsToList(target, list);
		} finally {
			oldFs.close();
		}
		
		if(wasWatching) {
			startWatch();
		}
	}

	public synchronized void syncTargetToArchive() throws IOException {
		String[] list = target.opendir("/").listRecursive();

		for(String path : list) {
			copy(target, zkfs, path);
		}

		pruneFsToList(zkfs, list);
	}

	protected void pruneFsToList(FS fs, String[] list) throws IOException {
		HashSet<String> paths = new HashSet<>();
		for(String path : list) {
			paths.add(path);
		}

		String[] localList = fs.opendir("/").listRecursive();
		for(String path : localList) {
			if(paths.contains(path)) continue;
			try {
				remove(fs, path, fs.lstat(path));
			} catch(ENOENTException exc) {}
		}
	}

	protected void syncPathArchiveToTarget(ZKFS oldFs, String path) throws IOException {
		Inode existing = null;
		try {
			existing = oldFs != null ? oldFs.inodeForPath(path) : null;
		} catch(ENOENTException exc) {}

		Inode incoming = null;
		try {
			incoming = zkfs.inodeForPath(path, false);
		} catch(ENOENTException exc) {}

		// TODO Someday: Consider a way to preserve local changes.

		if(existing == null) {
			if(incoming == null) return; // should not actually be possible
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
		} else if(incoming == null) {
			try {
				target.unlink(path);
			} catch(ENOENTException exc) {}
		} else {
			if(existing.compareTo(incoming) == 0) return;
			copy(zkfs, target, path);
		}
	}

	protected Stat copy(FS src, FS dest, String path) throws IOException {
		Stat srcStat = null, destStat = null;
		copyParentDirectories(src, dest, path);
		try {
			srcStat = src.lstat(path);
			try {
				destStat = dest.lstat(path);
			} catch(ENOENTException exc) {}
			
			if(srcStat.isRegularFile()) {
				copyFile(src, dest, path, srcStat, destStat);
			} else if(srcStat.isFifo()) {
				copyFifo(src, dest, path, srcStat, destStat);
			} else if(srcStat.isDevice()) {
				copyDevice(src, dest, path, srcStat, destStat);
			} else if(srcStat.isDirectory()) {
				copyDirectory(src, dest, path, srcStat, destStat);
			} else if(srcStat.isSymlink()) {
				copySymlink(src, dest, path, srcStat, destStat);
			}

			applyStat(srcStat, dest, path);
		} catch(ENOENTException exc) {
			try {
				dest.unlink(path);
			} catch(EISDIRException exc2) {
				dest.rmrf(path);
			} catch(ENOENTException exc2) {}
		}

		return srcStat;
	}

	protected void copyParentDirectories(FS src, FS dest, String path) throws IOException {
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
				logger.trace("FS {}: FSMirror write {} chunk offset {} length {}",
						Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
						path,
						srcFile.seek(0, File.SEEK_CUR),
						srcFile.getStat().getSize());
				byte[] chunk = srcFile.read(65536);
				destFile.write(chunk);
			}

			destFile.close();
			destFile = null;
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
}
