package com.acrescrypto.zksync.fs.zkfs;

import java.io.FileNotFoundException;
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
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.CommandFailedException;
import com.acrescrypto.zksync.exceptions.EISDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Directory;
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
	
	public FS getTarget() {
		return target;
	}
	
	public ZKFS getZKFS() {
		return zkfs;
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
		
		try {
			logger.info("FS {}: FSMirror starting watch of {}, {} active",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					localTarget.getRoot(),
					numActive());
			watchDirectory(dir, watcher, pathsByKey);
	
			watchThread = new Thread( () -> watchThread(flag, watcher, pathsByKey) );
			watchThread.start();
		} catch(Throwable exc) {
			decrementActive();
			throw exc;
		}
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
		try {
			while(flag.booleanValue()) {
				if(!watchThreadBody(flag, watcher, pathsByKey)) {
					break;
				}
			}
		} catch(Throwable exc) {
			logger.error("FS {}: Watch thread caught exception", exc);
			exc.printStackTrace();
			throw exc;
		} finally {
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
					Path basePath = Paths.get(((LocalFS) target).getRoot());
					String realPath = basePath.relativize(fullPath).toString();

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
			Path basePath = Paths.get(((LocalFS) target).getRoot());
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
		logger.info("FS {}: FSMirror observed change: {}",
				Util.formatArchiveId(zkfs.archive.config.archiveId),
				path);
		copy(target, zkfs, path);
	}

	protected synchronized void suspectedTargetPathChange(String path) throws IOException {
		if(!isChanged(path)) return;
		observedTargetPathChange(path);
	}
	
	protected boolean isChanged(String path) throws IOException {
		if(path.equals("") || path.equals("/")) return false;
		Stat tstat = getLstat(target, path), zstat = getLstat(zkfs, path);
		if(tstat == null && zstat == null) return false;
		if(tstat == null || zstat == null) {
			logger.trace("FS {}: FSMirror detects difference at {} due to existence (zkfs {}, target {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					zstat != null,
					tstat != null);
			return true;
		} else if(tstat.getType() != zstat.getType()) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing type (zkfs {}, target {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					zstat.getType(),
					tstat.getType());
			return true;
		} else if(zstat.getType() == Stat.TYPE_REGULAR_FILE && tstat.getSize() != zstat.getSize()) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing size (zkfs {}, target {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					zstat.getSize(),
					tstat.getSize());
			return true;
		} else if(zstat.getType() == Stat.TYPE_REGULAR_FILE && tstat.getMtime() != zstat.getMtime()) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing mtime (zkfs {}, target {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					zstat.getMtime(),
					tstat.getMtime());
			return true;
		} else if(tstat.getMode() != zstat.getMode() && zstat.getType() != Stat.TYPE_SYMLINK) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing mode (zkfs 0{}, target 0{})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					Integer.toOctalString(zstat.getMode()),
					Integer.toOctalString(tstat.getMode()));
			return true;
		} else if(zstat.getType() == Stat.TYPE_SYMLINK && !target.readlink(path).equals(zkfs.readlink(path))) {
			logger.trace("FS {}: FSMirror detects difference at {} due to differing target (zkfs {}, target {})",
					Util.formatArchiveId(zkfs.archive.config.archiveId),
					path,
					zkfs.readlink(path),
					target.readlink(path));
			return true;
		}
		
		return false; // nothing changed
	}

	public void syncArchiveToTarget() throws IOException {
		ZKFS oldFs = lastRev != null ? lastRev.readOnlyFS() : null;
		try {
			boolean wasWatching = isWatching();
			if(wasWatching) {
				stopWatch();
			}
	
			synchronized(this) {
				try(ZKDirectory dir = zkfs.opendir("/")) {
					dir.walk(Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS, (path, stat, isBrokenSymlink)->{
						if(!isChanged(path)) return;
						syncPathArchiveToTarget(oldFs, path);
					});
		
					pruneFs(target, zkfs);
					
					if(wasWatching && !isWatching()) {
						startWatch();
					}
				}
			}
		} finally {
			if(oldFs != null) {
				oldFs.close();
			}
		}
	}

	public synchronized void syncTargetToArchive() throws IOException {
		Directory dir = null;
		try {
			target.opendir("/").walk(Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS, (path, stat, brokenSymlink)->{
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
			dir.walk(Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS, (path, stat, isBrokenSymlink)->{
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
		RevisionTag tag;
		if(src instanceof ZKFS) {
			tag = ((ZKFS) src).baseRevision;
		} else {
			tag = ((ZKFS) dest).baseRevision;
		}

		copyParentDirectories(src, dest, path);
		try {
			srcStat = src.lstat(path);
			try {
				destStat = dest.lstat(path);
			} catch(ENOENTException exc) {}
			
			Util.debugLog("FSMirror " + zkfs.getArchive().getMaster().getName() + ": precopy " + Util.formatRevisionTag(tag) + " " + src.getClass().getSimpleName() + " -> " + dest.getClass().getSimpleName() + " " + path + " size " + srcStat.getSize());
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
			Stat zkstat = zkfs.lstat(path);
			Stat tstat = target.lstat(path);
			StringBuilder sb = new StringBuilder();
			sb.append("FSMirror " + zkfs.getArchive().getMaster().getName() + ": postcopy " + Util.formatRevisionTag(tag) + " " + src.getClass().getSimpleName() + " -> " + dest.getClass().getSimpleName() + " " + path + "\n");
			sb.append(String.format("\tLocalFS stat: size %d, mtime %d, type %02x\n",
					tstat.getSize(),
					tstat.getMtime(),
					tstat.getType()));
			sb.append(String.format("\t   ZKFS stat: size %d, mtime %d, type %02x, inodeId %d, nlink %d, identity %16x\n",
					zkstat.getSize(),
					zkstat.getMtime(),
					zkstat.getType(),
					zkstat.getInodeId(),
					zkfs.inodeForPath(path, false).nlink,
					zkfs.inodeForPath(path, false).identity));
			Util.debugLog(sb.toString());
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
						srcFile.getStat().getSize(),
						chunk.length);
				if(chunk.length == 0) {
					ZKFile zk = (ZKFile) srcFile;
					logger.error("FS {}: FSMirror DANGER ZONE -- {} reftag {}/{}, {} chunks, {} pages, revtag {}\n{}",
							Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
							path,
							Util.formatRefTag(zk.getInode().getRefTag()),
							Util.hexdumpStr("hextag", zk.getInode().getRefTag().getBytes()),
							zk.tree.numChunks,
							zk.tree.numPages,
							Util.formatRevisionTag(zk.zkfs.baseRevision),
							zk.inode.dump());
					InodeTable table = zk.getFS().getInodeTable();
					for(int i = 0; i < zk.getFS().getInodeTable().nextInodeId(); i++) {
						logger.info("FS {}: FSMirror Inode Dump {}\n{}",
								Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
								i,
								table.inodeWithId(i).dump());
					}
					throw new RuntimeException("FSMirror sync failed");
				}
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
					exc.getClass());
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
