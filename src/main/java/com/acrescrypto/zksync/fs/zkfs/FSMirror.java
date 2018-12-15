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

import static java.nio.file.StandardWatchEventKinds.*;

public class FSMirror {
	ZKFS zkfs;
	FS target;
	RevisionTag lastRev;
	
	Logger logger = LoggerFactory.getLogger(FSMirror.class);
	MutableBoolean watchFlag = new MutableBoolean();
	
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
		watchDirectory(dir, watcher, pathsByKey);
		
		new Thread( () -> watchThread(flag, watcher, pathsByKey) ).start();
	}
	
	public void stopWatch() {
		watchFlag.setFalse();
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
			try {
				WatchKey key = watcher.poll(10, TimeUnit.MILLISECONDS);
				if(key == null || flag.isFalse()) {
					continue;
				}
				try {
					for(WatchEvent<?> event : key.pollEvents()) {
						WatchEvent.Kind<?> kind = event.kind();
						if(kind == OVERFLOW) {
							logger.warn("Caught overflow on FS mirror; some local filesystem changes may not have made it into the archive.");
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
							logger.error("Caught exception mirroring local FS", exc);
						}
					}
				} catch(Exception exc) {
					logger.error("Mirror thread caught exception", exc);
				} finally {
					if(!key.reset()) return;
				}
			} catch (InterruptedException e) {
			}
		}
	}
	
	public synchronized void observedTargetPathChange(String path) throws IOException {
		copy(target, zkfs, path);
	}
	
	protected synchronized void suspectedTargetPathChange(String path) throws IOException {
		if(path.equals("") || path.equals("/")) return;
		Stat tstat = getLstat(target, path), zstat = getLstat(zkfs, path);
		if(tstat == null && zstat == null) return;
		if(tstat == null || zstat == null) {
		} else if(tstat.getType() != zstat.getType()) {
		} else if(tstat.getSize() != zstat.getSize()) {
		} else if(tstat.getMtime() != zstat.getMtime()) {
		} else if(tstat.getMode() != zstat.getMode()) {
		} else {
			return; // nothing was different
		}
		
		observedTargetPathChange(path);
	}
	
	public synchronized void syncArchiveToTarget() throws IOException {
		ZKFS oldFs = lastRev != null ? lastRev.getFS() : null;
		
		try {
			String[] list = zkfs.opendir("/").listRecursive();
			for(String path : list) {
				syncPathArchiveToTarget(oldFs, path);
			}
			
			pruneFsToList(target, list);
		} finally {
			oldFs.close();
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
			remove(fs, path, fs.lstat(path));
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
				 logger.warn("Caught exception copying path to target: " + path, exc);
			 } catch(UnsupportedOperationException exc) {
				 logger.info("Skipping path due to lack of local support (maybe we need superuser?): " + path, exc);
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
		String dir = src.dirname(path);
		if(!dest.exists(dir)) {
			copy(src, dest, dir);
		}
	}
	
	protected void copyFile(FS src, FS dest, String path, Stat srcStat, Stat destStat) throws IOException {
		File targetFile = null, archiveFile = null;
		try {
			targetFile = src.open(path, File.O_RDONLY);
			
			if(destStat != null && !destStat.isRegularFile()) {
				remove(dest, path, destStat);
			}
			
			archiveFile = dest.open(path, File.O_WRONLY|File.O_CREAT|File.O_TRUNC);
			while(targetFile.hasData()) {
				byte[] chunk = targetFile.read(65536);
				archiveFile.write(chunk);
			}
			
			archiveFile.close();
			archiveFile = null;
		} finally {
			ensureClosed(targetFile);
			ensureClosed(archiveFile);
		}
	}
	
	protected void copyFifo(FS src, FS dest, String path, Stat srcStat, Stat destStat) throws IOException {
		if(destStat != null && destStat.isFifo()) return;
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
		
		remove(dest, path, destStat);
		dest.mknod(path, srcStat.getType(), srcStat.getDevMajor(), srcStat.getDevMinor());
	}
	
	protected void copyDirectory(FS src, FS dest, String path, Stat srcStat, Stat destStat) throws IOException {
		if(destStat != null && destStat.isDirectory()) {
			return;
		}
		
		remove(dest, path, destStat);
		dest.mkdir(path);
	}
	
	protected void copySymlink(FS src, FS dest, String path, Stat srcStat, Stat destStat) throws IOException {
		if(destStat != null && destStat.isSymlink()
				&& src.readlink(path).equals(dest.readlink(path))) {
			return;
		}
		
		remove(dest, path, destStat);
		String target = src.readlink(path);
		dest.symlink(target, path);
	}
	
	protected Stat getLstat(FS fs, String path) {
		try {
			return fs.lstat(path);
		} catch(IOException exc) {
			return null;
		}
	}
	
	protected void applyStat(Stat stat, FS dest, String path) throws IOException {
		if(stat.isSymlink()) return;
		dest.chmod(path, stat.getMode());
		
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
		} catch(IOException exc) {}
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
		} catch(IOException|UnsupportedOperationException exc) {}
	}
}
