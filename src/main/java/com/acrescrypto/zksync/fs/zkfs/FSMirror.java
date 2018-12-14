package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.HashSet;

import com.acrescrypto.zksync.exceptions.EISDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class FSMirror {
	ZKFS zkfs;
	FS target;
	RevisionTag lastRev;
	
	public interface SyncOperation {
		void op() throws IOException;
	}
	
	public FSMirror(ZKFS zkfs, FS target) {
		this.zkfs = zkfs;
		this.target = target;
		this.lastRev = zkfs.baseRevision;
	}
	
	public void observedTargetPathChange(String path) throws IOException {
		copy(target, zkfs, path);
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
			 copy(zkfs, target, path);
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
			
			archiveFile = dest.open(path, File.O_WRONLY|File.O_CREAT);
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
