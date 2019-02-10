package com.acrescrypto.zksync.fs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.EISDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;

public abstract class FS implements AutoCloseable {
	protected static HashMap<File,Throwable> globalFileBacktraces = new HashMap<>();
	
	public synchronized static void addOpenFileHandle(File file, Throwable backtrace) {
		globalFileBacktraces.put(file, backtrace);
	}
	
	public synchronized static void removeOpenFileHandle(File file) {
		globalFileBacktraces.remove(file);
	}
	
	public static HashMap<File,Throwable> getGlobalOpenFiles() {
		return globalFileBacktraces;
	}
	
	public static boolean fileHandleTelemetryEnabled = false;
	
	public abstract Stat stat(String path) throws IOException;
	public abstract Stat lstat(String path) throws IOException;
	
	public abstract Directory opendir(String path) throws IOException;
	public abstract void mkdir(String path) throws IOException;
	public abstract void mkdirp(String path) throws IOException;
	public abstract void rmdir(String path) throws IOException;
	
	public abstract void unlink(String path) throws IOException;
	public abstract void link(String target, String link) throws IOException;
	public abstract void symlink(String target, String link) throws IOException;
	public abstract void symlink_unsafe(String source, String dest) throws IOException; // allow symlinks to exit fs scope
	public abstract String readlink(String link) throws IOException;
	public abstract String readlink_unsafe(String link) throws IOException;
	public abstract void mknod(String path, int type, int major, int minor) throws IOException;
	public abstract void mkfifo(String path) throws IOException;
	
	public abstract void chmod(String path, int mode) throws IOException;
	public abstract void chown(String path, int uid) throws IOException;
	public abstract void chown(String path, String user) throws IOException;
	public abstract void chgrp(String path, int gid) throws IOException;
	public abstract void chgrp(String path, String group) throws IOException;
		
	public abstract void setMtime(String path, long mtime) throws IOException;
	public abstract void setCtime(String path, long ctime) throws IOException;
	public abstract void setAtime(String path, long atime) throws IOException;
	
	public abstract void write(String path, byte[] contents, int offset, int length) throws IOException;
	public abstract File open(String path, int mode) throws IOException;
	public abstract void truncate(String path, long size) throws IOException;
	
	private Logger logger = LoggerFactory.getLogger(FS.class);
	protected HashMap<File,Throwable> localFileBacktraces = new HashMap<>();
	
	public long maxFileSize() {
		return Long.MAX_VALUE;
	}
	
	public void write(String path, byte[] contents) throws IOException {
		write(path, contents, 0, contents.length);
	}
	
	public byte[] read(String path) throws IOException {
		try(File file = open(path, File.O_RDONLY)) {
			byte[] bytes = file.read();
			return bytes;
		}
	}
	
	public void rmrf(String path) throws IOException {
		Directory dir = opendir(path);
		try {
			for(String entry : dir.list()) {
				String subpath = Paths.get(path, entry).toString();
				Stat lstat = lstat(subpath);
				
				if(lstat.isDirectory()) {
					rmrf(subpath);
				} else {
					unlink(subpath);
				}
			}
		} catch(Exception exc) {
			logger.error("Caught exception on rmrf(\"{}\"), exists={}: ", path, exists(path), exc);
		} finally {
			dir.close();
			rmdir(path);
		}
	}
	
	public void mv(String oldPath, String newPath) throws IOException {
		try {
			Stat stat;
			stat = stat(newPath);
			if(stat.isDirectory()) {
				newPath = Paths.get(newPath, basename(oldPath)).toString();
				stat = stat(newPath);
				if(stat.isDirectory()) {
					throw new EISDIRException(newPath);
				}
			} else {
				unlink(newPath);
			}
		} catch(ENOENTException exc) {}
		
		link(oldPath, newPath);
		unlink(oldPath);
	}
	
	public void cp(String oldPath, String newPath) throws IOException {
		try {
			Stat stat = stat(newPath);
			if(stat.isDirectory()) {
				newPath = Paths.get(newPath, basename(oldPath)).toString();
				stat = stat(newPath);
				if(stat.isDirectory()) {
					throw new EISDIRException(newPath);
				}
			}
		} catch(ENOENTException exc) {}
		
		File in = open(oldPath, File.O_RDONLY);
		File out = open(newPath, File.O_WRONLY|File.O_CREAT|File.O_TRUNC);
		
		byte[] buf = new byte[(int) Math.min(64*1024, in.getStat().getSize())];
		while(in.hasData()) {
			int readLen = in.read(buf, 0, buf.length);
			out.write(buf, 0, readLen);
		}
		
		in.close();
		out.close();
	}

	public String dirname(String path) {
		if(path.equals("/")) return path;
		String[] comps = path.split("/");
		String parent = String.join("/", Arrays.copyOfRange(comps, 0, comps.length-1));
		if(comps[0] == "") parent = "/" + parent;
		if(parent.equals("")) parent = "/";
		return parent;
	}
	
	public String basename(String path) {
		if(path.equals("/")) return "/";
		if(path.equals("")) return "";
		String[] comps = path.split("/");
		return comps[comps.length-1];
	}

	public String absolutePath(String path) {
		return absolutePath(path, "/");
	}
	
	public String absolutePath(String path, String root) {
		String fullPath = Paths.get(root, path).toString();
		ArrayList<String> comps = new ArrayList<String>();
		for(String comp : fullPath.split("/")) {
			if(comp.equals(".")) continue;
			else if(comp.equals("..")) {
				if(comps.size() > 0) comps.remove(comps.size()-1);
			} else {
				comps.add(comp);
			}
		}
		
		if(comps.size() == 0 || comps.size() == 1 && comps.get(0).equals("")) return "/";
		return String.join("/", comps);
	}
	
	public boolean exists(String path, boolean followLinks) {
		try {
			if(followLinks) stat(path);
			else lstat(path);
			return true;
		} catch(IOException e) {
			return false;
		}
	}
	
	public boolean exists(String path) {
		return exists(path, true);
	}
	
	public void squash(String path) throws IOException {
		try { setCtime(path, 0); } catch(UnsupportedOperationException e) {}
		try { setMtime(path, 0); } catch(UnsupportedOperationException e) {}
		try { setAtime(path, 0); } catch(UnsupportedOperationException e) {}
	}
	
	public void applyStat(String path, Stat stat) throws IOException {
		try { chown(path, stat.getUser()); } catch(UnsupportedOperationException exc) {}
		try { chgrp(path, stat.getGroup()); } catch(UnsupportedOperationException exc) {}
		try { chmod(path, stat.getMode()); } catch(UnsupportedOperationException exc) {}
		try { setCtime(path, stat.getCtime()); } catch(UnsupportedOperationException exc) {}
		try { setMtime(path, stat.getMtime()); } catch(UnsupportedOperationException exc) {}
		try { setAtime(path, stat.getAtime()); } catch(UnsupportedOperationException exc) {}
	}
	
	public void safeWrite(String path, byte[] contents) throws IOException {
		String safety = path + ".safety";
		write(safety, contents);
		try {
			if(exists(path)) unlink(path);
			link(safety, path);
		} finally {
			try {
				unlink(safety);
			} catch(IOException exc) {
				// permissions or directory contents may have changed underneath us
			}
		}
		squash(path);
	}
	
	public byte[] safeRead(String path) throws IOException {
		String safety = path + ".safety";
		if(exists(safety) && stat(path).getMtime() > 0) return read(safety);
		return read(path);
	}
	
	/** Alert FS that we may need to read this file soon; useful if FS implements some form of caching. */
	public void expect(String path) {
	}
	
	/** Return an instance of this FS class whose root is based in the subpath provided. Sort of like a chroot,
	 * except this FS remains unmodified and the chrooted FS is a new one that is returned. 
	 * @throws IOException */
	public abstract FS scopedFS(String path) throws IOException;
	
	/** Return an instance of this FS class with the scope removed. */
	public abstract FS unscopedFS() throws IOException;
	
	/** Close any resources associated with keeping this FS access open, e.g. sockets. The FS object may not be reused. */
	public void close() throws IOException {}
	
	/** Remove all content from filesystem */
	public void purge() throws IOException {
		if(exists("/")) {
			rmrf("/");
			
			if(!exists("/")) {
				mkdir("/");
			}
		}
	}
	
	public long storageSize(String path) throws IOException {
		// TODO API: (test) FS.storageSize
		long totalSize = 0;
		
		Stat s = stat(path);
		if(s.isDirectory()) {
			for(String item : opendir(path).listRecursive(0)) {
				totalSize += stat(item).size;
			}
		} else if(s.isRegularFile() || s.isSymlink()) {
			totalSize = s.size;
		}
		
		return totalSize;
	}

	public synchronized void reportOpenFile(File file) {
		if(!fileHandleTelemetryEnabled) return;
		
		Throwable backtrace = new Throwable();
		addOpenFileHandle(file, backtrace);
		localFileBacktraces.put(file, backtrace);
	}
	
	public synchronized void reportClosedFile(File file) {
		if(!fileHandleTelemetryEnabled) return;
		
		removeOpenFileHandle(file);
		localFileBacktraces.remove(file);
	}
	
	public synchronized HashMap<File,Throwable> getOpenFiles() {
		return localFileBacktraces;
	}
}
