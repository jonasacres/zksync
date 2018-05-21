package com.acrescrypto.zksync.fs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FS {
	public abstract Stat stat(String path) throws IOException;
	public abstract Stat lstat(String path) throws IOException;
	
	public abstract Directory opendir(String path) throws IOException;
	public abstract void mkdir(String path) throws IOException;
	public abstract void mkdirp(String path) throws IOException;
	public abstract void rmdir(String path) throws IOException;
	
	public abstract void unlink(String path) throws IOException;
	public abstract void link(String target, String link) throws IOException;
	public abstract void symlink(String target, String link) throws IOException;
	public abstract String readlink(String link) throws IOException;
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
	
	public long maxFileSize() {
		return Long.MAX_VALUE;
	}
	
	public void write(String path, byte[] contents) throws IOException {
		write(path, contents, 0, contents.length);
	}
	
	public byte[] read(String path) throws IOException {
		File file = open(path, File.O_RDONLY);
		byte[] bytes = file.read();
		file.close();
		return bytes;
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

	public String dirname(String path) {
		if(path.equals("/")) return path;
		String[] comps = path.split("/");
		String parent = String.join("/", Arrays.copyOfRange(comps, 0, comps.length-1));
		if(comps[0] == "") parent = "/" + parent;
		if(parent.equals("")) parent = "/";
		return parent;
	}
	
	public String basename(String path) {
		if(path == "/") return "/";
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
			unlink(safety);
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
	
	/** Close any resources associated with keeping this FS access open, e.g. sockets. The FS object may not be reused. */
	public void close() throws IOException {}
	
	/** Remove all content from filesystem */
	public void purge() throws IOException {
		if(exists("/")) {
			rmrf("/");
		}
	}
}
