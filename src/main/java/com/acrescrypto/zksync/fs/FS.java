package com.acrescrypto.zksync.fs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import com.acrescrypto.zksync.ThroughputMeter;
import com.acrescrypto.zksync.ThroughputTransaction;

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
	
	public abstract void write(String path, byte[] contents) throws IOException;
	public abstract File open(String path, int mode) throws IOException;
	public abstract void truncate(String path, long size) throws IOException;
	
	protected ThroughputMeter rxThroughput = new ThroughputMeter();
	protected long expectedReadBytes = 0;
	
	protected synchronized void expectRead(long count) {
		expectedReadBytes += count;
	}
	
	protected synchronized void expectedReadFinished(long count) {
		expectedReadBytes -= count;
	}
	
	public long getBytesPerSecond() {
		return rxThroughput.getBytesPerSecond();
	}
	
	public long expectedReadWaitTime(long bytes) {
		if(getBytesPerSecond() < 0) return -1;
		return 1000*(bytes + expectedReadBytes)/getBytesPerSecond();
	}
	
	public byte[] _read(String path) throws IOException {
		File file = open(path, File.O_RDONLY);
		byte[] bytes = file.read();
		file.close();
		return bytes;
	}
	
	public final byte[] read(String path) throws IOException {
		ThroughputTransaction transaction = rxThroughput.beginTransaction();
		byte[] bytes = _read(path);
		transaction.finish(bytes.length);
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
			exc.printStackTrace();
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
	
	public static String pathForHash(byte[] hash) {
	    StringBuilder sb = new StringBuilder();
	    for(int i = 0; i < hash.length; i++) {
	        sb.append(String.format("%02x", hash[i]));
	    	if(i < 2) sb.append("/");
	    }
	    
		return sb.toString();
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
}
