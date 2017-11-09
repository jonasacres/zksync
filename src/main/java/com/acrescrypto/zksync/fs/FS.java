package com.acrescrypto.zksync.fs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.exceptions.EINVALException;

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
	public abstract byte[] read(String path) throws IOException;
	public abstract File open(String path, int mode) throws IOException;
	public abstract void truncate(String path, long size) throws IOException;
	
	public void rmrf(String path) throws IOException {
		Directory dir = opendir(path);
		try {
			for(String entry : dir.list()) {
				String subpath = Paths.get(path, entry).toString();
				Stat lstat = stat(subpath);
				if(lstat.isDirectory()) rmrf(subpath);
				else unlink(subpath);
			}
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
	
	public String pathForHash(byte[] hash) {
	    StringBuilder sb = new StringBuilder();
	    for(int i = 0; i < hash.length; i++) {
	        sb.append(String.format("%02x", hash[i]));
	    	if(i < 2) sb.append("/");
	    }
	    
		return sb.toString();
	}
	
	public byte[] hashFromPath(String path) throws EINVALException {
		String[] comps = path.split("/");
		if(comps.length < 3) throw new EINVALException(path);
		String[] hexComps = { comps[comps.length-3], comps[comps.length-2], comps[comps.length-1] };
		return Util.hexToBytes(String.join("", hexComps));
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
}
