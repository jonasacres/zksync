package com.acrescrypto.zksync.fs.ramfs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import com.acrescrypto.zksync.exceptions.EACCESException;
import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.WalkAbortException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class RAMDirectory implements Directory {
	RAMFS fs;
	String path;
	
	public RAMDirectory(RAMFS fs, String path) {
		if(path.equals(".") || path.equals("")) path = "/";
		if(!path.startsWith("/")) path = "/" + path;
		this.fs = fs;
		this.path = path;
	}

	@Override
	public Collection<String> list() throws IOException {
		return list(0);
	}

	@Override
	public Collection<String> list(int opts) throws IOException {
		if(fs.lstat(path).isSymlink()) {
			try(RAMDirectory target = fs.opendir(fs.readlink(path))) {
				return target.list(opts);
			}
		}
		
		String prefix = fs.unscopedPath(path);
		if(!prefix.endsWith("/")) prefix += "/";
		
		LinkedList<String> matches = new LinkedList<String>();
		if((opts & LIST_OPT_INCLUDE_DOT_DOTDOT) != 0) {
			matches.add(".");
			matches.add("..");
		}
		
		synchronized(fs.inodesByPath) {
			Set<String> keys = new HashSet<>(fs.inodesByPath.keySet());
			for(String path : keys) {
				if(path.equals("/")) continue;
				if((opts & LIST_OPT_OMIT_DIRECTORIES) != 0 && fs.stat(path).isDirectory()) continue;
				if(path.startsWith(prefix) && path.substring(prefix.length()).indexOf("/") == -1) {
					matches.add(path.substring(prefix.length()));
				}
			}
		}
		
		return matches;
	}

	@Override
	public boolean contains(String entry) throws IOException {
		return fs.exists(subpath(entry));
	}
	
	@Override
	public boolean walk(DirectoryWalkCallback cb) throws IOException {
		return walk(0, cb);
	}
	
	@Override
	public boolean walk(int opts, DirectoryWalkCallback cb) throws IOException {
		try {
			walkRecursiveIterate(opts, "", cb);
			return true;
		} catch(WalkAbortException exc) {
			return false;
		}
	}
	
	protected void walkRecursiveIterate(int opts, String prefix, DirectoryWalkCallback cb) throws IOException {
		for(String entry : list(opts & ~Directory.LIST_OPT_OMIT_DIRECTORIES)) {
			String subpath = Paths.get(prefix, entry).toString(); // what we return in our results
			String realSubpath = Paths.get(path, entry).toString(); // what we can look up directly in fs
			try {
				Stat stat;
				boolean isBrokenSymlink = false;
				
				if((opts & Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS) == 0) {
					stat = fs.stat(realSubpath);
				} else {
					stat = fs.lstat(realSubpath);
					if(stat.isSymlink() && !fs.exists(realSubpath)) {
						isBrokenSymlink = true;
					}
				}
				
				if(stat.isDirectory()) {
					boolean isDotDir = entry.equals(".") || entry.equals("..");
					if((opts & Directory.LIST_OPT_OMIT_DIRECTORIES) == 0) {
						cb.foundPath(subpath, stat, isBrokenSymlink);
					}
					
					if(!isDotDir) {
						fs.opendir(realSubpath).walkRecursiveIterate(opts, subpath, cb);
					}
				} else {
					cb.foundPath(subpath, stat, isBrokenSymlink);
				}
			} catch(ENOENTException exc) {
				// busted symlink
				Stat lstat = fs.lstat(realSubpath);
				cb.foundPath(subpath, lstat, true);
			} catch(EACCESException exc) {
				// directory with bad permissions
				cb.foundPath(subpath, null, false);
			}
		}
	}

	@Override
	public Collection<String> listRecursive() throws IOException {
		return listRecursive(0);
	}

	@Override
	public Collection<String> listRecursive(int opts) throws IOException {
		LinkedList<String> matches = new LinkedList<String>();
		
		walk(opts, (path, stat, isBrokenSymlink)->{
			matches.add(path);
		});
		return matches;
	}

	@Override
	public Directory mkdir(String name) throws IOException {
		fs.mkdir(subpath(name));
		return fs.opendir(subpath(name));
	}

	@Override
	public void link(File target, String link) throws IOException {
		if(fs.exists(subpath(link))) throw new EEXISTSException(link);
		fs.setInode(subpath(link), ((RAMFile) target).inode);
	}

	@Override
	public void link(String target, String link) throws IOException {
		fs.link(target, subpath(link));
	}

	@Override
	public void unlink(String path) throws IOException {
		fs.unlink(subpath(path));
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public void close() throws IOException {
	}

	public String subpath(String basename) {
		return Paths.get(path, basename).toString();
	}
}
