package com.acrescrypto.zksync.fs.localfs;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;

import com.acrescrypto.zksync.exceptions.EACCESException;
import com.acrescrypto.zksync.exceptions.EISNOTDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.WalkAbortException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class LocalDirectory implements Directory {
	
	protected String path;
	protected LocalFS fs;
	protected Stat stat;
	
	LocalDirectory(LocalFS fs, String path, Stat stat) throws IOException {
		this.fs = fs;
		this.path = fs.absolutePath(path);
		this.stat = stat;
		if(!stat.isDirectory()) throw new EISNOTDIRException(path + ": not a directory");
	}
	
	public Collection<String> list() throws IOException {
		return list(0);
	}

	@Override
	public Collection<String> list(int opts) throws IOException {
		ArrayList<String> paths = new ArrayList<String>();
		// Files.newDirectoryStream(Paths.get(fs.root, path));
		try(DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(fs.root, path))) {
			for(Path entry: stream) {
				String entryPath = fs.join(path, entry.getFileName().toString());
				if((opts & LIST_OPT_OMIT_DIRECTORIES) != 0 && fs.stat(entryPath).isDirectory()) continue;
				paths.add(entry.getFileName().toString());
			}
		}
		
		if((opts & LIST_OPT_INCLUDE_DOT_DOTDOT) != 0) {
			paths.add(".");
			paths.add("..");
		}
		
		return paths;
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
	
	@Override
	public Collection<String> listRecursive() throws IOException {
		return listRecursive(0);
	}
	
	@Override
	public Collection<String> listRecursive(int opts) throws IOException {
		ArrayList<String> results = new ArrayList<String>();
		walk(opts, (path, stat, isBrokenSymlink, parent)->{
			results.add(path);
		});
		return results;
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
						cb.foundPath(subpath, stat, isBrokenSymlink, this);
					}
					
					if(!isDotDir) {
						fs.opendir(realSubpath).walkRecursiveIterate(opts, subpath, cb);
					}
				} else {
					cb.foundPath(subpath, stat, isBrokenSymlink, this);
				}
			} catch(ENOENTException exc) {
				// busted symlink
				Stat lstat = fs.lstat(realSubpath);
				cb.foundPath(subpath, lstat, true, this);
			} catch(EACCESException exc) {
				// directory with bad permissions
				cb.foundPath(subpath, null, false, this);
			}
		}
	}
	
	@Override
	public boolean contains(String entry) {
		java.io.File file;
		try {
			file = new java.io.File(fs.expandPath(Paths.get(path, entry).toString()));
			return file.exists();
		} catch (ENOENTException e) {
			// TODO API: (coverage) exception
			return false;
		}
	}
	
	@Override
	public Directory mkdir(String name) throws IOException {
		String fullPath = Paths.get(path, name).toString();
		fs.mkdir(fullPath);
		return fs.opendir(fullPath);
	}

	@Override
	public void link(File target, String link) throws IOException {
		link(target.getPath(), link);
	}
	
	@Override
	public void link(String target, String link) throws IOException {
		fs.link(target, Paths.get(this.path, link).toString());
	}

	@Override
	public void unlink(String target) throws IOException {
		fs.unlink(Paths.get(path, target).toString());
	}
	
	@Override
	public String getPath() {
		return path;
	}
	
	@Override
	public void close() {
	}
}
