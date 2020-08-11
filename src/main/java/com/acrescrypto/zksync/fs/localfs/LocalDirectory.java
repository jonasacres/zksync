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
import com.acrescrypto.zksync.fs.FSPath;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class LocalDirectory implements Directory {
	
	protected FSPath path;
	protected LocalFS fs;
	protected Stat stat;
	
	LocalDirectory(LocalFS fs, String path, Stat stat) throws IOException {
		this.fs   = fs;
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
		Path jPath = Paths.get(fs.root().join(path).toNative());
		try(DirectoryStream<Path> stream = Files.newDirectoryStream(jPath)) {
			for(Path entry: stream) {
				String entryPath = path.join(entry.getFileName().toString()).toPosix();
				if((opts & LIST_OPT_OMIT_DIRECTORIES) != 0 && fs.stat(entryPath).isDirectory()) continue;
				String posixPath = new FSPath(entry.getFileName().toString()).toPosix();
				paths.add(posixPath);
			}
		} catch(IOException exc) {
		    // we may run into access problems; just ignore those
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
			walkRecursiveIterate(opts, FSPath.with(""), cb);
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
	
	protected void walkRecursiveIterate(int opts, FSPath prefix, DirectoryWalkCallback cb) throws IOException {
		for(String entry : list(opts & ~Directory.LIST_OPT_OMIT_DIRECTORIES)) {
			FSPath subpath     = prefix.join(entry); // what we return in our results
			FSPath realSubpath = path  .join(entry); // what we can look up directly in fs
			
			try {
				Stat stat;
				boolean isBrokenSymlink = false;
				
				if((opts & Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS) == 0) {
					stat = fs.stat(realSubpath.standardize());
				} else {
					stat = fs.lstat(realSubpath.standardize());
					if(stat.isSymlink() && !fs.exists(realSubpath.standardize())) {
						isBrokenSymlink = true;
					}
				}
				
				if(stat.isDirectory()) {
					boolean isDotDir = entry.equals(".") || entry.equals("..");
					if((opts & Directory.LIST_OPT_OMIT_DIRECTORIES) == 0) {
						cb.foundPath(subpath.standardize(), stat, isBrokenSymlink, this);
					}
					
					if(!isDotDir) {
						fs.opendir(realSubpath.standardize()).walkRecursiveIterate(opts, subpath, cb);
					}
				} else {
					cb.foundPath(subpath.standardize(), stat, isBrokenSymlink, this);
				}
			} catch(ENOENTException exc) {
				// busted symlink
				Stat lstat = fs.lstat(realSubpath.standardize());
				cb.foundPath(subpath.standardize(), lstat, true, this);
			} catch(EACCESException exc) {
				// directory with bad permissions
				cb.foundPath(subpath.standardize(), null, false, this);
			}
		}
	}
	
	@Override
	public boolean contains(String entry) {
		String expandedPath = fs.root().join(path).normalize().join(entry).toNative();
		java.io.File file = new java.io.File(expandedPath);
		return file.exists();
	}
	
	@Override
	public Directory mkdir(String name) throws IOException {
		String fullPath = path.join(name).toNative();
		fs.mkdir(fullPath);
		return fs.opendir(fullPath);
	}

	@Override
	public void link(File target, String link) throws IOException {
		link(target.getPath(), link);
	}
	
	@Override
	public void link(String target, String link) throws IOException {
		fs.link(target, path.join(link).standardize());
	}

	@Override
	public void unlink(String target) throws IOException {
		fs.unlink(path.join(target).standardize());
	}
	
	@Override
	public String getPath() {
		return path.standardize();
	}
	
	@Override
	public void close() {
	}
}
