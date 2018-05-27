package com.acrescrypto.zksync.fs.ramfs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;

import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.File;

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
	public String[] list() throws IOException {
		return list(0);
	}

	@Override
	public String[] list(int opts) throws IOException {
		String prefix = fs.unscopedPath(path);
		if(!prefix.endsWith("/")) prefix += "/";
		
		LinkedList<String> matches = new LinkedList<String>();
		if((opts & LIST_OPT_INCLUDE_DOT_DOTDOT) != 0) {
			matches.add(".");
			matches.add("..");
		}
		
		synchronized(fs) {
			for(String path : fs.inodesByPath.keySet()) {
				if(path.equals("/")) continue;
				if((opts & LIST_OPT_OMIT_DIRECTORIES) != 0 && fs.stat(path).isDirectory()) continue;
				if(path.startsWith(prefix) && path.substring(prefix.length()).indexOf("/") == -1) {
					matches.add(path.substring(prefix.length()));
				}
			}
		}
		
		String[] array = new String[matches.size()];
		int i = 0;
		for(String match : matches) {
			array[i++] = match;
		}
		
		return array;
	}

	@Override
	public String[] listRecursive() throws IOException {
		return listRecursive(0);
	}

	@Override
	public boolean contains(String entry) throws IOException {
		return fs.exists(subpath(entry));
	}

	@Override
	public String[] listRecursive(int opts) throws IOException {
		String prefix = fs.unscopedPath(path);
		
		LinkedList<String> matches = new LinkedList<String>();
		if((opts & LIST_OPT_INCLUDE_DOT_DOTDOT) != 0) {
			matches.add(".");
			matches.add("..");
		}
		
		for(String path : fs.inodesByPath.keySet()) {
			if(!path.startsWith(prefix) || path.equals(prefix)) continue;
			if((opts & LIST_OPT_OMIT_DIRECTORIES) != 0 && fs.stat(fs.scopedPath(path)).isDirectory()) continue;
			path = path.substring(prefix.length());
			if(path.startsWith("/")) path = path.substring(1);
			matches.add(path);
		}
		
		String[] array = new String[matches.size()];
		int i = 0;
		for(String match : matches) {
			array[i++] = match;
		}
		
		return array;
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
