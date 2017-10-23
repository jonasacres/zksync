package com.acrescrypto.zksync.fs.localfs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import com.acrescrypto.zksync.exceptions.EISNOTDIRException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.File;

public class LocalDirectory implements Directory {
	
	protected String path;
	protected LocalFS fs;
	
	LocalDirectory(LocalFS fs, String path) throws IOException {
		this.fs = fs;
		this.path = path;
		if(!fs.stat(path).isDirectory()) throw new EISNOTDIRException(path + ": not a directory");
	}
	
	public String[] list() throws IOException {
		return list(0);
	}

	@Override
	public String[] list(int opts) throws IOException {
		ArrayList<String> paths = new ArrayList<String>();
		for(Path entry: Files.newDirectoryStream(Paths.get(fs.root, path))) {
			String entryPath = Paths.get(path, entry.getFileName().toString()).toString();
			if((opts & LIST_OPT_OMIT_DIRECTORIES) == 1 && fs.stat(entryPath).isDirectory()) continue;
			paths.add(entry.getFileName().toString());
		}
		
		int size = paths.size();
		if((opts & LIST_OPT_INCLUDE_DOT_DOTDOT) != 0) size += 2;
		String[] retval = new String[size];
		if((opts & LIST_OPT_INCLUDE_DOT_DOTDOT) != 0) {
			retval[retval.length-2] = ".";
			retval[retval.length-1] = "..";
		}
		
		for(int i = 0; i < paths.size(); i++) retval[i] = paths.get(i);
		return retval;
	}
	
	@Override
	public String[] listRecursive() throws IOException {
		return listRecursive(0);
	}
	
	@Override
	public String[] listRecursive(int opts) throws IOException {
		ArrayList<String> results = new ArrayList<String>();
		listRecursiveIterate(opts, results, "");
		String[] buf = new String[results.size()];
		return results.toArray(buf);
	}
	
	public void listRecursiveIterate(int opts, ArrayList<String> results, String prefix) throws IOException {
		for(String entry : list(opts & ~Directory.LIST_OPT_OMIT_DIRECTORIES)) {
			String subpath = Paths.get(prefix, entry).toString(); // what we return in our results
			String realSubpath = Paths.get(path, entry).toString(); // what we can look up directly in fs
			if(fs.stat(Paths.get(path, entry).toString()).isDirectory()) {
				boolean isDotDir = entry.equals(".") || entry.equals("..");
				if((opts & Directory.LIST_OPT_OMIT_DIRECTORIES) == 0) {
					results.add(subpath);
				}
				
				if(!isDotDir) fs.opendir(realSubpath).listRecursiveIterate(opts, results, subpath);
			} else {
				results.add(subpath);
			}
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
