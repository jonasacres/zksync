package com.acrescrypto.zksync.fs.sshfs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.File;

public class SSHDirectory implements Directory {
	protected SSHFS fs;
	protected String path;
	
	public SSHDirectory(SSHFS fs, String path) {
		this.fs = fs;
		this.path = fs.absolutePath(path);
	}

	@Override
	public String[] list() throws IOException {
		return list(0);
	}

	@Override
	public String[] list(int opts) throws IOException {
		String ls = new String(fs.execAndCheck("ls", "-1 \"" + fs.qualifiedPath(path) + "\""));
		String[] files;
		if(ls.length() == 0) {
			files = new String[0];
		} else {
			files = ls.split("\n");
		}
		
		ArrayList<String> filtered = new ArrayList<String>();
		for(String file : files) {
			String filepath = Paths.get(path, file).toString();
			if((opts & LIST_OPT_INCLUDE_DOT_DOTDOT) == 0 && (file.equals(".") || file.equals(".."))) continue;
			if((opts & LIST_OPT_OMIT_DIRECTORIES) != 0 && fs.stat(filepath).isDirectory()) continue;
			filtered.add(file);
		}
		
		String[] array = new String[filtered.size()];
		filtered.toArray(array);
		return array;
	}

	@Override
	public String[] listRecursive() throws IOException {
		return listRecursive(0);
	}

	@Override
	public String[] listRecursive(int opts) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean contains(String entry) throws IOException {
		return fs.exists(fs.qualifiedPath(Paths.get(path, entry).toString()));
	}

	@Override
	public SSHDirectory mkdir(String name) throws IOException {
		String newPath = Paths.get(path, name).toString();
		fs.mkdir(newPath);
		return new SSHDirectory(fs, newPath);
	}

	@Override
	public void link(File _target, String link) throws IOException {
		SSHFile target = (SSHFile) _target;
		link(target.path, link);
	}

	@Override
	public void link(String target, String link) throws IOException {
		fs.link(target, Paths.get(path, link).toString());
	}

	@Override
	public void unlink(String path) throws IOException {
		fs.unlink(path);
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public void close() throws IOException {
	}

}
