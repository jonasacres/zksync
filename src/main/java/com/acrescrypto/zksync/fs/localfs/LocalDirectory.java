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
		this.path = path;
		if(!fs.stat(path).isDirectory()) throw new EISNOTDIRException(path + ": not a directory");
	}

	public String[] list() throws IOException {
		ArrayList<String> paths = new ArrayList<String>();
		for(Path entry: Files.newDirectoryStream(Paths.get(this.path))) {
			paths.add(entry.toString());
		}
		
		String[] retval = new String[paths.size()];
		for(int i = 0; i < paths.size(); i++) retval[i] = paths.get(i);
		return retval;
	}

	public Directory mkdir(String name) throws IOException {
		String fullPath = Paths.get(path, name).toString();
		fs.mkdir(fullPath);
		return fs.opendir(fullPath);
	}

	public void link(String path, File file) throws IOException {
		link(path, file.getPath());
	}
	
	public void link(String src, String target) throws IOException {
		fs.link(src, Paths.get(this.path, target).toString());
	}

	public void unlink(String target) throws IOException {
		fs.unlink(Paths.get(path, target).toString());
	}
	
	public String getPath() {
		return path;
	}
}
