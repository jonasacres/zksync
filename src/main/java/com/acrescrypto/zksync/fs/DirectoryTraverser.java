package com.acrescrypto.zksync.fs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Stack;

public class DirectoryTraverser {
	Stack<String> paths = new Stack<String>();
	String nextPath;
	
	FS fs;
	boolean hideDirectories = true;
	
	public DirectoryTraverser(FS fs, Directory base) throws IOException {
		this.fs = fs;
		addDirectory(base);
		stageNext();
	}
	
	public boolean hasNext() {
		return nextPath != null;
	}
	
	public String next() throws IOException {
		if(nextPath == null) return null;
		String retPath = nextPath;
		
		stageNext();
		return retPath;
	}
	
	protected void stageNext() throws IOException {
		/* a bit convoluted, but we stage the next result in nextPath so we can filter out directories while still
		 * having hasNext() work in all cases.
		 */
		nextPath = paths.isEmpty() ? null : paths.pop();
		if(nextPath == null) return;
		
		if(hideDirectories) {
			while(fs.stat(nextPath).isDirectory()) {
				addDirectory(fs.opendir(nextPath));
				if(paths.isEmpty()) {
					nextPath = null;
					break;
				}
				
				nextPath = paths.pop();
			}
		} else {
			if(fs.stat(nextPath).isDirectory()) {
				addDirectory(fs.opendir(nextPath));
			}
		}
	}
	
	protected void addDirectory(Directory dir) throws IOException {
		String[] subpaths = dir.list();
		for(String path : subpaths) {
			paths.add(Paths.get(dir.getPath(), path).toString());
		}
	}
}
