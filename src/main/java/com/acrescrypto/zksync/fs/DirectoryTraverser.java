package com.acrescrypto.zksync.fs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;

public class DirectoryTraverser {
	LinkedList<String> paths = new LinkedList<String>();
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
		nextPath = popPath();
		if(nextPath == null) return;
		
		if(hideDirectories) {
			while(nextPath != null && fs.stat(nextPath).isDirectory()) {
				addDirectory(fs.opendir(nextPath));
				nextPath = popPath();
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
		dir.close();
	}
	
	protected String popPath() {
		/** right now our only customer is PageQueue which really wants stuff in random order.
		 * This isn't a uniform distribution: files from directories we've visited will be selected more often
		 * than files in directories we haven't visited yet. This should be good enough for our purposes. 
		 */
		if(paths.isEmpty()) return null;
		int index = (int) (Math.random() * paths.size());
		return paths.remove(index);
	}
}
