package com.acrescrypto.zksync.fs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import com.acrescrypto.zksync.utility.Shuffler;

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
		/* As a concession to make PageQueue easier to implement, (non-cryptographic) randomness is added
		 * to the order that paths are returned in by shuffling the entries in each directory. This is NOT
		 * equivalent to a uniform shuffle of the entire result set, but is likely good enough for the purposes
		 * of ensuring peers send files in a generally different order from one another.
		 */
		Collection<String> subpaths = dir.list();
		ArrayList<String> arrayified = new ArrayList<>(subpaths);
		Shuffler shuffler = new Shuffler(subpaths.size());
		while(shuffler.hasNext()) {
			String path = arrayified.get(shuffler.next());
			paths.add(Paths.get(dir.getPath(), path).toString());
		}
		dir.close();
	}
	
	protected String popPath() {
		if(paths.isEmpty()) return null;
		return paths.remove();
	}
}
