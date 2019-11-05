package com.acrescrypto.zksync.fs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import com.acrescrypto.zksync.utility.Shuffler;

public class DirectoryTraverser {
	public class TraversalEntry {
		String path;
		Stat stat;
		
		public TraversalEntry(String path, Stat stat) {
			this.path = path;
			this.stat = stat;
		}
		
		public String getPath() { return path; }
		public Stat getStat() { return stat; }
	}
	
	LinkedList<TraversalEntry> entries = new LinkedList<>();
	TraversalEntry nextEntry;
	Directory base;
	
	FS fs;
	boolean initialized = false;

	boolean recursive = true;
	boolean hideDirectories = true;
	boolean followSymlinks = false;
	boolean includeDotDirs = false;
	boolean includeBase = false;
	
	boolean baseAdded = false;
	
	public DirectoryTraverser(FS fs, Directory base) throws IOException {
		this(fs, base, false);
	}
	
	public static DirectoryTraverser withPath(FS fs, String path) throws IOException {
		try(Directory dir = fs.opendir(path)) {
			return new DirectoryTraverser(fs, dir, true);
		}
	}
	
	public DirectoryTraverser(FS fs, Directory base, boolean immediateInit) throws IOException {
		this.fs = fs;
		this.base = base;
		if(immediateInit) {
			initializeIfNeeded();
		}
	}
	
	protected void initializeIfNeeded() throws IOException {
		if(initialized) return;
		initialized = true;
		addDirectory(base);
		stageNext();
	}
	
	public boolean hasNext() throws IOException {
		initializeIfNeeded();
		return nextEntry != null;
	}
	
	public TraversalEntry next() throws IOException {
		initializeIfNeeded();
		if(nextEntry == null) return null;
		TraversalEntry retEntry = nextEntry;
		
		stageNext();
		return retEntry;
	}
	
	protected void stageNext() throws IOException {
		/* a bit convoluted, but we stage the next result in nextEntry so we can filter out directories while still
		 * having hasNext() work in all cases.
		 */
		nextEntry = popEntry();
		if(nextEntry == null) {
			if(!includeBase || baseAdded) return;
			
			Stat stat = followSymlinks ? fs.stat(base.getPath()) : fs.lstat(base.getPath());
			nextEntry = new TraversalEntry(base.getPath(), stat);
			baseAdded = true;
			return;
		}
		
		String basename = fs.basename(nextEntry.path);
		boolean isDotDir = basename.equals(".") || basename.equals("..");

		if(recursive) {
			if(hideDirectories) {
				while(nextEntry != null && nextEntry.stat.isDirectory()) {
					if(!isDotDir) {
						addDirectory(fs.opendir(nextEntry.path));
					}
					
					nextEntry = popEntry();
					basename = fs.basename(nextEntry.path);
					isDotDir = basename.equals(".") || basename.equals("..");
				}
			} else {
				if(nextEntry.stat.isDirectory() && !isDotDir) {
					addDirectory(fs.opendir(nextEntry.path));
				}
			}
		}
	}
	
	protected void addDirectory(Directory dir) throws IOException {
		/* As a concession to make PageQueue easier to implement, (non-cryptographic) randomness is added
		 * to the order that paths are returned in by shuffling the entries in each directory. This is NOT
		 * equivalent to a uniform shuffle of the entire result set, but is likely good enough for the purposes
		 * of ensuring peers send files in a generally different order from one another.
		 */
		int flags = includeDotDirs ? Directory.LIST_OPT_INCLUDE_DOT_DOTDOT : 0;
		Collection<String> subpaths = dir.list(flags);
		ArrayList<String> arrayified = new ArrayList<>(subpaths);
		Shuffler shuffler = new Shuffler(subpaths.size());
		while(shuffler.hasNext()) {
			String subpath = arrayified.get(shuffler.next()),
			          path = Paths.get(dir.getPath(), subpath).toString();
			Stat stat = followSymlinks ? fs.stat(path) : fs.lstat(path);
			TraversalEntry entry = new TraversalEntry(path, stat);
			entries.add(entry);
		}
		dir.close();
	}
	
	protected TraversalEntry popEntry() {
		if(entries.isEmpty()) return null;
		return entries.remove();
	}
	
	public boolean getHideDirectories() {
		return hideDirectories;
	}
	
	public void setHideDirectories(boolean hideDirectories) {
		this.hideDirectories = hideDirectories;
	}

	public boolean getFollowSymlinks() {
		return followSymlinks;
	}
	
	public void setFollowSymlinks(boolean followSymlinks) {
		this.followSymlinks = followSymlinks;
	}
	
	public boolean getRecursive() {
		return recursive;
	}

	public void setRecursive(boolean recursive) {
		this.recursive = recursive;
	}
	
	public boolean getIncludeDotDirs() {
		return includeDotDirs;
	}
	
	public void setIncludeDotDirs(boolean includeDotDirs) {
		this.includeDotDirs = includeDotDirs;
	}
	
	public boolean getIncludeBase() {
		return includeBase;
	}
	
	public void setIncludeBase(boolean includeBase) {
		this.includeBase = includeBase;
	}
}
