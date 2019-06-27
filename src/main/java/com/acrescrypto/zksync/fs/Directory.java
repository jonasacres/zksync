package com.acrescrypto.zksync.fs;

import java.io.IOException;
import java.util.Collection;

public interface Directory extends AutoCloseable {
	public interface DirectoryWalkCallback {
		void foundPath(String path, Stat stat, boolean isBrokenSymlink, Directory dir) throws IOException;
	}

	public final static int LIST_OPT_OMIT_DIRECTORIES = 1 << 0;
	public final static int LIST_OPT_INCLUDE_DOT_DOTDOT = 1 << 1;
	public final static int LIST_OPT_DONT_FOLLOW_SYMLINKS = 1 << 2;
	
	public boolean walk(DirectoryWalkCallback cb) throws IOException;
	public boolean walk(int opts, DirectoryWalkCallback cb) throws IOException;
	
	public Collection<String> list() throws IOException;
	public Collection<String> list(int opts) throws IOException;
	public Collection<String> listRecursive() throws IOException;
	
	public boolean contains(String entry) throws IOException;
	public Collection<String> listRecursive(int opts) throws IOException;
	public Directory mkdir(String name) throws IOException;
	public void link(File target, String link) throws IOException;
	public void link(String target, String link) throws IOException;
	public void unlink(String path) throws IOException;
	public String getPath();
	public void close() throws IOException;
}
