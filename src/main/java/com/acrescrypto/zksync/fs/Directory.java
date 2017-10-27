package com.acrescrypto.zksync.fs;

import java.io.IOException;

public interface Directory {
	public final static int LIST_OPT_OMIT_DIRECTORIES = 1 << 0;
	public final static int LIST_OPT_INCLUDE_DOT_DOTDOT = 1 << 1;
	
	public String[] list() throws IOException;
	public String[] list(int opts) throws IOException;
	public String[] listRecursive() throws IOException;
	public String[] listRecursive(int opts) throws IOException;
	public Directory mkdir(String name) throws IOException;
	public void link(File target, String link) throws IOException;
	public void link(String target, String link) throws IOException;
	public void unlink(String path) throws IOException;
	public String getPath();
	public void close() throws IOException;
}
