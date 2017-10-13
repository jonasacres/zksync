package com.acrescrypto.zksync.fs;

import java.io.IOException;

public interface Directory {
	public String[] list() throws IOException;
	public Directory mkdir(String name) throws IOException;
	public void link(File target, String link) throws IOException;
	public void link(String target, String link) throws IOException;
	public void unlink(String path) throws IOException;
	public String getPath();
}
