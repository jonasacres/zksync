package com.acrescrypto.zksync.fs;

import java.io.IOException;

public interface Directory {
	public String[] list() throws IOException;
	public void link(String path, File file) throws IOException;
	public void link(String path, String dest) throws IOException;
	public void unlink(String path) throws IOException;
}
