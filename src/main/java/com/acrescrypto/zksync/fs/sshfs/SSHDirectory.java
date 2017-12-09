package com.acrescrypto.zksync.fs.sshfs;

import java.io.IOException;

import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.File;

public class SSHDirectory implements Directory {

	@Override
	public String[] list() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] list(int opts) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] listRecursive() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean contains(String entry) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public String[] listRecursive(int opts) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Directory mkdir(String name) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void link(File target, String link) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void link(String target, String link) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void unlink(String path) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getPath() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
