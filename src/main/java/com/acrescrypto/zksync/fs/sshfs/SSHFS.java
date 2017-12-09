package com.acrescrypto.zksync.fs.sshfs;

import java.io.IOException;

import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.Stat;

public class SSHFS extends FS {

	@Override
	public Stat stat(String path) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stat lstat(String path) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SSHDirectory opendir(String path) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void mkdir(String path) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void mkdirp(String path) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rmdir(String path) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void unlink(String path) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void link(String target, String link) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void symlink(String target, String link) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String readlink(String link) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void mknod(String path, int type, int major, int minor) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void mkfifo(String path) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void chmod(String path, int mode) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void chown(String path, int uid) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void chown(String path, String user) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void chgrp(String path, int gid) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void chgrp(String path, String group) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setMtime(String path, long mtime) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setCtime(String path, long ctime) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setAtime(String path, long atime) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(String path, byte[] contents) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public byte[] read(String path) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SSHFile open(String path, int mode) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void truncate(String path, long size) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
