package com.acrescrypto.zksync.fs.remotefs;

import java.io.IOException;

import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class RemoteFS extends FS {

	@Override
	public Stat stat(String path) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Stat lstat(String path) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public Directory opendir(String path) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void mkdir(String path) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void mkdirp(String path) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void rmdir(String path) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void unlink(String path) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void link(String target, String link) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void symlink(String target, String link) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String readlink(String link) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void mknod(String path, int type, int major, int minor) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void mkfifo(String path) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void chmod(String path, int mode) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void chown(String path, int uid) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void chown(String path, String user) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void chgrp(String path, int gid) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void chgrp(String path, String group) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setMtime(String path, long mtime) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setCtime(String path, long ctime) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void setAtime(String path, long atime) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void write(String path, byte[] contents) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] _read(String path) throws IOException {
		/* TODO P2P: Parse out the specific page/chunk we're trying to access, and request it from the remote peer.
		 */
		return null;
	}

	@Override
	public File open(String path, int mode) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void truncate(String path, long size) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public RemoteFS scopedFS(String subpath) {
		throw new UnsupportedOperationException(); // no concept of scoping a RemoteFS
	}
}
