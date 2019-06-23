package com.acrescrypto.zksync.fs.swarmfs;

import java.io.IOException;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.TimedReader;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.net.PeerSwarm;
import com.acrescrypto.zksync.utility.Util;

public class SwarmFS extends FS implements TimedReader {
	public final static int REQUEST_PRIORITY = 100;
	public final static int REQUEST_TAG_STRUCTURE_PRIORITY = 200;
	PeerSwarm swarm;

	public SwarmFS(PeerSwarm swarm) {
		this.swarm = swarm;
	}

	@Override
	public Stat stat(String path) throws IOException {
		byte[] tag = Page.tagForPath(path);
		if(tag.length != swarm.getConfig().getAccessor().getMaster().getCrypto().hashLength()) {
			throw new ENOENTException(path);
		}

		Stat stat = new Stat();
		stat.setGid(0);
		stat.setUid(0);
		stat.setMode(0400);
		stat.setType(Stat.TYPE_REGULAR_FILE);
		stat.setDevMinor(0);
		stat.setDevMajor(0);
		stat.setGroup("root");
		stat.setUser("root");
		stat.setAtime(0);
		stat.setMtime(0);
		stat.setCtime(0);
		stat.setSize(swarm.getConfig().getSerializedPageSize());
		stat.setInodeId(Util.shortTag(tag));
		
		return stat;
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
	public void symlink_unsafe(String target, String link) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public String readlink(String link) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String readlink_unsafe(String link) throws IOException {
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
	public void write(String path, byte[] contents, int offset, int length) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public byte[] read(String path) throws IOException {
		return read(path, -1);
	}

	@Override
	public File open(String path, int mode) throws IOException {
		byte[] pageTag = Page.tagForPath(path);
		swarm.waitForPage(REQUEST_PRIORITY, pageTag, -1);
		return swarm.getConfig().getCacheStorage().open(path, mode);
	}

	@Override
	public void truncate(String path, long size) throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public SwarmFS scopedFS(String subpath) {
		throw new UnsupportedOperationException(); // no concept of scoping here
	}
	
	@Override
	public SwarmFS unscopedFS() {
		return new SwarmFS(swarm);
	}
	
	public PeerSwarm getSwarm() {
		return swarm;
	}
	
	public String toString() {
		return this.getClass().getSimpleName() + " " + Util.formatArchiveId(swarm.getConfig().getArchiveId());
	}

	public byte[] read(String path, long timeoutRemainingMs) throws IOException {
		byte[] pageTag = Page.tagForPath(path);
		swarm.waitForPage(REQUEST_PRIORITY, pageTag, timeoutRemainingMs);
		return swarm.getConfig().getCacheStorage().read(path);
	}
}
