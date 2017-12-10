package com.acrescrypto.zksync.fs.sshfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import net.schmizz.sshj.xfer.LocalDestFile;
import net.schmizz.sshj.xfer.LocalFileFilter;
import net.schmizz.sshj.xfer.LocalSourceFile;

public class SSHMemFile implements LocalSourceFile, LocalDestFile {
	public long atime, mtime;
	public int mode;
	
	protected ByteBuffer contentsBuf = ByteBuffer.allocate(0);
	
	public byte[] contents() {
		return contentsBuf.array();
	}
	
	public void setContents(byte[] contents) {
		contentsBuf = ByteBuffer.wrap(contents);
	}

	@Override
	public SSHMemFile getChild(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SSHMemFile getTargetDirectory(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SSHMemFile getTargetFile(String arg0) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setLastAccessedTime(long atime) throws IOException {
		this.atime = atime;
	}

	@Override
	public void setLastModifiedTime(long mtime) throws IOException {
		this.mtime = mtime;
	}

	@Override
	public void setPermissions(int mode) throws IOException {
		this.mode = mode;
	}

	@Override
	public Iterable<SSHMemFile> getChildren(LocalFileFilter filter) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getLastAccessTime() throws IOException {
		return atime;
	}

	@Override
	public long getLastModifiedTime() throws IOException {
		return mtime;
	}

	@Override
	public long getLength() {
		return contentsBuf.capacity();
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getPermissions() throws IOException {
		return mode;
	}

	@Override
	public boolean isDirectory() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isFile() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean providesAtimeMtime() {
		return true;
	}

}
