package com.acrescrypto.zksync.fs.sshfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.ResizableOutputStream;

import net.schmizz.sshj.xfer.LocalDestFile;
import net.schmizz.sshj.xfer.LocalFileFilter;
import net.schmizz.sshj.xfer.LocalSourceFile;

public class SSHMemFile implements LocalSourceFile, LocalDestFile {
	public long atime, mtime;
	public int mode = 0644;
	
	protected ByteBuffer contentsBuf = ByteBuffer.allocate(0);
	protected boolean dirty = false;
	protected ResizableOutputStream outputStream;
	protected String name;
	
	public byte[] contents() {
		checkDirty();
		return contentsBuf.array();
	}
	
	public void setContents(byte[] contents) {
		contentsBuf = ByteBuffer.wrap(contents);
	}

	@Override
	public SSHMemFile getChild(String arg0) {
		throw new RuntimeException("children not supported by SSHMemfile");
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		return outputStream = new ResizableOutputStream((byte[] buf, int offset) -> {
			dirty = true;
		});
	}

	@Override
	public SSHMemFile getTargetDirectory(String arg0) throws IOException {
		throw new RuntimeException("writing directories not supported by SSHMemfile");
	}

	@Override
	public SSHMemFile getTargetFile(String arg0) throws IOException {
		name = arg0;
		return this;
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
		throw new RuntimeException("children not supported by SSHMemfile");
	}

	@Override
	public InputStream getInputStream() throws IOException {
		checkDirty();
		return new ByteArrayInputStream(contentsBuf.array());
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
		checkDirty();
		return contentsBuf.capacity();
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public int getPermissions() throws IOException {
		return mode;
	}

	@Override
	public boolean isDirectory() {
		return false;
	}

	@Override
	public boolean isFile() {
		return true;
	}

	@Override
	public boolean providesAtimeMtime() {
		return true;
	}
	
	protected void checkDirty() {
		if(!dirty) return;
		contentsBuf = ByteBuffer.wrap(outputStream.toArray());
		dirty = false;
	}
}
