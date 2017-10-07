package com.acrescrypto.zksync.fs.localfs;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.concurrent.TimeUnit;

import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class LocalFile extends File {
	protected String path;
	protected RandomAccessFile fileHandle;
	protected FileChannel channel;
	protected long offset, size;
	protected LocalFS fs;
	
	LocalFile(LocalFS fs, String path, int mode) throws IOException {
		String modeStr = null;
		if((mode & O_RDWR) != 0) modeStr = "rw";
		if((mode & O_RDONLY) != 0) modeStr = "r";
		if((mode & O_WRONLY) != 0) modeStr = "rw"; // "w" is not supported apparently
		
		if(!Files.exists(Paths.get(path), (mode & O_NOFOLLOW) != 0 ? LinkOption.NOFOLLOW_LINKS : null)) {
			if((mode & O_CREAT) == 0 || (mode & O_WRONLY) == 0) throw new FileNotFoundException(path);
		}
		
		this.fileHandle = new RandomAccessFile(path, modeStr);
		this.channel = this.fileHandle.getChannel();
		this.fs = fs;
		
		if((mode & O_APPEND) != 0) this.channel.position(this.fileHandle.length());
	}

	@Override
	public String getPath() {
		return this.path;
	}

	@Override
	public Stat getStat() throws IOException {
		Stat stat = new Stat();
		PosixFileAttributes attrs = Files.readAttributes(Paths.get(path), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
		stat.setGroup(attrs.group().getName());
		stat.setUser(attrs.owner().getName());
		stat.setAtime(attrs.lastAccessTime().to(TimeUnit.NANOSECONDS));
		stat.setMtime(attrs.lastModifiedTime().to(TimeUnit.NANOSECONDS));
		stat.setCtime(attrs.creationTime().to(TimeUnit.NANOSECONDS));
		stat.setSize(attrs.size());

		int perms = 0;
		for(PosixFilePermission p : attrs.permissions()) {
			perms |= p.ordinal();
		}
		
		stat.setMode(perms);
		
		return stat;
	}

	@Override
	public void truncate(long size) throws IOException {
		FileOutputStream stream = null;
		FileChannel chan = null;
		
		try {
			stream = new FileOutputStream(this.path, true);
			chan = stream.getChannel();
			chan.truncate(size);
		} finally {
			if(chan != null) chan.close();
			if(stream != null) stream.close();
		}
	}

	@Override
	public void setMtime(long mtime) throws IOException {
		FileTime fileTime = FileTime.from(mtime, TimeUnit.NANOSECONDS);
		Files.setAttribute(Paths.get(path), "lastModifiedTime", fileTime);
	}

	@Override
	public void setAtime(long atime) throws IOException {
		FileTime fileTime = FileTime.from(atime, TimeUnit.NANOSECONDS);
		Files.setAttribute(Paths.get(path), "lastAccessTime", fileTime);
	}

	@Override
	public void setCtime(long ctime) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int read(byte[] buf, int offset, int maxLength) throws IOException {
		return channel.read(ByteBuffer.wrap(buf, offset, maxLength));
	}

	@Override
	public void write(byte[] data) throws IOException {
		channel.write(ByteBuffer.wrap(data));
	}

	@Override
	public void seek(long pos, int mode) throws IOException {
		long newOffset = -1;
		
		switch(mode) {
		case SEEK_SET:
			newOffset = pos;
			break;
		case SEEK_CUR:
			newOffset = channel.position() + pos;
			break;
		case SEEK_END:
			newOffset = getStat().getSize();
			break;
		}
		
		if(newOffset < 0) throw new IllegalArgumentException();
		channel.position(newOffset);
	}

	@Override
	public void close() throws IOException {
		channel.close();
		fileHandle.close();
	}

	@Override
	public void copy(File file) throws IOException {
		while(file.hasData()) write(file.read(1024*64));
		
		// zksync records both numeric and string IDs, but local FS may not map the same numeric ID to the same string
		// therefore, just use the string name, since that's more portable
		
		fs.chgrp(path, file.getStat().getGroup());
		fs.chown(path, file.getStat().getUser());
		fs.chmod(path, file.getStat().getMode());
		fs.setAtime(path, file.getStat().getAtime());
		fs.setCtime(path, file.getStat().getCtime());
		fs.setMtime(path, file.getStat().getMtime());
	}

	@Override
	public void rewind() {
		offset = 0;
	}

	@Override
	public boolean hasData() {
		return offset < size;
	}

}
