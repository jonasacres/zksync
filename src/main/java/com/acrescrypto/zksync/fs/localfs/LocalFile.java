package com.acrescrypto.zksync.fs.localfs;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

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
		
		if(!fs.exists(path, (mode & O_NOFOLLOW) == 0)) {
			if((mode & O_CREAT) == 0 || (mode & O_WRONLY) == 0) throw new FileNotFoundException(path);
		}
		
		this.fileHandle = new RandomAccessFile(Paths.get(fs.getRoot(), path).toString(), modeStr);
		this.channel = this.fileHandle.getChannel();
		this.fs = fs;
		this.path = path;
		
		if((mode & O_APPEND) != 0) this.channel.position(this.fileHandle.length());
	}

	@Override
	public String getPath() {
		return this.path;
	}

	@Override
	public Stat getStat() throws IOException {
		return fs.stat(path);
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
