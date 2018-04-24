package com.acrescrypto.zksync.fs.localfs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;

import com.acrescrypto.zksync.exceptions.*;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class LocalFile extends File {
	protected String path;
	protected RandomAccessFile fileHandle;
	protected FileChannel channel;
	protected long offset, size;
	protected int mode;
	
	LocalFile(LocalFS fs, String path, int mode) throws IOException {
		super(fs);
		String modeStr = null;
		if((mode & O_RDWR) != 0) modeStr = "rw";
		if((mode & O_RDONLY) != 0) modeStr = "r";
		if((mode & O_WRONLY) != 0) modeStr = "rw"; // "w" is not supported apparently
		
		if(!fs.exists(path, (mode & O_NOFOLLOW) == 0)) {
			if((mode & O_CREAT) == 0 || (mode & O_WRONLY) == 0) throw new ENOENTException(path);
		}
		
		try {
			if((mode & O_NOFOLLOW) != 0 && fs.lstat(path).isSymlink()) throw new EMLINKException(path);
			if(fs.stat(path).isDirectory()) {
				throw new EISDIRException(path);
			}
		} catch(ENOENTException e) {
		}
		
		this.fileHandle = new RandomAccessFile(Paths.get(fs.getRoot(), path).toString(), modeStr);
		this.channel = this.fileHandle.getChannel();
		this.path = path;
		this.mode = mode;
		
		if((mode & O_APPEND) != 0) this.channel.position(this.fileHandle.length());
		else if((mode & (O_TRUNC|O_WRONLY)) == (O_TRUNC|O_WRONLY)) truncate(0);
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
		assertWritable();
		fs.truncate(path, size);
	}

	@Override
	protected int _read(byte[] buf, int offset, int maxLength) throws IOException {
		assertReadable();
		return channel.read(ByteBuffer.wrap(buf, offset, maxLength));
	}

	@Override
	public void write(byte[] data) throws IOException {
		assertWritable();
		channel.write(ByteBuffer.wrap(data));
		size = Math.max(size, channel.position());
	}
	
	@Override
	public void flush() throws IOException {
		channel.force(true);
		fileHandle.getFD().sync();
	}

	@Override
	public long seek(long pos, int mode) throws IOException {
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
		return channel.position();
	}

	@Override
	public void close() throws IOException {
		channel.close();
		fileHandle.close();
	}

	@Override
	public void copy(File file) throws IOException {
		assertWritable();
		file.rewind();
		while(file.hasData()) write(file.read(1024*64));
		flush();
		
		// zksync records both numeric and string IDs, but local FS may not map the same numeric ID to the same string
		// therefore, just use the string name, since that's more portable
		
		try { fs.chgrp(path, file.getStat().getGroup()); } catch(UnsupportedOperationException exc) {}
		try { fs.chown(path, file.getStat().getUser()); } catch(UnsupportedOperationException exc) {}
		try { fs.chmod(path, file.getStat().getMode()); } catch(UnsupportedOperationException exc) {}
		try { fs.setAtime(path, file.getStat().getAtime()); } catch(UnsupportedOperationException exc) {}
		try { fs.setCtime(path, file.getStat().getCtime()); } catch(UnsupportedOperationException exc) {}
		try { fs.setMtime(path, file.getStat().getMtime()); } catch(UnsupportedOperationException exc) {}
	}

	@Override
	public void rewind() throws IOException {
		channel.position(0);
	}
	
	@Override
	public int available() throws IOException {
		return (int) (channel.size() - channel.position());
	}

	@Override
	public boolean hasData() throws IOException {
		return channel.position() < channel.size();
	}
	
	protected void assertWritable() throws EACCESException {
		if((mode & File.O_WRONLY) == 0) throw new EACCESException(path);
	}
	
	protected void assertReadable() throws EACCESException {
		if((mode & File.O_RDONLY) == 0) throw new EACCESException(path);
	}
}
