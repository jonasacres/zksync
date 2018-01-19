package com.acrescrypto.zksync.fs.sshfs;

import java.io.IOException;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class SSHFile extends File {
	protected SSHFS fs;
	protected String path;
	protected int mode;
	protected long offset;
	
	protected Stat cachedStat;
	
	public SSHFile(SSHFS fs, String path, int mode) throws IOException {
		this.fs = fs;
		this.path = path;
		this.mode = mode;
		
		try {
			fs.stat(path);
		} catch(ENOENTException exc) {
			if((mode & O_CREAT) == 0) throw exc;
			fs.execAndCheck("touch", "\"" + fs.qualifiedPath(path) + "\"");
		}
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public Stat getStat() throws IOException {
		if(cachedStat != null) return cachedStat;
		return cachedStat = fs.stat(path);
	}

	@Override
	public void truncate(long size) throws IOException {
		fs.truncate(path, size);
	}
	
	@Override
	public int read(byte[] buf, int offset, int maxLength) throws IOException {
		byte[] data = fs.execAndCheck("dd",
				"ibs=1 count=" + maxLength + " skip=" + offset + " if=\"" + fs.qualifiedPath(path) + "\"");
		for(int i = 0; i < data.length; i++) buf[i] = data[i];
		return data.length;
	}

	@Override
	public void write(byte[] data) throws IOException {
		fs.execAndCheck("dd",
				"obs=1 conv=notrunc seek=" + offset + " of=\"" + fs.qualifiedPath(path) + "\"",
				data);
		offset += data.length;
	}

	@Override
	public long seek(long pos, int mode) throws IOException {
		switch(mode) {
		case SEEK_SET:
			offset = pos;
			break;
		case SEEK_CUR:
			offset += pos;
			break;
		case SEEK_END:
			offset = getStat().getSize() + pos;
			break;
		}
		
		return offset;
	}

	@Override
	public void flush() throws IOException {
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public void copy(File file) throws IOException {
		truncate(0);
		seek(0, SEEK_SET);
		
		while(true) {
			byte[] data = file.read(64*1024);
			if(data.length == 0) return;
			write(data);
		}
	}

	@Override
	public void rewind() throws IOException {
		offset = 0;
	}

	@Override
	public boolean hasData() throws IOException {
		return offset < cachedStat.getSize();
	}

}
