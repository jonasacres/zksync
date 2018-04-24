package com.acrescrypto.zksync.fs.sshfs;

import java.io.IOException;

import com.acrescrypto.zksync.exceptions.EACCESException;
import com.acrescrypto.zksync.exceptions.EISDIRException;
import com.acrescrypto.zksync.exceptions.EMLINKException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class SSHFile extends File {
	protected String path;
	protected int mode;
	protected long offset;
	
	protected Stat cachedStat;
	protected SSHFS sshfs;
	
	public SSHFile(SSHFS fs, String path, int mode) throws IOException {
		super(fs);
		this.sshfs = fs;
		this.path = path;
		this.mode = mode;
		
		try {
			if((mode & O_NOFOLLOW) != 0 && fs.lstat(path).isSymlink()) throw new EMLINKException(path);
			if(getStat().isDirectory()) throw new EISDIRException(path);
		} catch(ENOENTException exc) {
			if((mode & O_CREAT) == 0) throw exc;
			fs.execAndCheck("touch", "\"" + fs.qualifiedPath(path) + "\"");
			getStat();
		}
		
		if((mode & O_APPEND) != 0) offset = getStat().getSize();
		else if((mode & (O_TRUNC|O_WRONLY)) == (O_TRUNC|O_WRONLY)) truncate(0);
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
		if((mode & O_WRONLY) == 0) throw new EACCESException(path);
		fs.truncate(path, size);
		this.cachedStat = null;
	}
	
	@Override
	protected int _read(byte[] buf, int bufOffset, int maxLength) throws IOException {
		if((mode & O_RDONLY) == 0) throw new EACCESException(path);
		if(offset >= cachedStat.getSize()) return 0;
		byte[] data = sshfs.execAndCheck("dd",
				"ibs=1 count=" + maxLength + " skip=" + offset + " if=\"" + sshfs.qualifiedPath(path) + "\"");
		for(int i = 0; i < data.length; i++) buf[bufOffset+i] = data[i];
		offset += data.length;
		return data.length;
	}

	@Override
	public void write(byte[] data) throws IOException {
		if((mode & O_WRONLY) == 0) throw new EACCESException(path);
		sshfs.execAndCheck("dd",
				"obs=1 conv=notrunc seek=" + offset + " of=\"" + sshfs.qualifiedPath(path) + "\"",
				data);
		offset += data.length;
		if(offset > getStat().getSize()) getStat().setSize(offset);
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
		file.rewind();
		
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

	@Override
	public int available() throws IOException {
		return 0;
	}

}
