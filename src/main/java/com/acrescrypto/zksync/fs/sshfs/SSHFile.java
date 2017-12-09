package com.acrescrypto.zksync.fs.sshfs;

import java.io.IOException;

import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class SSHFile extends File {

	@Override
	public String getPath() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Stat getStat() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void truncate(long size) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int read(byte[] buf, int offset, int maxLength) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void write(byte[] data) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long seek(long pos, int mode) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void copy(File file) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void rewind() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean hasData() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}
