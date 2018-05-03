package com.acrescrypto.zksync.fs.remotefs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.exceptions.EACCESException;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

/** Handles fetching a file from a remote peer. The only supported operation is fetching a page file in a read-only,
 * sequential operation covering the entire file.
 */
public class PeerConnectionFile extends File {
	protected PeerConnectionFS fs;
	protected String path;
	protected int mode;
	protected Stat stat;
	protected boolean doesntExist;
	protected ByteBuffer pageBuf, readBuf;
	
	public PeerConnectionFile(PeerConnectionFS fs, String path, int mode) throws EINVALException {
		super(fs);
		this.path = path;
		this.mode = mode;
		this.pageBuf = ByteBuffer.allocate(fs.connection.getSocket().getSwarm().getArchive().getConfig().getPageSize());
		this.readBuf = ByteBuffer.wrap(pageBuf.array());
		if(this.mode != File.O_RDONLY) {
			throw new EINVALException(path);
		}
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public Stat getStat() throws IOException {
		if(stat != null) return stat;
		// TODO P2P: verify existence of file; construct fake stat once obtained
		return null;
	}

	@Override
	public void truncate(long size) throws IOException {
		throw new EACCESException(path);
	}

	@Override
	protected int _read(byte[] buf, int offset, int maxLength) throws IOException {
		if(doesntExist) throw new ENOENTException(path);
		if(readBuf.position() == pageBuf.position()) {
			if(!pageBuf.hasRemaining()) return -1;
			// TODO: block until pageBuf has more bytes
		}
		
		int readLength = Math.min(maxLength, pageBuf.position() - readBuf.position());
		readBuf.get(readLength);
		return readLength;
	}

	@Override
	public void write(byte[] data) throws IOException {
		throw new EACCESException(path);
	}

	@Override
	public long seek(long pos, int mode) throws IOException {
		long newOffset;
		if(mode == SEEK_SET) {
			newOffset = pos;
		} else if(mode == SEEK_CUR) {
			newOffset = readBuf.position() + pos;
		} else if(mode == SEEK_END) {
			newOffset = pageBuf.capacity() + pos;
		} else {
			throw new EINVALException(path);
		}
		
		if(newOffset < 0 || newOffset >= pageBuf.capacity()) {
			throw new EINVALException(path);
		}
		
		readBuf.position((int) newOffset);
		return newOffset;
	}

	@Override
	public void flush() throws IOException {
	}

	@Override
	public void close() throws IOException {
		// TODO P2P: Auto-generated method stub
		
	}

	@Override
	public void copy(File file) throws IOException {
		throw new EACCESException(path);
	}

	@Override
	public void rewind() throws IOException {
		readBuf.rewind();
	}

	@Override
	public boolean hasData() throws IOException {
		return readBuf.position() < pageBuf.position();
	}

	@Override
	public int available() throws IOException {
		return pageBuf.position() - readBuf.position();
	}

}
