package com.acrescrypto.zksync.fs.remotefs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.exceptions.EACCESException;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.utility.Util;

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
	protected long shortTag;
	
	public PeerConnectionFile(PeerConnectionFS fs, String path, int mode) throws EACCESException {
		super(fs);
		this.path = path;
		this.mode = mode;
		this.pageBuf = ByteBuffer.allocate(fs.connection.getSocket().getSwarm().getArchive().getConfig().getPageSize());
		this.readBuf = ByteBuffer.wrap(pageBuf.array());
		this.shortTag = ByteBuffer.wrap(Util.hexToBytes(path.replace("/", ""))).getLong();
		if(this.mode != File.O_RDONLY) {
			throw new EACCESException(path); // TODO P2P: (review) right exception for "tried to open r/o file as writable?"
		}
		
		// TODO P2P: (refactor) This class needs to actually fill up pageBuf somewhere! Callback from swarm?
	}

	@Override
	public String getPath() {
		return path;
	}

	@Override
	public Stat getStat() throws IOException {
		if(stat != null) return stat;
		fs.connection.waitForReady();
		if(!fs.connection.hasFile(shortTag)) throw new ENOENTException(path);
		
		Stat stat = new Stat();
		stat.setGid(0);
		stat.setUid(0);
		stat.setMode(0444); // read-only for everyone
		stat.setType(Stat.TYPE_REGULAR_FILE);
		stat.setDevMinor(0);
		stat.setDevMajor(0);
		stat.setGroup("root");
		stat.setUser("root");
		stat.setAtime(0);
		stat.setCtime(0);
		stat.setMtime(0);
		stat.setSize(fs.connection.getSocket().getSwarm().getArchive().getConfig().getPageSize());
		stat.setInodeId(shortTag);
		this.stat = stat;
		
		return stat;
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
			// TODO P2P: (refactor) block until pageBuf has more bytes
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
