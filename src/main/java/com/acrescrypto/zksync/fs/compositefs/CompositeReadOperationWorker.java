package com.acrescrypto.zksync.fs.compositefs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class CompositeReadOperationWorker {
	protected CompositeReadOperation op;
	protected FS fs;
	protected File file;
	protected Stat stat;
	protected Thread thread;
	protected long startTime;
	protected boolean aborted;
	
	/** TODO P2P: The whole file is cached into memory -- potentially multiple times. Not bad at 64k, but what about 4gb? */
	protected ByteBuffer buf;

	public CompositeReadOperationWorker(CompositeReadOperation op, FS fs) {
		this.op = op;
		this.fs = fs;
		this.thread = new Thread(()->workerThread());
		
		thread.start();
	}

	protected void workerThread() {
		try {
			switch(op.mode) {
			case CompositeReadOperation.MODE_DOWNLOAD:
				file = fs.open(op.path, File.O_RDONLY);
				stat = file.getStat();
				op.receivedStat(stat);
				break;
			case CompositeReadOperation.MODE_STAT:
				stat = fs.stat(op.path);
				op.receivedStat(stat);
				return;
			case CompositeReadOperation.MODE_LSTAT:	
				stat = fs.lstat(op.path);
				op.receivedStat(stat);
				return;
			}
			
			buf = ByteBuffer.allocate((int) stat.getSize());
			this.startTime = System.currentTimeMillis();
			InputStream stream = file.getInputStream();
			while(buf.hasRemaining() && !aborted) {
				if(stream.available() <= 0) { // TODO P2P: ensure all input streams will always buffer at least some data in advance
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
					}
				}

				int r = stream.read(buf.array(), buf.position(), Math.min(stream.available(), buf.remaining()));
				buf.position(buf.position() + r);
			}

			if(!buf.hasRemaining()) {
				op.receivedData(fs, buf.array());
			}
		} catch(ENOENTException exc) {
			op.disqualifyFS(fs);
		} catch(IOException exc) {
			op.failFS(fs);
		} finally {
			try {
				if(file != null) file.close();
			} catch(IOException exc) {}
		}
	}

	public void abort() {
		aborted = true;
	}

	public long eta() {
		if(buf == null || buf.position() == 0) return fs.expectedReadWaitTime(op.readSize());
		double elapsedMillis = System.currentTimeMillis() - startTime;
		double rxBytes = buf.position();
		double remainingBytes = op.readSize() - rxBytes;
		return (long) Math.ceil(elapsedMillis/rxBytes * remainingBytes);
	}
}
