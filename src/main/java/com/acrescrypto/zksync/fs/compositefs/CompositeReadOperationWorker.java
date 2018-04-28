package com.acrescrypto.zksync.fs.compositefs;

import java.io.IOException;

import com.acrescrypto.zksync.StorableBuffer;
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
	
	protected StorableBuffer buf;

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
				op.receivedStat(this);
				break;
			case CompositeReadOperation.MODE_STAT:
				stat = fs.stat(op.path);
				op.receivedStat(this);
				return;
			case CompositeReadOperation.MODE_LSTAT:	
				stat = fs.lstat(op.path);
				op.receivedStat(this);
				return;
			}
			
			buf = StorableBuffer.scratchBuffer(1024);
			this.startTime = System.currentTimeMillis();
			while(buf.getLength() < stat.getSize() && !aborted) {
				byte[] chunk = file.read(1024);
				if(chunk.length == 0) break;
				synchronized(this) {
					buf.put(chunk);
				}
			}

			if(buf.getLength() == stat.getSize()) {
				op.receivedData(this);
			} else {
				op.composite.failSupplementaryFS(fs, false); // FS didn't give us the right number of bytes for some reason
			}
		} catch(ENOENTException exc) {
			op.disqualifyFS(fs);
		} catch(IOException exc) {
			op.composite.failSupplementaryFS(fs, false);
		} finally {
			try {
				if(file != null) file.close();
			} catch(IOException exc) {}
		}
	}

	public void abort() {
		aborted = true;
	}

	public synchronized long eta() {
		if(buf == null || buf.getLength() == 0) return fs.expectedReadWaitTime(op.readSize());
		double elapsedMillis = System.currentTimeMillis() - startTime;
		double rxBytes = buf.getLength();
		double remainingBytes = op.readSize() - rxBytes;
		return (long) Math.ceil(elapsedMillis/rxBytes * remainingBytes);
	}
}
