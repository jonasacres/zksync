package com.acrescrypto.zksync.fs.compositefs;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;

public class CompositeReadOperation {
	public interface IncomingDataValidator {
		public boolean isValid(String path, InputStream data);
	}
	
	public interface IncomingStatValidator {
		public boolean isValid(String path, Stat stat);
	}
	
	public final static int MODE_DOWNLOAD = 0;
	public final static int MODE_STAT = 1;
	public final static int MODE_LSTAT = 2;

	protected CompositeFS composite;
	protected LinkedList<CompositeReadOperationWorker> workers = new LinkedList<CompositeReadOperationWorker>();
	protected HashMap<FS,Long> disqualified = new HashMap<FS,Long>();
	protected String path;
	protected Thread supervisorThread;
	protected Stat stat;
	protected IOException exception;
	protected IncomingDataValidator dataValidator;
	protected IncomingStatValidator statValidator;
	protected boolean finished, success;
	public int mode;
	protected int maxWorkers = 4;
	protected double switchoverThreshold = 0.8;
	protected long disqualificationInterval = 1000*60*5;
	
	public CompositeReadOperation(CompositeFS composite, String path, IncomingStatValidator statValidator, IncomingDataValidator dataValidator, int mode) {
		this.composite = composite;
		this.path = path;
		this.mode = mode;
		this.supervisorThread = (new Thread(()->superviseThread()));
		this.statValidator = statValidator;
		this.dataValidator = dataValidator;
		supervisorThread.start();
	}
	
	public Stat getStat() {
		return stat;
	}
	
	public CompositeReadOperation waitForStat() throws IOException {
		while(stat == null && !isFinished()) {
			try {
				synchronized(this) {
					this.wait(100);
				}
			} catch(InterruptedException exc) {
			}
		}
		
		if(exception != null) throw exception;
		if(stat == null) throw new ENOENTException(path);
		
		return this;
	}
	
	public CompositeReadOperation waitToFinish() throws IOException {
		while(!isFinished()) {
			try {
				synchronized(this) {
					this.wait(100);
				}
			} catch (InterruptedException e) {
			}
		}
		
		if(exception != null) throw exception;
		if(stat == null || !success) throw new ENOENTException(path);
		
		return this;
	}
	
	public boolean isFinished() {
		return finished;
	}

	protected void superviseThread() {
		while(!finished) {
			FS nextBest = selectFS();
			if(workers.isEmpty()) {
				if(nextBest == null) {
					finish(false);
					return;
				}

				makeWorker(nextBest);
			} else {
				long size = Stat.STAT_SIZE;
				if(stat != null) {
					size = stat.getSize();
				}

				// if our worker is at least a X% improvement on time, add it in
				if(nextBest.expectedReadWaitTime(size) < switchoverThreshold*eta()) {
					makeWorker(nextBest);
				}
			}

			synchronized(this) {
				try {
					this.wait(100);
				} catch (InterruptedException e) {}
			}
		}
	}

	protected synchronized void makeWorker(FS fs) {
		disqualifyFS(fs);
		pruneWorkers(maxWorkers-1);
		workers.add(new CompositeReadOperationWorker(this, fs));
	}

	protected synchronized FS selectFS() {
		pruneDisqualified();
		long bestTime = Long.MAX_VALUE;
		FS bestFs = null;
		long size = readSize(), median = medianEta();

		for(FS fs : composite.getSupplementaries()) {
			if(disqualified.containsKey(fs)) continue;
			long time = fs.expectedReadWaitTime(size);
			if(time < 0) {
				time = median;
			}
			
			if(bestFs == null || time < bestTime) {
				bestFs = fs;
				bestTime = time;
			}
		}

		return bestFs;
	}

	protected synchronized void pruneWorkers(int max) {
		assert(max > 0);
		while(workers.size() > max) {
			pruneSlowestWorker();
		}
	}

	protected synchronized void pruneSlowestWorker() {
		if(workers.isEmpty()) return;
		long worstEta = -1, median = medianEta();
		CompositeReadOperationWorker worstWorker = null;

		for(CompositeReadOperationWorker worker : workers) {
			long eta = worker.eta();
			
			if(eta < 0) {
				eta = median;
			}
			
			if(worstWorker == null || eta > worstEta) {
				worstEta = eta;
				worstWorker = worker;
			}
		}

		if(worstWorker != null) {
			worstWorker.abort();
			workers.remove(worstWorker);
		}
	}

	protected synchronized void pruneDisqualified() {
		HashMap<FS,Long> newDisqualified = new HashMap<FS,Long>();
		long now = System.currentTimeMillis();
		for(FS fs : disqualified.keySet()) {
			long deadline = disqualified.get(fs);
			if(deadline > now)  {
				newDisqualified.put(fs, deadline);
			}
		}

		this.disqualified = newDisqualified;
	}

	protected synchronized long eta() {
		long bestTime = Long.MAX_VALUE;
		boolean allowUntested = false;
		for(CompositeReadOperationWorker worker : workers) {
			long time = worker.eta();
			allowUntested = true;
			bestTime = Math.min(time, bestTime);
		}

		if(allowUntested) {
			long median = medianEta();
			if(median >= 0) {
				bestTime = Math.min(bestTime, median);
			}
		}

		return bestTime;
	}

	protected synchronized long medianEta() {
		long size = readSize();
		LinkedList<Long> times = new LinkedList<Long>();

		for(FS fs : composite.getSupplementaries()) {
			long time = fs.expectedReadWaitTime(size);
			if(time < 0) continue;
			times.add(time);
		}

		if(times.isEmpty()) return -1;
		Collections.sort(times, (t0, t1)->Long.compare(t0, t1));
		return times.get(times.size()/2);
	}

	protected long readSize() {
		return stat == null ? Stat.STAT_SIZE : stat.getSize();
	}

	protected void disqualifyFS(FS fs) {
		disqualified.put(fs, System.currentTimeMillis());
	}

	protected synchronized void finishedWorker(CompositeReadOperationWorker worker) {
		workers.remove(worker);
		this.notifyAll();
	}
	
	protected synchronized void receivedStat(CompositeReadOperationWorker worker) {
		this.stat = worker.stat;
		
		if(!statValidator.isValid(path, stat)) {
			/** Sent us an illegal stat, e.g. way too big for a zkfs page file. Blacklist that peer! */
			composite.failSupplementaryFS(worker.fs, true);
			return;
		}
		
		if(mode != MODE_DOWNLOAD) {
			finish(true);
			return;
		}
		
		try {
			if(stat.isRegularFile()) return;
			else if(stat.isDirectory()) makeDirectory();
			else {
				/** SUBTLE: Sending us anything other than a file or directory in this version is a sign of evil, because
				 * we just don't have cause to do that right now. CompositeFS is only used to sync archives, which consist
				 * solely of regular files and directories.
				 */
				composite.failSupplementaryFS(worker.fs, true);
			};
		} catch(IOException exc) {
			finishException(exc);
		}
	}
	
	protected void makeDirectory() throws IOException {
		composite.backingFS.mkdirp(path);
		composite.backingFS.applyStat(path, stat);
		finish(true);
	}

	protected synchronized void receivedData(CompositeReadOperationWorker worker) {
		try {
			InputStream stream = worker.buf.getInputStream();
			if(!dataValidator.isValid(path, stream)) {
				composite.failSupplementaryFS(worker.fs, true);
				return;
			}

			if(worker.stat.isRegularFile()) {
				File outFile = composite.backingFS.open(path, File.O_WRONLY|File.O_CREAT|File.O_TRUNC);
				do {
					byte[] readBuf = new byte[1024];
					int r = stream.read(readBuf);
					if(r <= 0) break;
					outFile.write(readBuf);
				} while(true);
				
				composite.backingFS.applyStat(path, worker.stat);
				worker.buf.delete();
				finish(true);
			} else {
				// file type changed between when we got the stat and now
				receivedStat(worker);
				finish(false); // already should be called in receivedStat, but better safe than sorry
			}
		} catch (IOException exc) {
			finishException(exc);
		}
	}
	
	protected synchronized void finishException(IOException exc) {
		this.exception = exc;
		finish(false);
	}

	protected synchronized void finish(boolean success) {
		if(finished) return;
		this.finished = true;
		this.success = success;
		for(CompositeReadOperationWorker worker : workers) {
			worker.abort();
		}
		this.notifyAll();
	}
}
