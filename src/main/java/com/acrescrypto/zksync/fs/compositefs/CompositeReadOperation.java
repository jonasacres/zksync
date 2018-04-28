package com.acrescrypto.zksync.fs.compositefs;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.Stat;

public class CompositeReadOperation {
	public interface IncomingDataValidator {
		public boolean isValid(byte[] data);
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
	protected IncomingDataValidator validator;
	protected byte[] data;
	protected boolean finished;
	public int mode;
	protected int maxWorkers = 4;
	protected double switchoverThreshold = 0.8;
	protected long disqualificationInterval = 1000*60*5;
	
	public CompositeReadOperation(CompositeFS composite, String path, IncomingDataValidator validator, int mode) {
		this.composite = composite;
		this.path = path;
		this.mode = mode;
		this.supervisorThread = (new Thread(()->superviseThread()));
		this.validator = validator;
		supervisorThread.start();
	}
	
	public Stat getStat() {
		return stat;
	}
	
	public CompositeReadOperation waitForStat() {
		while(stat == null && !isFinished()) {
			try {
				this.wait(100);
			} catch(InterruptedException exc) {
			}
		}
		
		return this;
	}
	
	public CompositeReadOperation waitToFinish() {
		while(!isFinished()) {
			try {
				this.wait(100);
			} catch (InterruptedException e) {
			}
		}
		
		return this;
	}
	
	public boolean isFinished() {
		return finished;
	}

	public byte[] getData() {
		return data;
	}

	protected void superviseThread() {
		while(!finished) {
			FS nextBest = selectFS();
			if(workers.isEmpty()) {
				if(nextBest == null) {
					finish(null);
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
		// TODO P2P: prune disqualified
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

	protected void failFS(FS fs) {
		// TODO P2P: disconnect
	}

	protected synchronized void finishedWorker(CompositeReadOperationWorker worker) {
		workers.remove(worker);
		this.notifyAll();
	}
	
	protected synchronized void receivedStat(Stat stat) {
		this.stat = stat;
		if(mode != MODE_DOWNLOAD) {
			finish(null);
			return;
		}
		
		try {
			if(stat.isRegularFile()) return;
			else if(stat.isDirectory()) makeDirectory();
			else finish(null);
		} catch(IOException exc) {
			finish(null); // TODO P2P: do we need some stronger sort of failure for when we obtain the file, but the backingfs is hosed?
		}
	}
	
	protected void makeDirectory() throws IOException {
		composite.backingFS.mkdirp(path);
		composite.backingFS.applyStat(path, stat);
		finish(null); // TODO P2P: is this distinguishable from failure? i guess stat is set...
	}

	protected synchronized void receivedData(FS fs, byte[] data) {
		if(!validator.isValid(data)) {
			failFS(fs);
			return;
		}
		
		try {
			if(stat.isRegularFile()) {
				composite.backingFS.safeWrite(path, data);
				composite.backingFS.applyStat(path, stat);
			} else {
				// TODO P2P: shouldn't happen, but what if it does?
			}
		} catch (IOException e) {
			// TODO P2P: again we need to decide what to do if backingfs fails...
		}

		finish(data);
	}

	protected synchronized void finish(byte[] data) {
		if(finished) return;
		this.data = data;
		this.finished = true;
		for(CompositeReadOperationWorker worker : workers) {
			worker.abort();
		}
		this.notifyAll();
	}
}
