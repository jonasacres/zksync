package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.compositefs.CompositeFS;

public class CompositeReadOperation {
	public interface PeerOperationTask {
		public boolean download(Stat stat, FS fs);
	}

	public interface PeerOperationCallback {
		public void finished(Stat finalStat, FS finalFs);
	}

	protected CompositeFS composite;
	protected String path;
	protected PeerOperationTask task; // TODO: when to set these?
	protected PeerOperationCallback callback;

	protected HashSet<FS> visited = new HashSet<FS>();
	protected HashMap<Stat,Collection<FS>> qualified = new HashMap<Stat,Collection<FS>>(); // FSes that have successfully stat'ed the file
	protected int numStatThreads, numDownloadThreads;
	protected int qualifiedThreshold = 3; // how big should the qualified pool be while we download? helps failover recovery
	protected int simultaneousDownloads = 1; // how many peers we will try to fetch the same file from at once
	protected boolean finished;
	
	/** TODO: This still sucks.
	 * 
	 * It always selects untested peers, and offers no means to abort a peer that is taking too long.
	 * This is mitigated by the fact that pages are 64k by default, but what about larger pages? And what about peers
	 * that sit there and do nothing? Keep an estimate of average performance of untested peers. Then, any peer in a
	 * download thread can be tested periodically to see if it is exceeding the expected performance of other peers by
	 * a safe margin. If not, another peer can be spun up -- and only if it is superior, does it replace the initial
	 * download.
	 * 
	 * Also randomly accept them with a m/q chance, where m is the number of qualified peers we have no estimate for,
	 * and q is the total number of qualified peers.
	 */

	public CompositeReadOperation(CompositeFS composite, String path) {
		this.composite = composite;
		this.path = path;
		new Thread(()->statSupervisorThread());
		new Thread(()->downloadSupervisorThread());
	}

	protected void statSupervisorThread() {
		while(canContinue()) {
			waitForCondition(qualified.size() + numStatThreads < qualifiedThreshold || !canContinue());
			statNextFS();
		}

		synchronized(this) {
			fail(); // no-op if we already finished
			this.notifyAll();
		}
	}

	protected synchronized void statNextFS() {
		FS best = bestStatCandidate();
		if(best == null) return;
		visited.add(best);
		runStatThread(best);
		numStatThreads++;
	}

	protected FS bestStatCandidate() {
		FS bestFS = null;
		long bestTime = Long.MAX_VALUE;

		for(FS fs : composite.getSupplementaries()) {
			if(visited.contains(fs)) continue;
			long time = fs.expectedReadWaitTime(Stat.STAT_SIZE);
			if(bestFS == null || time < bestTime) {
				bestFS = fs;
				bestTime = time;
			}
		}

		return bestFS;
	}
	
	protected void runStatThread(FS fs) {
		new Thread(()-> {
			try {
				Stat stat = fs.stat(path);
				qualified.putIfAbsent(stat, new HashSet<FS>());
				qualified.get(stat).add(fs);
			} catch(ENOENTException exc) {
			} catch (IOException exc) {
				// TODO: notify someone that this peer is throwing IOExceptions so we can remove from peer list
			}

			synchronized(this) {
				numStatThreads--;
				this.notifyAll();
			}
		}).start();
	}

	protected void downloadSupervisorThread() {
		while(canContinue()) {
			waitForCondition(numDownloadThreads < simultaneousDownloads || !canContinue());
			downloadNextCandidate();
		}
		
		synchronized(this) {
			fail(); // no-op if we already finished
			this.notifyAll();
		}
	}
	
	protected synchronized void downloadNextCandidate() {
		Stat bestStat = null;
		FS bestFs = null;
		long bestTime = Long.MAX_VALUE;
		
		for(Stat stat : qualified.keySet()) {
			for(FS fs : qualified.get(stat)) {
				long time = fs.expectedReadWaitTime(stat.getSize());
				if(bestFs == null || time < bestTime) {
					bestStat = stat;
					bestFs = fs;
					bestTime = time;
				}
			}
		}
		
		if(bestFs != null) {
			qualified.get(bestStat).remove(bestFs);
			numDownloadThreads++;
			runDownloadThread(bestStat, bestFs);
		}
	}
	
	protected void runDownloadThread(Stat stat, FS fs) {
		new Thread(() -> {
			if(task.download(stat, fs)) {
				this.finish(stat,  fs);
			}
			
			synchronized(this) {
				numDownloadThreads--;
				this.notifyAll();
			}
		}).start();
	}

	protected synchronized boolean canContinue() {
		if(finished) return false;
		if(numStatThreads > 0 || numDownloadThreads > 0) return true;
		for(FS fs : composite.getSupplementaries()) {
			if(!visited.contains(fs) || isQualified(fs)) return true;
		}

		return false;
	}

	protected boolean isQualified(FS fs) {
		for(Collection<FS> list : qualified.values()) {
			if(list.contains(fs)) return true;
		}

		return false;
	}

	protected synchronized void waitForCondition(boolean condition) {
		while(!condition) {
			try {
				this.wait();
			} catch(InterruptedException exc) {}
		}
	}

	protected synchronized void finish(Stat stat, FS fs) {
		if(finished) return;
		finished = true;
		callback.finished(null, null);
	}

	protected synchronized void fail() {
		finish(null, null);
	}
}
