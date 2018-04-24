package com.acrescrypto.zksync;

import java.util.ArrayList;
import java.util.function.Predicate;

public class ThroughputMeter {
	protected ArrayList<ThroughputTransaction> transactions = new ArrayList<ThroughputTransaction>();
	protected ArrayList<ThroughputTransaction> pending = new ArrayList<ThroughputTransaction>();
	protected long pruneInterval = 1000*60*5;
	protected long bytesPerSecond = -1;
	
	public ThroughputMeter() {
	}
	
	public ThroughputTransaction beginTransaction() {
		ThroughputTransaction newTx = new ThroughputTransaction(this);
		pending.add(newTx);
		return newTx;
	}
	
	/** -1 if we haven't measured yet. Else, last recorded average bytes per second. */
	public long getBytesPerSecond() {
		return bytesPerSecond;
	}
	
	protected synchronized void finish(ThroughputTransaction tx) {
		assert(pending.remove(tx));
		if(tx.bytes > 0) transactions.add(tx);
		prune();
		recalculate();
	}
	
	protected Predicate<ThroughputTransaction> isExpired() {
		return isLapsed(System.currentTimeMillis() - pruneInterval); 
	}
	
	protected Predicate<ThroughputTransaction> isLapsed(long currentTime) {
		return tx -> tx.endTime <= currentTime; 
	}
	
	protected void prune() {
		transactions.removeIf(isExpired());
	}
	
	protected void recalculate() {
		if(transactions.isEmpty()) return; // leave old result, if any, in place
		long intervalEnd = transactions.get(0).endTime;
		long deadTime = 0, bytes = 0;
		
		for(ThroughputTransaction tx : transactions) {
			if(tx.startTime > intervalEnd) {
				deadTime += tx.startTime - intervalEnd;
			}
			
			if(tx.endTime > intervalEnd) intervalEnd = tx.endTime;
			bytes += tx.bytes;
		}
		
		long milliseconds = Math.max(1, (intervalEnd - transactions.get(0).startTime - deadTime));
		this.bytesPerSecond = 1000*bytes / milliseconds;
	}
}
