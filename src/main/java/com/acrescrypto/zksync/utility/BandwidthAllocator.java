package com.acrescrypto.zksync.utility;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableInt;

public class BandwidthAllocator {
	public static long DEFAULT_ALLOCATION_INTERVAL_MS = 100;
	
	private long allocationIntervalMs;
	private long allocationPool;
	private long reallocationTime;
	private double bytesPerSecond;
	private boolean reallocating;
	private Map<BandwidthAllocation,Long> pendingAllocations = new HashMap<>();
	
	public class BandwidthAllocation {
		long expirationTime;
		long bytesRemaining;
		
		public BandwidthAllocation() {}
		
		public long requestBytes(long requestSize) {
			if(bytesRemaining <= 0 || System.currentTimeMillis() >= expirationTime) {
				renew(requestSize);
			}
			
			long allowed = Math.min(bytesRemaining, requestSize);
			if(allowed < requestSize) {
				allowed += requestExtra(requestSize - allowed);
			}
			
			bytesRemaining -= allowed;
			return bytesRemaining;
		}
		
		public void renew(long requested) {
			renewAllocation(this, requested);
		}
		
		public void relinquish() {
			if(bytesRemaining <= 0) return;
			relinquishAllocation(this);
		}
	}
	
	public BandwidthAllocator(double bytesPerSecond, long allocationIntervalMs) {
		this.allocationIntervalMs = allocationIntervalMs;
		this.bytesPerSecond = bytesPerSecond;
	}
	
	public BandwidthAllocator(double bytesPerSecond) {
		this(bytesPerSecond, DEFAULT_ALLOCATION_INTERVAL_MS);
	}
	
	public BandwidthAllocation requestAllocation() {
		return new BandwidthAllocation();
	}
	
	protected void renewAllocation(BandwidthAllocation allocation, long bytesRequested) {
		boolean willReallocate = false;
		synchronized(this) {
			pendingAllocations.put(allocation, bytesRequested);
			allocation.bytesRemaining = 0;
			if(reallocating) {
				try {
					this.wait(); // woken after we call reallocate()
				} catch (InterruptedException e) {}
			} else {
				// first thread to renew their allocation is responsible for calling reallocate()
				willReallocate = true;
			}
		}
		
		if(willReallocate) {
			Util.sleep(reallocationTime - System.currentTimeMillis());
			reallocate();
		}
	}
	
	protected boolean hasExtra() {
		return allocationPool > 0;
	}
	
	protected synchronized long requestExtra(long bytesRequested) {
		long allocation = Math.min(allocationPool, bytesRequested);
		allocationPool -= allocation;
		return allocation;
	}
	
	protected synchronized void relinquishAllocation(BandwidthAllocation allocation) {
		allocationPool += allocation.bytesRemaining;
	}
	
	protected synchronized void reallocate() {
		allocationPool = (long) (bytesPerSecond/1000.0 * allocationIntervalMs);
		MutableInt remaining = new MutableInt(pendingAllocations.size());
		reallocationTime = System.currentTimeMillis() + allocationIntervalMs;
		
		while(allocationPool > 0 && remaining.intValue() > 0) {
			long averagePortion = allocationPool / remaining.intValue();
			pendingAllocations.forEach((allocation, bytesRequested)->{
				if(allocation.bytesRemaining >= bytesRequested) return;
				long bytesAllocated = Math.min(bytesRequested - allocation.bytesRemaining, averagePortion);
				allocationPool -= bytesAllocated;
				allocation.bytesRemaining += bytesAllocated;
				allocation.expirationTime = reallocationTime;
				if(allocation.bytesRemaining >= bytesRequested) {
					remaining.decrement();
				}
			});
		}
		
		reallocating = false;
		this.notifyAll();
	}
	
	public long getAllocationIntervalMs() {
		return allocationIntervalMs;
	}
	
	public double getBytesPerSecond() {
		return bytesPerSecond;
	}
}
