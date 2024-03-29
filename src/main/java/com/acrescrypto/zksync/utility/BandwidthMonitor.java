package com.acrescrypto.zksync.utility;

import java.util.LinkedList;

public class BandwidthMonitor {
	private int sampleDurationMs;
	private int sampleExpirationMs;
	private long lastRecalculationTime;
	private long currentBytesInInterval;
	private long startTime;
	private long currentRateBytesPerSecond;
	private long lifetimeBytes;
	
	private LinkedList<BandwidthMonitor> parents = new LinkedList<>();
	private LinkedList<Sample> samples = new LinkedList<>();
	private Sample currentSample;
	
	class Sample {
		long timeStart, finishTime, expirationTime;
		long bytesSeen;
		
		public Sample() {
			this.timeStart = Util.currentTimeMillis();
			this.finishTime = Util.currentTimeMillis() + sampleDurationMs;
			this.expirationTime = Util.currentTimeMillis() + sampleExpirationMs;
			this.bytesSeen = 0;
		}
		
		public boolean isFinished() {
			return Util.currentTimeMillis() >= finishTime;
		}
		
		public boolean isExpired() {
			return Util.currentTimeMillis() >= expirationTime;
		}
		
		public void addObservation(long bytes) {
			this.bytesSeen += bytes;
		}
	}
	
	protected BandwidthMonitor() {}
	
	public BandwidthMonitor(int sampleDurationMs, int sampleExpirationMs) {
		this.sampleDurationMs = sampleDurationMs;
		this.sampleExpirationMs = sampleExpirationMs;
		this.startTime = Util.currentTimeMillis();
	}
	
	public BandwidthMonitor(BandwidthMonitor parent) {
		this.sampleDurationMs = parent.sampleDurationMs;
		this.sampleExpirationMs = parent.sampleExpirationMs;
		this.startTime = Util.currentTimeMillis();
		this.addParent(parent);
	}
	
	public void clear() {
		this.samples.clear();
		this.currentSample = null;
		this.lastRecalculationTime = 0;
	}

	public long observeTraffic(long bytes) {
		if(bytes <= 0) return bytes; // passthrough without doing anything
		LinkedList<BandwidthMonitor> parentClone;
		synchronized(this) {
			lifetimeBytes += bytes;
			if(sampleDurationMs == -1 && sampleExpirationMs == -1) return bytes;

			parentClone = new LinkedList<>(parents);
			if(currentSample == null || currentSample.isFinished()) {
				currentSample = new Sample();
				samples.add(currentSample);
			}
			
			currentSample.addObservation(bytes);
		}
		
		for(BandwidthMonitor parent : parentClone) {
			parent.observeTraffic(bytes);
		}
		
		return bytes;
	}
	
	protected synchronized void recalculate() {
		long totalSeen       = 0,
			 oldestTimestamp = Math.max(
						startTime,
						Util.currentTimeMillis() - sampleExpirationMs
					);
		
		samples.removeIf((sample)->sample.isExpired());
		if(currentSample != null && currentSample.isExpired()) {
			currentSample = null;
		}

		if(currentSample == null) {
			currentRateBytesPerSecond = 0;
			return;
		}

		
		for(Sample sample : samples) {
			totalSeen += sample.bytesSeen;
			oldestTimestamp = Math.min(oldestTimestamp, sample.timeStart);
		}
		
		long sampleInterval = Util.currentTimeMillis() - oldestTimestamp;
		currentRateBytesPerSecond = (long) ((1.0 * totalSeen) / (sampleInterval / 1000.0));
		currentBytesInInterval = totalSeen;
		lastRecalculationTime = Util.currentTimeMillis();
	}
	
	protected void checkCalculation() {
		if(Util.currentTimeMillis() > lastRecalculationTime) {
			recalculate();
		}
	}
	
	public synchronized void addParent(BandwidthMonitor parent) {
		parents.add(parent);
	}
	
	public synchronized void removeParent(BandwidthMonitor parent) {
		parents.remove(parent);
	}
	
	public long getBytesPerSecond() {
		checkCalculation();
		return currentRateBytesPerSecond;
	}
	
	public long getBytesInInterval() {
		checkCalculation();
		return currentBytesInInterval;
	}
	
	public int getSampleDurationMs() {
		return sampleDurationMs;
	}
	
	public int getSampleExpirationMs() {
		return sampleExpirationMs;
	}
	
	public long getLifetimeBytes() {
		return lifetimeBytes;
	}
	
	public String toString() {
		double rate = getBytesPerSecond();
		int order = (int) Math.floor(Math.log(rate)/Math.log(1024));
		String unit = "B/s";
		
		if(order >= 4) {
			rate /= 1024*1024*1024*1024;
			unit = "TiB/s";
		} else if(order == 3) {
			rate /= 1024*1024*1024;
			unit = "GiB/s";
		} else if(order == 2) {
			rate /= 1024*1024;
			unit = "MiB/s";
		} else if(order == 1) {
			rate /= 1024;
			unit = "KiB/s";
		}
		
		return String.format("%.02f %s", rate, unit);
	}
}
