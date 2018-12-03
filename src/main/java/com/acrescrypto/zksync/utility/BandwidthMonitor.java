package com.acrescrypto.zksync.utility;

import java.util.LinkedList;

public class BandwidthMonitor {
	private int sampleDurationMs;
	private int sampleExpirationMs;
	private long lastRecalculationTime;
	private long currentBytesInInterval;
	private long startTime;
	private double currentRateBytesPerSecond;
	
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
	
	public long observeTraffic(long bytes) {
		if(bytes <= 0) return bytes; // passthrough without doing anything
		if(sampleDurationMs == -1 && sampleExpirationMs == -1) return bytes;
		LinkedList<BandwidthMonitor> parentClone;
		synchronized(this) {
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
		long totalSeen = 0, oldestTimestamp = Math.max(startTime, Util.currentTimeMillis()-sampleExpirationMs);
		
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
		currentRateBytesPerSecond = (1.0 * totalSeen) / (sampleInterval / 1000.0);
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
	
	public double getBytesPerSecond() {
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
}
