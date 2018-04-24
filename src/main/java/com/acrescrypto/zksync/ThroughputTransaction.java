package com.acrescrypto.zksync;

public class ThroughputTransaction {
	protected long startTime, endTime;
	protected long bytes;
	protected ThroughputMeter meter;
	
	public ThroughputTransaction(ThroughputMeter meter) {
		this.startTime = System.currentTimeMillis();
		this.meter = meter;
	}
	
	public void finish(long bytes) {
		this.endTime = System.currentTimeMillis();
		this.bytes = bytes;
		meter.finish(this);
	}
}
