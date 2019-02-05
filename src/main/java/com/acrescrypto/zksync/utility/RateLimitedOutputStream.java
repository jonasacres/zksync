package com.acrescrypto.zksync.utility;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.utility.BandwidthAllocator.BandwidthAllocation;

public class RateLimitedOutputStream extends OutputStream {
	private OutputStream output;
	private BandwidthAllocator allocator;
	private BandwidthAllocation allocation;
	private BandwidthMonitor monitor;
	protected final Logger logger = LoggerFactory.getLogger(RateLimitedOutputStream.class);
	
	public RateLimitedOutputStream(OutputStream output, BandwidthAllocator allocator) {
		this(output, allocator, new BandwidthMonitor(-1, -1));
	}
	
	public RateLimitedOutputStream(OutputStream output, BandwidthAllocator allocator, BandwidthMonitor parent) {
		this.output = output;
		this.allocator = allocator;
		this.allocation = allocator.requestAllocation();
		this.monitor = new BandwidthMonitor(parent.getSampleDurationMs(), parent.getSampleExpirationMs());
		if(parent != null) {
			this.monitor.addParent(parent);
		}
	}

	@Override
	public void close() throws IOException {
		output.close();
	}
	
	@Override
	public void flush() throws IOException {
		output.flush();
	}
	
	@Override
	public void write(int b) throws IOException {
		allocation.requestBytes(1); // guaranteed to return 1
		output.write(b);
		monitor.observeTraffic(1);
	}
	
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		int written = 0;
		while(written < len) {
			int writeLen = (int) allocation.requestBytes(len - written);
			logger.trace("RateLimitedOutputStream tx {} bytes, requested {}", writeLen, len);
			output.write(b,  off + written, writeLen);
			if(!allocator.isUnlimited()) flush(); // buffering weakens our control over bandwidth usage 
			monitor.observeTraffic(writeLen);
			written += writeLen;
		}
	}
	
	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	public BandwidthMonitor getMonitor() {
		return monitor;
	}

	public void setMonitor(BandwidthMonitor monitor) {
		this.monitor = monitor;
	}
}
