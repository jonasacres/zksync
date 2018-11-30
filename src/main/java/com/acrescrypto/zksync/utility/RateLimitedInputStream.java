package com.acrescrypto.zksync.utility;

import java.io.IOException;
import java.io.InputStream;

import com.acrescrypto.zksync.utility.BandwidthAllocator.BandwidthAllocation;

public class RateLimitedInputStream extends InputStream {
	private InputStream input;
	private BandwidthAllocator allocator;
	private BandwidthAllocation allocation;
	private BandwidthMonitor monitor;
	
	public RateLimitedInputStream(InputStream input, BandwidthAllocator allocator) {
		this(input, allocator, new BandwidthMonitor(-1, -1));
	}
	
	public RateLimitedInputStream(InputStream input, BandwidthAllocator allocator, BandwidthMonitor parent) {
		this.input = input;
		this.allocator = allocator;
		this.allocation = allocator.requestAllocation();
		this.monitor = new BandwidthMonitor(parent.getSampleDurationMs(), parent.getSampleExpirationMs());
		this.monitor.addParent(parent);
	}

	@Override
	public int read() throws IOException {
		allocation.requestBytes(1); // guaranteed to return 1
		monitor.observeTraffic(1);
		return input.read();
	}
	
	@Override
	public int read(byte[] buf) throws IOException {
		int readLen = (int) allocation.requestBytes(buf.length);
		return (int) monitor.observeTraffic(input.read(buf, 0, readLen));
	}
	
	@Override
	public int read(byte[] buf, int offset, int length) throws IOException {
		int readLen = (int) allocation.requestBytes(length);
		return (int) monitor.observeTraffic(input.read(buf, offset, readLen));
	}
	
	@Override
	public long skip(long numSkipped) throws IOException {
		int skipLen = (int) allocation.requestBytes(numSkipped);
		return monitor.observeTraffic(input.skip(skipLen));
	}
	
	@Override
	public int available() throws IOException {
		return (int) Math.min(input.available(),
				allocator.extraBytesAvailable() + allocation.getBytesRemaining());
	}
	
	@Override
	public void close() throws IOException {
		input.close();
	}
	
	@Override
	public void mark(int readlimit) {
		input.mark(readlimit);
	}
	
	@Override
	public void reset() throws IOException {
		input.reset();
	}
	
	@Override
	public boolean markSupported() {
		return input.markSupported();
	}

	public InputStream getInput() {
		return input;
	}

	public void setInput(InputStream input) {
		this.input = input;
	}

	public BandwidthMonitor getMonitor() {
		return monitor;
	}

	public void setMonitor(BandwidthMonitor monitor) {
		this.monitor = monitor;
	}
}
