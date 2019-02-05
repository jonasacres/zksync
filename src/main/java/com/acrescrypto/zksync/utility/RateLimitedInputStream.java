package com.acrescrypto.zksync.utility;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.utility.BandwidthAllocator.BandwidthAllocation;

public class RateLimitedInputStream extends InputStream {
	private InputStream input;
	private BandwidthAllocator allocator;
	private BandwidthAllocation allocation;
	private BandwidthMonitor monitor;
	protected final Logger logger = LoggerFactory.getLogger(RateLimitedInputStream.class);
	
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
		return read(buf, 0, buf.length);
	}
	
	@Override
	public int read(byte[] buf, int offset, int length) throws IOException {
		int readLen = (int) allocation.requestBytes(length);
		logger.trace("RateLimitedInputStream rx {} bytes, requested {}", readLen, length);
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
