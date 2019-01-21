package com.acrescrypto.zksync.utility;

import java.util.LinkedList;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

public class MemLogAppender extends AppenderBase<ILoggingEvent> {
	private static MemLogAppender sharedMemlog;
	
	public static MemLogAppender sharedInstance() {
		return sharedMemlog;
	}
	
	public interface MemLogMonitor {
		void receivedEntry(ILoggingEvent entry);
	}
	
	LinkedList<ILoggingEvent> entries = new LinkedList<>();
	LinkedList<MemLogMonitor> monitors = new LinkedList<>();
	private int historyDepth = 16384;
	private int threshold = Integer.MIN_VALUE;
	
	public MemLogAppender() {
		sharedMemlog = this;
	}
	
	protected synchronized void prune() {
		while(historyDepth >= 0 && entries.size() > historyDepth) {
			entries.remove();
		}
	}
	
	public synchronized void purge() {
		entries.clear();
	}
	
	protected synchronized void pruneToThreshold() {
		LinkedList<ILoggingEvent> pruned = new LinkedList<>();
		for(ILoggingEvent record : entries) {
			if(record.getLevel().toInt() >= getThreshold()) {
				pruned.add(record);
			}
		}
		
		entries = pruned;
		prune();
	}
	
	public LinkedList<ILoggingEvent> getEntries(int length) {
		return getEntries(-1, length, Integer.MIN_VALUE);
	}
	
	public LinkedList<ILoggingEvent> getEntries(int offset, int length) {
		return getEntries(offset, length, Integer.MIN_VALUE);
	}
	
	public synchronized LinkedList<ILoggingEvent> getEntries(int offset, int length, int threshold) {
		LinkedList<ILoggingEvent> results = new LinkedList<>();
		int maxOffset = entries.size();
		
		if(length == 0) return results;
		
		int toSkip;
		if(offset >= 0) {
			toSkip = offset;
		} else {
			toSkip = entries.size() + offset - length + 1;
			maxOffset += offset;
		}
		
		int idx = -1;
		for(ILoggingEvent entry : entries) {
			idx++;
			if(entry.getLevel().toInt() < threshold) continue;
			if(idx < toSkip) continue;
			if(length > 0 && results.size() >= length) break;
			if(idx > maxOffset) break;
			results.add(entry);
		}
		
		return results;
	}

	@Override
	public void append(ILoggingEvent entry) {
		if(entry.getLevel().toInt() < getThreshold()) return;
		synchronized(this) {
			entries.add(entry);
			prune();
		}
		
		for(MemLogMonitor monitor : monitors) {
			monitor.receivedEntry(entry);
		}
	}
	
	public int numEntries() {
		return entries.size();
	}

	public int getHistoryDepth() {
		return historyDepth;
	}

	public synchronized void setHistoryDepth(int historyDepth) {
		this.historyDepth = historyDepth;
		prune();
	}

	public int getThreshold() {
		return threshold;
	}

	public synchronized void setThreshold(int threshold) {
		this.threshold = threshold;
		pruneToThreshold();
		prune();
	}
	
	public synchronized void addMonitor(MemLogMonitor monitor) {
		monitors.add(monitor);
	}
	
	public synchronized void removeMonitor(MemLogMonitor monitor) {
		monitors.remove(monitor);
	}
	
	public synchronized void clearMonitors() {
		monitors.clear();
	}
}
