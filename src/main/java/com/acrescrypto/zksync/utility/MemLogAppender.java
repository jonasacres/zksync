package com.acrescrypto.zksync.utility;

import java.util.LinkedList;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

public class MemLogAppender extends AppenderBase<ILoggingEvent> {
	private static MemLogAppender sharedMemlog;
	
	public static MemLogAppender sharedInstance() {
		if(sharedMemlog == null) {
			new MemLogAppender();
		}
		
		return sharedMemlog;
	}
	
	public interface MemLogMonitor {
		void receivedEntry(LogEvent entry);
	}
	
	public class LogEvent {
		ILoggingEvent entry;
		long entryId;
		
		public LogEvent(ILoggingEvent entry) {
			this.entry = entry;
			this.entryId = issueEntryId();
		}
		
		public ILoggingEvent getEntry() { return entry; }
		public long getEntryId() { return entryId; }
	}
	
	LinkedList<LogEvent> entries = new LinkedList<>();
	LinkedList<MemLogMonitor> monitors = new LinkedList<>();
	private int historyDepth = 16384;
	private int threshold = Integer.MIN_VALUE;
	private long nextEntryId = 0;
	
	public MemLogAppender() {
		if(sharedMemlog == null) {
			sharedMemlog = this;
		}
	}
	
	protected synchronized long issueEntryId() {
		return nextEntryId++;
	}
	
	protected synchronized void prune() {
		while(historyDepth >= 0 && entries.size() > historyDepth) {
			entries.remove();
		}
	}
	
	public synchronized void purge() {
		entries.clear();
	}
	
	public synchronized void hardPurge() {
		purge();
		nextEntryId = 0;
	}
	
	protected synchronized void pruneToThreshold() {
		LinkedList<LogEvent> pruned = new LinkedList<>();
		for(LogEvent event : entries) {
			if(event.entry.getLevel().toInt() >= getThreshold()) {
				pruned.add(event);
			}
		}
		
		entries = pruned;
		prune();
	}
	
	public LinkedList<LogEvent> getEntries(int length) {
		return getEntries(-1, length, Integer.MIN_VALUE, Long.MIN_VALUE, Long.MAX_VALUE);
	}
	
	public LinkedList<LogEvent> getEntries(int offset, int length) {
		return getEntries(offset, length, Integer.MIN_VALUE, Long.MIN_VALUE, Long.MAX_VALUE);
	}
	
	public synchronized LinkedList<LogEvent> getEntries(int offset, int length, int threshold, long after, long before) {
		LinkedList<LogEvent> results = new LinkedList<>();
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
		for(LogEvent event : entries) {
			idx++;
			if(event.entry.getLevel().toInt() < threshold) continue;
			if(event.getEntryId() <= after) continue;
			if(event.getEntryId() >= before) continue;
			if(idx < toSkip) continue;
			
			if(length > 0 && results.size() >= length) break;
			if(idx > maxOffset) break;
			results.add(event);
		}
		
		return results;
	}

	@Override
	public void append(ILoggingEvent entry) {
		LogEvent event = new LogEvent(entry);
		if(entry.getLevel().toInt() < getThreshold()) return;
		synchronized(this) {
			entries.add(event);
			prune();
		}
		
		for(MemLogMonitor monitor : monitors) {
			monitor.receivedEntry(event);
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
