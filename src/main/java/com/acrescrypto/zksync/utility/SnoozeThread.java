package com.acrescrypto.zksync.utility;

public class SnoozeThread {
	public interface SnoozeThreadCallback {
		void callback();
	}
	
	long delayMs;
	long deadline;
	long expiration;
	boolean cancelled;
	boolean callbackOnManualCancel;
	SnoozeThreadCallback callback;
	
	public SnoozeThread(long delayMs, boolean callbackOnManualCancel, SnoozeThreadCallback callback) {
		this(delayMs, -1, callbackOnManualCancel, callback);
	}
	
	public SnoozeThread(long delayMs, long maxTimeMs, boolean callbackOnManualCancel, SnoozeThreadCallback callback) {
		this.delayMs = delayMs;
		this.deadline = maxTimeMs < 0 ? Long.MAX_VALUE : System.currentTimeMillis() + maxTimeMs;
		this.callback = callback;
		this.callbackOnManualCancel = callbackOnManualCancel;
		snooze();
		SnoozeThreadSupervisor.shared().add(this);
	}

	
	public synchronized void cancel() {
		this.cancelled = true;
		SnoozeThreadSupervisor.shared().update();
	}
	
	public boolean isCancelled() {
		return this.cancelled;
	}
	
	public boolean isExpired() {
		return System.currentTimeMillis() >= expiration;
	}
	
	public void runTask() {
		cancelled = true;
		callback.callback();
	}
	
	public synchronized boolean snooze() {
		if(cancelled) return false;
		this.expiration = Math.min(deadline, System.currentTimeMillis() + delayMs);
		return true;
	}
}
