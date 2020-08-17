package com.acrescrypto.zksync.utility;

public class SnoozeThread {
	public interface SnoozeThreadCallback {
		void callback();
	}
	
	private long delayMs;
	private long maxTimeMs;
	private long deadline;
	private long expiration;
	private boolean cancelled;
	private boolean callbackOnManualCancel;
	SnoozeThreadCallback callback;
	
	public SnoozeThread(long delayMs, boolean callbackOnManualCancel, SnoozeThreadCallback callback) {
		this(delayMs, -1, callbackOnManualCancel, callback);
	}
	
	public SnoozeThread(long delayMs, long maxTimeMs, boolean callbackOnManualCancel, SnoozeThreadCallback callback) {
		this.delayMs   = delayMs;
		this.maxTimeMs = maxTimeMs;
		this.deadline  = maxTimeMs < 0 ? Long.MAX_VALUE : System.currentTimeMillis() + maxTimeMs;
		this.callback  = callback;
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
	
	public long getFireTime() {
		return Math.min(deadline, expiration);
	}
	
	public long getDeadline() {
		return deadline;
	}
	
	public long getExpirationMs() {
		return expiration;
	}
	
	public long getMaxTimeMs() {
		return maxTimeMs;
	}
	
	public void setDelayMs(long delayMs) {
		if(delayMs == this.delayMs) return;
		
		long newExpiration = this.expiration - this.delayMs + delayMs;
		this.delayMs = delayMs;
		this.expiration = newExpiration;
		SnoozeThreadSupervisor.shared().reevaluate(this);
	}
	
	public void setMaxTimeMs(long maxTimeMs) {
		if(maxTimeMs == this.maxTimeMs) return;

		long newDeadline = this.deadline - this.maxTimeMs + maxTimeMs;
		this.maxTimeMs = maxTimeMs;
		this.deadline = newDeadline;
		SnoozeThreadSupervisor.shared().reevaluate(this);
	}

	public long getDelayMs() {
		return delayMs;
	}

	public boolean isCalledBackOnManualCancel() {
		return callbackOnManualCancel;
	}
}
