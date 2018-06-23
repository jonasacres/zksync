package com.acrescrypto.zksync.utility;

public class SnoozeThread {
	public interface SnoozeThreadCallback {
		void callback();
	}
	
	int delayMs;
	long expiration;
	boolean cancelled;
	boolean callbackOnManualCancel;
	SnoozeThreadCallback callback;
	
	public SnoozeThread(int delayMs, boolean callbackOnManualCancel, SnoozeThreadCallback callback) {
		this.delayMs = delayMs;
		this.callback = callback;
		this.callbackOnManualCancel = callbackOnManualCancel;
		snooze();
		new Thread(()->snoozeThread()).start();
	}
	
	public synchronized void cancel() {
		this.cancelled = true;
		this.notifyAll();
	}
	
	public boolean isCancelled() {
		return this.cancelled;
	}
	
	public synchronized boolean snooze() {
		if(cancelled) return false;
		this.expiration = System.currentTimeMillis() + delayMs;
		return true;
	}
	
	protected synchronized void snoozeThread() {
		Thread.currentThread().setName("SnoozeThread " + delayMs + "ms");
		while(!cancelled && System.currentTimeMillis() < expiration) {
			try {
				this.wait(expiration - System.currentTimeMillis());
			} catch (InterruptedException e) {}
		}
		
		if(!cancelled || callbackOnManualCancel) {
			cancelled = true;
			callback.callback();
		}
	}
}
