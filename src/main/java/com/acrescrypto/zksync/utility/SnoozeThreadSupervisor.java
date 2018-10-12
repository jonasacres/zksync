package com.acrescrypto.zksync.utility;

import java.util.Collection;
import java.util.LinkedList;

public class SnoozeThreadSupervisor {
	static SnoozeThreadSupervisor shared;
	
	protected LinkedList<SnoozeThread> items = new LinkedList<>();
	protected SnoozeThread nextExpiredItem;
	protected GroupedThreadPool threadPool = GroupedThreadPool.newFixedThreadPool("SnoozeThreadSupervisor", 4);
	protected boolean closed;
	
	public static SnoozeThreadSupervisor shared() {
		if(shared == null) {
			shared = new SnoozeThreadSupervisor();
		}
		
		return shared;
	}
	
	protected SnoozeThreadSupervisor() {
		new Thread(()->monitorThread()).start();
	}
	
	public synchronized void add(SnoozeThread item) {
		items.add(item);
		if(nextExpiredItem == null || item.expiration < nextExpiredItem.expiration) {
			nextExpiredItem = item;
			this.notifyAll();
		}
	}
	
	public boolean isEmpty() {
		return items.isEmpty();
	}
	
	public synchronized void reset() {
		items.clear();
	}
	
	public synchronized void close() {
		if(shared == this) shared = null;
		reset();
		threadPool.shutdownNow();
		closed = true;
		this.notifyAll();
	}
	
	protected void monitorThread() {
		Util.setThreadName("SnoozeThreadSupervisor monitor");
		while(!closed) {
			prune(true);
			
			synchronized(this) {
				long waitTime = Long.MAX_VALUE;
				
				if(nextExpiredItem != null) {
					waitTime = nextExpiredItem.expiration - System.currentTimeMillis();
				}
				
				if(waitTime > 0) {
					try {
						this.wait(waitTime);
					} catch (InterruptedException e) {}
				}
			}
		}
	}
	
	protected void runCallback(SnoozeThread item) {
		if(threadPool.isShutdown()) return;
		threadPool.submit(()->{
			item.runTask();
		});
	}
	
	public synchronized void prune(boolean runTasks) {
		LinkedList<SnoozeThread> newList = new LinkedList<>();
		nextExpiredItem = null;

		for(SnoozeThread item : items) {
			if(!item.isExpired() && !item.isCancelled()) {
				newList.add(item);
				if(nextExpiredItem == null || item.expiration < nextExpiredItem.expiration) {
					nextExpiredItem = item;
				}
				
				continue;
			}
			
			// item must be expired or cancelled
			
			if(runTasks && (item.isExpired() || item.callbackOnManualCancel)) {
				runCallback(item);
			}
		}
		
		items = newList;
	}

	public Collection<SnoozeThread> getItems() {
		return items;
	}

	public synchronized void dump() {
		System.out.println("Pending snooze threads: " + items.size());
		for(SnoozeThread item : items) {
			System.out.println("\t" + item.delayMs + "ms " + item.callback.getClass().getSimpleName());
		}
	}

	public synchronized void update() {
		this.notifyAll();
	}
}
