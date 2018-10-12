package com.acrescrypto.zksync.utility;

import java.util.Collection;
import java.util.LinkedList;

import com.acrescrypto.zksync.utility.Util.AnonymousCallback;
import com.acrescrypto.zksync.utility.Util.WaitTest;

public class WaitSupervisor {
	public class WaitTask {
		long maxDelayMs, startTime, frequency, nextCheck, expire;
		WaitTest test;
		AnonymousCallback action;
		
		WaitTask(long maxDelayMs, long frequency, WaitTest test, AnonymousCallback action) {
			long now = System.currentTimeMillis();
			this.maxDelayMs = maxDelayMs;
			this.frequency = frequency;
			this.test = test;
			this.action = action;
			this.nextCheck = now + frequency;
			this.expire = now + maxDelayMs;
		}
	}
	
	static WaitSupervisor shared;
	
	public static WaitSupervisor shared() {
		if(shared == null) {
			shared = new WaitSupervisor();
		}
		
		return shared;
	}
	
	protected GroupedThreadPool threadPool = GroupedThreadPool.newFixedThreadPool("WaitSupervisor", 4);
	LinkedList<WaitTask> tasks = new LinkedList<>();
	boolean closed;
	
	protected WaitSupervisor() {
		new Thread(()->monitorThread()).start();
	}
	
	public synchronized void add(long maxDelayMs, long frequency, WaitTest test, AnonymousCallback action) {
		tasks.add(new WaitTask(maxDelayMs, frequency, test, action));
		this.notifyAll();
	}
	
	protected void monitorThread() {
		Thread.currentThread().setName("WaitSupervisor");
		while(!closed) {
			LinkedList<WaitTask> newTasks = new LinkedList<>();
			long nextCheck = Long.MAX_VALUE;
			long now = System.currentTimeMillis();
			
			synchronized(this) {
				for(WaitTask task : tasks) {
					if(task.nextCheck <= now) {
						if(task.test.test()) {
							// task will not be added to newTasks, callback will not be invoked
							continue;
						}
						
						task.nextCheck = now + task.frequency;
						if(task.expire <= now) {
							threadPool.submit(()->{
								try {
									task.action.cb();
								} catch (Exception e) {}
							});
							continue;
						}
					}
					
					if(task.nextCheck < nextCheck) {
						nextCheck = task.nextCheck;
					}
					
					newTasks.add(task);
				}
				
				tasks = newTasks;
				
				try {
					this.wait(nextCheck - now);
				} catch (InterruptedException e) {}
			}
		}
	}
	
	public Collection<WaitTask> getTasks() {
		return tasks;
	}
	
	public void dump() {
		System.out.println("Pending wait tasks: " + tasks.size());
		for(WaitTask task : tasks) {
			System.out.println("\t" + task.maxDelayMs + "ms/" + task.frequency + "ms: " + task.action.getClass().getSimpleName());
		}
	}
}
