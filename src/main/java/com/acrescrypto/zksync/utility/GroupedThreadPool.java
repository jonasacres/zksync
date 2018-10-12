package com.acrescrypto.zksync.utility;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

public class GroupedThreadPool {
	public ThreadGroup threadGroup;
	protected ExecutorService executor;
	
	public final static int MODE_FIXED = 0;
	public final static int MODE_CACHED = 1;

	public static GroupedThreadPool newFixedThreadPool(String name, int maxThreads) {
		return newFixedThreadPool(Thread.currentThread().getThreadGroup(), name, maxThreads);
	}
	
	public static GroupedThreadPool newFixedThreadPool(ThreadGroup parent, String name, int maxThreads) {
		return new GroupedThreadPool(parent, name, maxThreads, MODE_FIXED);
	}
	
	public static GroupedThreadPool newCachedThreadPool(String name) {
		return newCachedThreadPool(Thread.currentThread().getThreadGroup(), name);
	}

	public static GroupedThreadPool newCachedThreadPool(ThreadGroup parent, String name) {
		return new GroupedThreadPool(parent, name, 0, MODE_CACHED);
	}
	
	public GroupedThreadPool(ThreadGroup parent, String name, int maxThreads, int mode) {
		threadGroup = new ThreadGroup(parent, name);
		ThreadFactory factory = new ThreadFactory() {
			public Thread newThread(Runnable r) {
                return new Thread(threadGroup, ()->{
                	Thread.currentThread().setName(name + " active thread");
                	try {
                		r.run();
                	} finally {
                		Thread.currentThread().setName(name + " idle thread");
                	}
                });
            }
		};
		
		switch(mode) {
		case MODE_FIXED:
			executor = Executors.newFixedThreadPool(maxThreads, factory);
			break;
		case MODE_CACHED:
			executor = Executors.newCachedThreadPool(factory);
			break;
		default:
			throw new RuntimeException("Invalid GroupedThreadPool mode");
		}
	}
	
	public Future<?> submit(Runnable task) {
		return executor.submit(task);
	}

	public void shutdownNow() {
		executor.shutdownNow();
	}

	public boolean isShutdown() {
		return executor.isShutdown();
	}
}
