package com.acrescrypto.zksync.utility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class TaskPool<T,R> {
	protected int concurrency;
	protected boolean complete, closed, closesOnSuccess = true;
	protected ArrayList<Thread> threads = new ArrayList<Thread>();
	protected BlockingQueue<T> dataQueue = new LinkedBlockingDeque<T>();
	protected TaskPoolCallback<T,R> callback;
	protected TaskPoolTask<T,R> task;
	
	protected T resultInput;
	protected R resultOutput;
	
	public interface TaskPoolCallback<T, R> {
		void complete(T resultInput, R resultOutput);
	}
	
	public interface OneoffTask {
		Object runTask();
	}
	
	public interface TaskPoolTask<T,R> {
		R runTask(T data);
	}
	
	public static Object oneoff(long waitTime, OneoffTask task) {
		ArrayList<Integer> list = new ArrayList<Integer>();
		list.add(0);
		
		return new TaskPool<Integer,Object>(1, list)
		  .setTask((Integer trash) -> { return task.runTask(); })
		  .launch()
		  .waitForResult(waitTime)
		  .getResult();
	}
	
	public TaskPool(int concurrency, Collection<T> dataQueue) {
		if(dataQueue != null) this.dataQueue.addAll(dataQueue);
		this.concurrency = concurrency;
	}
	
	public TaskPool<T,R> setCallback(TaskPoolCallback<T,R> callback) {
		this.callback = callback;
		return this;
	}
	
	public TaskPool<T,R> setTask(TaskPoolTask<T,R> task) {
		this.task = task;
		return this;
	}
	
	public TaskPool<T,R> add(T input) {
		dataQueue.add(input);
		return this;
	}
	
	public TaskPool<T,R> holdOpen() {
		closesOnSuccess = false;
		return this;
	}
	
	public TaskPool<T,R> done() {
		closed = true;
		return this;
	}
	
	public synchronized TaskPool<T,R> waitForResult() {
		while(!complete) {
			try {
				this.wait();
			} catch (InterruptedException e) {
			}
		}
		
		return this;
	}
	
	public synchronized TaskPool<T,R> waitForResult(long timeoutMs) {
		long ts = System.currentTimeMillis();
		while(!complete && System.currentTimeMillis() < ts + timeoutMs) {
			try {
				this.wait(timeoutMs);
			} catch (InterruptedException e) {
			}
		}
		
		return this;
	}
	
	public T getResultInput() {
		return resultInput;
	}
	
	public R getResult() {
		return resultOutput;
	}

	public TaskPool<T,R> launch() {
		new Thread(() -> {
			while(true) {
				if(complete) return;
				launchNextThread();
				
				if(closed && threads.isEmpty()) {
					fail();
					return;
				}
				
				if(threads.size() >= concurrency || closed) {
					// we can't immediately put another thread into the pool; wait a bit for a thread to report a result. 
					synchronized(this) {
						try {
							this.wait(10);
						} catch (InterruptedException e) {}
					}
				}
			}
		}).start();
		
		return this;
	}
	
	protected synchronized void launchNextThread() {
		if(threads.size() >= concurrency) return;
		T item = null;
		while(item == null) {
			try {
				dataQueue.take();
			} catch(InterruptedException exc) {}
		}
		
		Thread thread = new Thread(() -> {
			try {
				R result = this.task.runTask(item);
				if(result != null) {
					complete(item, result);
				}
			} finally {
				this.notifyAll();
			}
		});
		
		threads.add(thread);
		thread.start();
	}
	
	private synchronized void fail() {
		if(complete) return;
		complete = true;
		if(callback != null) callback.complete(null, null);
		this.notifyAll();
	}

	private synchronized void complete(T item, R result) {
		if(complete) return;
		if(closesOnSuccess) complete = true;
		if(callback != null) callback.complete(item, result);
		this.notifyAll();
	}
}
