package com.acrescrypto.zksync.utility;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InputStreamWatcher {
	public interface InputStreamWatchReceivedDataCallback {
		public void receivedData(ByteBuffer data);
	}
	
	public interface InputStreamWatchClosedStreamCallback {
		public void closed();
	}
	
	public class InputStreamWatchInstance {
		Queue<Expectation> expectations = new LinkedList<>();
		ByteBuffer rxBuffer;
		String     name;
		
		protected class Expectation {
			boolean                               started;
			int                                   expectedBytes;
			InputStreamWatchReceivedDataCallback  callback;
			
			public Expectation(int expectedBytes, InputStreamWatchReceivedDataCallback callback) {
				this.expectedBytes = expectedBytes;
				this.callback      = callback;
				this.started       = false;
			}
			
			public void begin() {
				int cap = rxBuffer.capacity();
				boolean needNewBuffer =      rxBuffer == null
						                 || (cap < expectedBytes || cap > expectedBytes + 1024);
				
				if(needNewBuffer) {
					rxBuffer = ByteBuffer.allocate(expectedBytes);
				} else {
					rxBuffer.clear();
				}
				
				rxBuffer.limit(0);
			}
			
			public void read() throws IOException {
				if(!started) begin();
				
				int available = stream.available();
				if(available == 0) return;
				
				int numToRead = Math.min(available, expectedBytes);
				int numRead = stream.read(rxBuffer.array(), rxBuffer.limit(), numToRead);
				
				if(numRead < 0) {
					logger.debug("InputStreamWatcher {}: Reached EOF while waiting on additional data", name);
					close();
				}
				
				rxBuffer.limit(rxBuffer.limit() + numRead);
				expectedBytes -= numRead;
				
				if(expectedBytes <  0) {
					logger.error("InputStreamWatcher {}: Exceeded expected read length, rxBuffer contains {} bytes", name, rxBuffer.limit());
				}
				
				if(expectedBytes <= 0) {
					callback.receivedData(rxBuffer);
					finishExpectation();
				}
			}
		}
		
		boolean                                scheduled;
		InputStream                            stream;
		InputStreamWatchClosedStreamCallback   closedCallback;
		
		public InputStreamWatchInstance(InputStream stream, String name) {
			this.stream        = stream;
			this.name          = name;
		}
		
		public String getName() {
			return name;
		}
		
		public synchronized void close() throws IOException {
			try {
				stream.close();
			} finally {
				removeWatchInstance(this);
			}
		}
		
		public synchronized InputStreamWatchInstance expect(int length, InputStreamWatchReceivedDataCallback readCallback) {
			Expectation expectation = new Expectation(length, readCallback);
			expectations.add(expectation);
			
			if(expectations.size() == 1) {
				synchronized(InputStreamWatcher.this) {
					InputStreamWatcher.this.notifyAll();
				}
			}
			
			return this;
		}
		
		protected void finishExpectation() {
			expectations.poll();
		}
		
		public void onClose(InputStreamWatchClosedStreamCallback closedCallback) {
			this.closedCallback = closedCallback;
		}
		
		protected boolean needsCheck() {
			return !this.expectations.isEmpty() && !scheduled;
		}
		
		protected synchronized void scheduleCheck() {
			if(scheduled) return;
			
			scheduled = true;
			threadPool.submit(()->check());
		}
		
		protected synchronized void check() {
			scheduled = false;
			
			if(!needsCheck()) return;
			try {
				Expectation current = expectations.peek();
				current.read();
				if(needsCheck()) {
					synchronized(InputStreamWatcher.this) {
						InputStreamWatcher.this.notifyAll();
					}
				}
			} catch (IOException exc) {
				try {
					logger.debug("InputStreamWatcher {}: Caught exception checking stream", name, exc);
					close();
				} catch (IOException exc2) {
					logger.error("InputStreamWatcher {}: Caught exception closing stream", name, exc2);
				}
			}
		}
	}
	
	protected GroupedThreadPool threadPool;
	protected LinkedList<InputStreamWatchInstance> watchInstances = new LinkedList<>();
	private final Logger logger = LoggerFactory.getLogger(InputStreamWatcher.class);
	
	public InputStreamWatcher(GroupedThreadPool threadPool) {
		this.threadPool = threadPool;
	}
	
	public synchronized InputStreamWatchInstance watch(InputStream stream, String name) {
		InputStreamWatchInstance instance = new InputStreamWatchInstance(stream, name);
		watchInstances.add(instance);
		this.notifyAll();
		
		return instance;
	}
	
	public synchronized void removeWatchInstance(InputStreamWatchInstance instance) {
		watchInstances.remove(instance);
	}
	
	protected void watchThread() {
		while(true) {
			try {
				for(InputStreamWatchInstance instance : watchInstances) {
					if(!instance.needsCheck()) continue;
					instance.scheduleCheck();
				}
				
				/* Scan whenever we add an instance or finish checking one.
				 * And also defensively scan every 100ms in case we somehow fail to notify...
				 */
				this.wait(100);
			} catch(Exception exc) {
				logger.error("InputStreamWatcher: Uncaught exception in watch thread", exc);
			}
		}
	}
}
