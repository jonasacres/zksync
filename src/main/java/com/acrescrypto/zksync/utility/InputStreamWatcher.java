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
		Queue       <Expectation> expectations = new LinkedList<>();
		ByteBuffer                rxBuffer;
		String                    name;
		
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
				int cap               =      rxBuffer.capacity();
				boolean needNewBuffer =      rxBuffer == null
						                 || (cap < expectedBytes || cap > expectedBytes + 1024);
				
				if(needNewBuffer) {
					rxBuffer = ByteBuffer.allocate(expectedBytes);
				} else {
					rxBuffer.clear();
				}
				
				rxBuffer.limit(0);
			}
			
			public boolean read() throws IOException {
				if(!started) begin();
				
				int available = stream.available();
				if(available == 0) return false;
				
				int numToRead = Math.min(available, expectedBytes);
				int numRead = stream.read(rxBuffer.array(), rxBuffer.limit(), numToRead);
				
				if(numRead < 0) {
					logger.debug("InputStreamWatcher {}: Reached EOF while waiting on additional data", name);
					close();
					return false;
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
				
				return !expectations.isEmpty() && stream.available() > 0;
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
			try {
				if(scheduled)               return;
				if(stream.available() == 0) return;
				
				scheduled = true;
				threadPool.submit(()->check());
			} catch(IOException exc) {
				logger.warn("InputStreamWatcher {}: Caught exception polling stream", name, exc);
				try {
					close();
				} catch(IOException exc2) {
					logger.error("InputStreamWatcher {}: Caught exception closing socket", name, exc2);
				}
			}
		}
		
		protected synchronized void check() {
			scheduled = false;
			
			if(!needsCheck()) return;
			try {
				boolean shouldContinue;
				do {
					/* Keep this thread reading the stream and delivering data as long as
					 * we have an open expectation and reading is non-blocking.
					 */
					Expectation current = expectations.peek();
					shouldContinue = current.read();
				} while(shouldContinue);
				
				if(needsCheck()) {
					/* If we still have open expectations, let the watcher know to poll again
					 * right away.
					 */
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
	
	protected synchronized void watchThread() {
		while(true) {
			try {
				for(InputStreamWatchInstance instance : watchInstances) {
					if(!instance.needsCheck()) continue;
					instance.scheduleCheck();
				}
				
				/* Scan whenever we add an instance or finish checking one.
				 * And also defensively scan every 1ms in case we somehow fail to notify...
				 * 
				 * What would be REALLY nice is to select on this so that we get notified of
				 * available bytes, instead of having to poll for them. 
				 */
				this.wait(1);
			} catch(Exception exc) {
				logger.error("InputStreamWatcher: Uncaught exception in watch thread", exc);
			}
		}
	}
}
