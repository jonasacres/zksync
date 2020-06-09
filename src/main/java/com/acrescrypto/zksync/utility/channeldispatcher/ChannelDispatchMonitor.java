package com.acrescrypto.zksync.utility.channeldispatcher;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.utility.BandwidthAllocator;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.Util;

public class ChannelDispatchMonitor {
	public interface ChannelDispatchClosedCallback {
		void closed();
	}
	
	public interface ChannelDispatchReadCallback {
		void read(ByteBuffer data);
	}
	
	public class Expectation {
		int                              expectedBytes;
		ChannelDispatchReadCallback      callback;
		boolean                          active;
		
		public Expectation(int expectedBytes, ChannelDispatchReadCallback callback) {
			this.expectedBytes    =      expectedBytes;
			this.callback         =      callback;
		}
		
		public void begin() {
			active                =      true;
			int     cap           =      rxBuffer.capacity();
			boolean needNewBuffer =      rxBuffer == null
	                                  || cap < expectedBytes
	                                  || cap > expectedBytes + BUFFER_SCALE_INCREMENT;

			if(needNewBuffer) {
				rxBuffer          =      ByteBuffer.allocate(expectedBytes);
			} else {
				rxBuffer.clear();
			}
		}
		
		public void read() throws IOException {
			if(!active) begin();
			
			// TODO: Bandwidth limitation
			int numRead = channel.read(rxBuffer);
			
			if(numRead < 0) {
				logger.debug("ChannelDispatch {}: Reached EOF while waiting on additional data", name);
				close();
				return;
			}
			
			monitorRx.observeTraffic(numRead);
			expectedBytes -= numRead;
			if(expectedBytes <  0) {
				logger.error("ChannelDispatch {}: Exceeded expected read length, rxBuffer contains {} bytes", name, rxBuffer.limit());
			}
			
			if(expectedBytes <= 0) {
				rxBuffer.limit(rxBuffer.position());
				rxBuffer.position(0);
				callback.read(rxBuffer);
				finishExpectation();
			}
		}
	}
	
	public final static int DEFAULT_BUFFER_SIZE    = 1024;
	public final static int BUFFER_SCALE_INCREMENT = 1024;
	
	protected String                        name;
	protected SocketChannel                 channel;
	protected BandwidthAllocator            allocatorTx,
	                                        allocatorRx;
	protected BandwidthMonitor              monitorTx,
	                                        monitorRx;
	protected SelectionKey                  acceptDispatchKey,
	                                        acceptLocalKey,
	                                        txDispatchKey,
	                                        txLocalKey,
	                                        rxDispatchKey,
	                                        rxLocalKey;
	protected Selector                      acceptSelector,
	                                        txSelector,
	                                        rxSelector;
	protected ByteBuffer                    txBuffer,
	                                        rxBuffer;
	protected boolean                       calledClosedCallback;
	protected ChannelDispatchClosedCallback closedCallback;
	protected Queue<Expectation>            expectations;
	protected ChannelDispatch               dispatch;
	protected long                          acceptStartTime,
	                                        txStartTime,
	                                        rxStartTime;
	
	private   Logger                        logger = LoggerFactory.getLogger(ChannelDispatchMonitor.class);
	
	public ChannelDispatchMonitor(ChannelDispatch dispatch, String name, SocketChannel channel) throws IOException {
		this.dispatch           = dispatch;
		this.name               = name;
		this.channel            = channel;
		this.expectations       = new LinkedList<>();
		
		this.txBuffer           = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
		this.rxBuffer           = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
		
		this.txSelector         = Selector.open();
		this.rxSelector         = Selector.open();
		
		this.txLocalKey         = channel.register(txSelector,     SelectionKey.OP_WRITE );
		this.rxLocalKey         = channel.register(rxSelector,     SelectionKey.OP_READ  );
	}
	
	public void setClosedCallback(ChannelDispatchClosedCallback closedCallback) {
		this.closedCallback = closedCallback;
	}
	
	public SelectableChannel getChannel() {
		return channel;
	}
	
	public void setBandwidthAllocatorTx(BandwidthAllocator allocatorTx) {
		this.allocatorTx = allocatorTx;
	}
	
	public void setBandwidthAllocatorRx(BandwidthAllocator allocatorRx) {
		this.allocatorRx = allocatorRx;
	}
	
	public void setBandwidthMonitorTx  (BandwidthMonitor   monitorTx) {
		this.monitorTx   = monitorTx;
	}
	
	public void setBandwidthMonitorRx  (BandwidthMonitor   monitorRx) {
		this.monitorRx   = monitorRx;
	}
	
	public BandwidthAllocator getBandwidthAllocatorTx() {
		return allocatorTx;
	}
	
	public BandwidthAllocator getBandwidthAllocatorRx() {
		return allocatorRx;
	}
	
	public BandwidthMonitor   getBandwidthMonitorTx() {
		return monitorTx;
	}
	
	public BandwidthMonitor   getBandwidthMonitorRx() {
		return monitorRx;
	}
	
	public synchronized void close() throws IOException {
		if(calledClosedCallback)   return;		
		calledClosedCallback     = true;
		
		unregisterRx();
		unregisterTx();
		acceptLocalKey.cancel();
		txLocalKey    .cancel();
		rxLocalKey    .cancel();
		
		if(channel.isOpen())       channel.close();
		if(closedCallback != null) closedCallback.closed();
		
		closedCallback           = null;
		rxBuffer                 = null;
		txBuffer                 = null;
		expectations             = null;
	}
	
	public boolean closed() {
		return calledClosedCallback;
	}
	
	public void send(byte[]     bytes) throws IOException {
		send(ByteBuffer.wrap(bytes));
	}
	
	public void send(ByteBuffer bytes) throws IOException {
		reallocateTxBuffer(bytes.remaining());
		txBuffer.limit(txBuffer.position() + bytes.remaining());
		txBuffer.put(bytes);
		
		registerTx();
	}
	
	public ChannelDispatchMonitor expect(int length, ChannelDispatchReadCallback callback) {
		Expectation expectation = new Expectation(length, callback);
		expectations.add(expectation);
		
		if(expectations.size() == 1) {
			registerRx();
		}
		
		return this;
	}
	
	public void finishExpectation() {
		expectations.remove();
	}
	
	protected void registerTx() {
		if(txDispatchKey     != null) return;
		txDispatchKey         = registerKey(SelectionKey.OP_WRITE);
	}
	
	protected void registerRx() {
		if(rxDispatchKey     != null) return;
		rxDispatchKey         = registerKey(SelectionKey.OP_READ);
	}
	
	protected SelectionKey registerKey(int flags) {
		SelectionKey key;
		
		try {
			key = channel.register(
					dispatch.getSelector(),
					flags
				);
			key.attach(this);
			return key;
		} catch (ClosedChannelException exc) {
			logger.info("ChannelDispatch {}: Caught exception registering channel for selection with flags {}", name, flags, exc);
			try {
				close();
			} catch (IOException exc2) {
				logger.error("ChannelDispatch {}: Caught exception closing channel after handling closed channel exception", name, exc);
				exc2.printStackTrace();
			}
			
			return null;
		}
	}
	
	protected void unregisterTx() {
		if(txDispatchKey == null) return;
		txDispatchKey.cancel();
		txDispatchKey = null;
	}

	protected void unregisterRx() {
		if(rxDispatchKey == null) return;
		rxDispatchKey.cancel();
		rxDispatchKey = null;
	}
	
	protected void reallocateTxBuffer(int neededExtraSize) {
		// how much room will we have AFTER adding this to the buffer?
		int spareCapacity     = txBuffer.capacity()
				              - txBuffer.limit()
				              - neededExtraSize;
		
		// how many bytes will we have queued up AFTER we add this in?
		int totalSize         = txBuffer.limit()
				              + neededExtraSize
				              - txBuffer.position();
		
		int downsizeThreshold = (int) Math.max(txBuffer.limit(),  0.5 * totalSize);
		
		if(    spareCapacity       <  0                      // buffer too small
		    || spareCapacity       >  BUFFER_SCALE_INCREMENT // buffer too big
		    || txBuffer.position() >= downsizeThreshold      // buffer too used up
		  ) {
			/* The buffer grows in increments of BUFFER_SCALE_INCREMENT, with a minimum of 1
			 * increment. */
			int rounded    = (int) Math.ceil((double) totalSize / (double) BUFFER_SCALE_INCREMENT) * BUFFER_SCALE_INCREMENT;
			int allocation = Math.max(DEFAULT_BUFFER_SIZE, rounded);
			
			ByteBuffer newTxBuffer = ByteBuffer.allocate(allocation);
			newTxBuffer.put(txBuffer);
			txBuffer = newTxBuffer;
		}
	}
	
	protected synchronized void doTx() {
		if(closed())              return;
		txStartTime = Util.currentTimeMillis();
		
		try {
			while(canWrite()) {
				// TODO: Bandwidth limitation
				int numWritten = channel.write(txBuffer);
				monitorTx.observeTraffic(numWritten);
			}
		} catch(IOException exc) {
			logger.info("ChannelDispatch {}: Caught exception writing channel; closing", name, exc);
			try {
				close();
			} catch(IOException exc2) {
				logger.error("ChannelDispatch {}: Caught exception closing channel after handling exception writing channel", name, exc2);
			}
		} finally {
			if(closed()) return;
			
			if(!txBuffer.hasRemaining()) {
				// We've sent all pending data; resize the buffer if appropriate
				reallocateTxBuffer(0);
			}
			
			registerTx();
		}
	}

	protected synchronized void doRx() {
		if(expectations.isEmpty())      return;
		if(closed())                    return;
		rxStartTime = Util.currentTimeMillis();
		
		try {
			while(canRead()) {
				expectations.peek().read();
			}
		} catch (IOException exc) {
			logger.warn("ChannelDispatch {}: Caught exception reading channel; closing", name, exc);
			try {
				close();
			} catch (IOException exc2) {
				logger.error("ChannelDispatch {}: Caught exception closing channel after handling exception reading channel", name, exc2);
			}
		} finally {
			if(closed())               return;
			if(expectations.isEmpty()) return;
			registerRx();
		}
	}
	
	protected boolean canWrite() throws IOException {
		if(closed())                    return false;
		if(!txBuffer.hasRemaining())    return false;
		
		long expiration = txStartTime + dispatch.maxWorkerThreadDuration();
		boolean expired = Util.currentTimeMillis() > expiration;
		if(expired)                     return false;
		
		return txSelector.selectNow() > 0;
	}
	
	protected boolean canRead() throws IOException {
		if(closed())                    return false;
		if(expectations.isEmpty())      return false;

		long expiration = rxStartTime + dispatch.maxWorkerThreadDuration();
		boolean expired = Util.currentTimeMillis() > expiration;
		if(expired)                     return false;
		
		return rxSelector.selectNow() > 0;
	}
}
