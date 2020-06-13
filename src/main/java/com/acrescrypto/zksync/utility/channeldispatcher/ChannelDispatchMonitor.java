package com.acrescrypto.zksync.utility.channeldispatcher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
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
	
	public interface ChannelDispatchConnectedCallback {
		void connected() throws Exception;
	}
	
	public interface ChannelDispatchReadCallback {
		void read(ByteBuffer data) throws Exception;
	}
	
	public interface ChannelDispatchWriteCallback {
		void sent() throws Exception;
	}
	
	public interface ChannelDispatchExceptionHandler {
		void exception(Exception exc);
	}
	
	public class Transmission {
		ChannelDispatchWriteCallback        callback;
		ChannelDispatchExceptionHandler     exceptionHandler;
		ByteBuffer                          data;
		
		public Transmission(
			ByteBuffer                      data,
			ChannelDispatchExceptionHandler exceptionHandler,
			ChannelDispatchWriteCallback    callback)
		{
			this.exceptionHandler  =        exceptionHandler;
			this.callback          =        callback;
			this.data              =        data;
		}
		
		public void write() throws IOException {
			if(!data.hasRemaining()) {
				try {
					if(callback != null) callback.sent();
				} catch(Exception exc) {
					exceptionHandler.exception(exc);
				}
				
				finishTransmission();
			}
		}
	}
	
	public class Expectation {
		boolean                             active;
		ChannelDispatchReadCallback         callback;
		ChannelDispatchExceptionHandler     exceptionHandler;
		int                                 expectedBytes;
		
		public Expectation(
			int                             expectedBytes,
			ChannelDispatchExceptionHandler exceptionHandler,
			ChannelDispatchReadCallback     callback)
		{
			this.exceptionHandler  =        exceptionHandler;
			this.callback          =        callback;
			this.expectedBytes     =        expectedBytes;
		}
		
		public void begin() {
			active                 =        true;
			int     cap            =        rxBuffer.capacity();
			boolean needNewBuffer  =        rxBuffer == null
	                                     || cap < expectedBytes
	                                     || cap > expectedBytes + BUFFER_SCALE_INCREMENT;

			if(needNewBuffer) {
				rxBuffer           =        ByteBuffer.allocate(expectedBytes);
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
				try {
					callback.read(rxBuffer);
				} catch(Exception exc) {
					exceptionHandler.exception(exc);
				}
				
				finishExpectation();
			}
		}
	}
	
	public final static int DEFAULT_BUFFER_SIZE    = 1024;
	public final static int BUFFER_SCALE_INCREMENT = 1024;
	
	protected String                           name;
	protected SocketChannel                    channel;
	protected BandwidthAllocator               allocatorTx,
	                                           allocatorRx;
	protected BandwidthMonitor                 monitorTx,
	                                           monitorRx;
	protected SelectionKey                     txDispatchKey,
	                                           txLocalKey,
	                                           rxDispatchKey,
	                                           rxLocalKey,
	                                           connectDispatchKey;
	protected Selector                         txSelector,
	                                           rxSelector;
	protected ByteBuffer                       rxBuffer;
	protected boolean                          calledClosedCallback;
	protected ChannelDispatchClosedCallback    closedCallback;
	protected ChannelDispatchConnectedCallback connectedCallback;
	protected Queue<Expectation>               expectations;
	protected Queue<Transmission>              transmissions;
	protected ChannelDispatch                  dispatch;
	protected long                             txStartTime,
	                                           rxStartTime;
	protected ChannelDispatchExceptionHandler  defaultExceptionHandler;
	
	private   Logger                           logger = LoggerFactory.getLogger(ChannelDispatchMonitor.class);
	
	public static ChannelDispatchMonitor connectChannel(ChannelDispatch dispatch, String name, String host, int port, ChannelDispatchConnectedCallback callback) throws IOException {
		SocketChannel     channel  = SocketChannel.open();
		InetSocketAddress address  = new InetSocketAddress(host, port);
		/* TODO: Above InetSocketAddress blocks while host is resolved.
		 * How do we do non-blocking DNS? For now, we will just have to tolerate it. */
		
		channel.configureBlocking(false);
		channel.connect          (address);
		
		ChannelDispatchMonitor monitor = new ChannelDispatchMonitor(dispatch, name, channel);
		monitor.setConnectedCallback(callback);
		monitor.registerConnect();
		
		return monitor;
	}
	
	public ChannelDispatchMonitor(ChannelDispatch dispatch, String name, SocketChannel channel) throws IOException {
		channel.configureBlocking(false);
		
		this.dispatch                = dispatch;
		this.name                    = name;
		this.channel                 = channel;
		this.expectations            = new LinkedList<>();
		this.transmissions           = new LinkedList<>();
		this.defaultExceptionHandler = (exc) -> {
			logger.error("ChannelDispatch {}: Unhandled exception", exc);
			try {
				close();
			} catch (IOException exc2) {
				logger.error("ChannelDispatch {}: Exception closing dispatch following unhandled exception", exc2);
			}
		};
		
		this.rxBuffer                = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE);
		
		this.txSelector              = Selector.open();
		this.rxSelector              = Selector.open();
		
		this.txLocalKey              = channel.register(txSelector, SelectionKey.OP_WRITE);
		this.rxLocalKey              = channel.register(rxSelector, SelectionKey.OP_READ );
	}
	
	public void setClosedCallback(ChannelDispatchClosedCallback closedCallback) {
		this.closedCallback = closedCallback;
	}
	
	public void setDefaultExceptionHandler(ChannelDispatchExceptionHandler handler) {
		this.defaultExceptionHandler = handler;
	}
	
	public void setConnectedCallback(ChannelDispatchConnectedCallback connectedCallback) {
		this.connectedCallback = connectedCallback;
	}
	
	public SocketChannel getChannel() {
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
		txLocalKey    .cancel();
		rxLocalKey    .cancel();
		
		if(channel.isOpen())       channel.close();
		if(closedCallback != null) closedCallback.closed();
		
		closedCallback           = null;
		rxBuffer                 = null;
		expectations             = null;
		transmissions            = null;
	}
	
	public boolean closed() {
		return calledClosedCallback;
	}
	
	public void send(byte[] bytes) throws IOException {
		send(ByteBuffer.wrap(bytes), defaultExceptionHandler, null);
	}
	
	public void send(
			ByteBuffer                      bytes,
			ChannelDispatchWriteCallback    callback
		) throws IOException
	{
		send(bytes, defaultExceptionHandler, callback);
	}

	public void send(
			ByteBuffer                      bytes,
			ChannelDispatchExceptionHandler exceptionHandler,
			ChannelDispatchWriteCallback    callback
		) throws IOException
	{
		Transmission transmission         = new Transmission(bytes, exceptionHandler, callback);
		transmissions.add(transmission);
		registerTx();
	}
	
	public ChannelDispatchMonitor expect(int length, ChannelDispatchReadCallback callback) {
		return expect(length, defaultExceptionHandler, callback);
	}
	
	public ChannelDispatchMonitor expect(int length, ChannelDispatchExceptionHandler exceptionHandler, ChannelDispatchReadCallback callback) {
		Expectation expectation = new Expectation(length, exceptionHandler, callback);
		expectations.add(expectation);
		
		if(expectations.size() == 1) {
			registerRx();
		}
		
		return this;
	}
	
	protected void finishExpectation() {
		expectations.remove();
	}
	
	protected void finishTransmission() {
		transmissions.remove();
	}
	
	protected void registerTx() {
		if(txDispatchKey     != null) return;
		txDispatchKey         = registerKey(SelectionKey.OP_WRITE);
	}
	
	protected void registerRx() {
		if(rxDispatchKey     != null) return;
		rxDispatchKey         = registerKey(SelectionKey.OP_READ);
	}
	
	protected void registerConnect() {
		if(connectDispatchKey != null) return;
		connectDispatchKey    = registerKey(SelectionKey.OP_CONNECT);
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
	
	protected synchronized void doTx() {
		if(transmissions.isEmpty())  return;
		if(closed())                 return;
		txStartTime = Util.currentTimeMillis();
		
		try {
			while(canWrite()) {
				transmissions.peek().write();
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
	
	protected synchronized void doConnect() {
		try {
			connectDispatchKey.cancel();
			connectDispatchKey = null;
			
			if(connectedCallback != null) connectedCallback.connected();
		} catch(Exception exc) {
			logger.error("ChannelDispatch {}: Caught exception finishing connection", name, exc);
			try {
				close();
			} catch(IOException exc2) {
				logger.error("ChannelDispatch {}: Caught exception closing socket after catching exception finishing connection", name, exc2);
			}
		}
	}
	
	protected boolean canWrite() throws IOException {
		if(closed())                    return false;
		if(transmissions.isEmpty())     return false;
		
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
