package com.acrescrypto.zksync.utility.channeldispatcher;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.Util;

public class ChannelDispatch {
	protected ConcurrentHashMap <ChannelDispatchMonitor,Boolean>    activeRxMonitors     = new ConcurrentHashMap<>(),
			                                                        activeTxMonitors     = new ConcurrentHashMap<>();
	protected ConcurrentHashMap <ChannelDispatchAcceptor,Boolean>   activeAcceptMonitors = new ConcurrentHashMap<>();
            
	protected Selector          selector;
	protected GroupedThreadPool workerPool;
	protected boolean           running;
	

	private   Logger            logger = LoggerFactory.getLogger(ChannelDispatch.class);
	
	public ChannelDispatch(GroupedThreadPool workerPool) throws IOException {
		this.workerPool = workerPool;
		this.selector   = Selector.open();
		this.running    = true;
		
		start();
	}
	
	public Selector getSelector() {
		return selector;
	}
	
	public void stop() {
		running = false;
	}
	
	public void start() {
		running = true;
		new Thread(()->selectThread()).start();
	}
	
	protected void selectThread() {
		Util.setThreadName("ChannelDispatch selector");
		logger.debug("Starting ChannelDispatch selector thread");
		
		while(running) {
			try {
				select();
			} catch(Exception exc) {
				logger.error("Uncaught exception in ChannelDispatch select thread", exc);
			}
		}

		logger.debug("Stopping ChannelDispatch selector thread");
	}
	
	protected void select() {
		try {
			selector.select();
			Set<SelectionKey> selected = selector.selectedKeys();
			
			for(SelectionKey key : selected) {
				if(key.isAcceptable()) {
					activateMonitorForAccept((ChannelDispatchAcceptor) key.attachment());
				}
				
				if(key.isWritable()) {
					activateMonitorForTx((ChannelDispatchMonitor) key.attachment());
				}
				
				if(key.isReadable()) {
					activateMonitorForRx((ChannelDispatchMonitor) key.attachment());
				}
			}
		} catch(IOException exc) {
			logger.warn("ChannelDispatch: Caught exception when selecting RX channels", exc);
		}
	}
	
	protected void activateMonitorForAccept(ChannelDispatchAcceptor monitor) {
		if(activeAcceptMonitors.get(monitor)) return; // monitor has active accept worker
		activeAcceptMonitors.put(monitor, true);
		monitor.unregisterAccept();
		
		workerPool.submit(()->{
			try {
				monitor.doAccept();
			} finally {
				activeAcceptMonitors.remove(monitor);
			}
		});
	}
	
	protected void activateMonitorForTx(ChannelDispatchMonitor monitor) {
		if(activeTxMonitors.get(monitor)) return; // monitor has active tx worker
		activeTxMonitors.put(monitor, true);
		monitor.unregisterTx();
		
		workerPool.submit(()->{
			try {
				monitor.doTx();
			} finally {
				activeTxMonitors.remove(monitor);
			}
		});
	}
	
	protected void activateMonitorForRx(ChannelDispatchMonitor monitor) {
		if(activeRxMonitors.get(monitor)) return; // monitor has active rx worker
		activeRxMonitors.put(monitor, true);
		monitor.unregisterRx();
		
		workerPool.submit(()->{
			try {
				monitor.doRx();
			} finally {
				activeRxMonitors.remove(monitor);
			}
		});
	}
	
	/** How long (in ms) can a monitor rx/tx handler hold onto a worker thread before it
	 *  must return and go through the select/queuing process again?
	 */
	protected int maxWorkerThreadDuration() {
		return 10;
	}
}
