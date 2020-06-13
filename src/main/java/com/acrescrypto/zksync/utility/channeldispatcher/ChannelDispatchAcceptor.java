package com.acrescrypto.zksync.utility.channeldispatcher;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksync.utility.channeldispatcher.ChannelDispatchMonitor.ChannelDispatchClosedCallback;

public class ChannelDispatchAcceptor {
	public interface ChannelDispatchAcceptCallback {
		void accept(SocketChannel channel);
	}
	
	protected String                        name;
	protected ServerSocketChannel           channel;
	protected SelectionKey                  acceptDispatchKey,
	                                        acceptLocalKey;
	protected Selector                      acceptSelector;
	protected boolean                       calledClosedCallback;
	protected ChannelDispatchClosedCallback closedCallback;
	protected ChannelDispatchAcceptCallback acceptCallback;
	protected ChannelDispatch               dispatch;
	protected long                          acceptStartTime;
	
	private   Logger                        logger = LoggerFactory.getLogger(ChannelDispatchAcceptor.class);

	public ChannelDispatchAcceptor(ChannelDispatch dispatch, String name, ServerSocketChannel channel) throws IOException {
		this.dispatch           = dispatch;
		this.name               = name;
		this.channel            = channel;
		
		this.acceptSelector = Selector.open();
		this.acceptLocalKey = channel.register(acceptSelector, SelectionKey.OP_ACCEPT);
		registerAccept();
	}
	
	public void setAcceptCallback(ChannelDispatchAcceptCallback acceptCallback) {
		if(this.acceptCallback == acceptCallback) return;
		this.acceptCallback = acceptCallback;
		
		if(this.acceptCallback != null) {
			registerAccept();
		} else {
			unregisterAccept();
		}
	}
	
	public void setClosedCallback(ChannelDispatchClosedCallback closedCallback) {
		this.closedCallback = closedCallback;
	}
	
	public synchronized void close() throws IOException {
		if(calledClosedCallback)   return;		
		calledClosedCallback     = true;
		
		unregisterAccept();
		acceptLocalKey.cancel();
		
		if(channel.isOpen())       channel.close();
		if(closedCallback != null) closedCallback.closed();
		
		closedCallback           = null;
		acceptCallback           = null;
	}
	
	public boolean closed() {
		return calledClosedCallback;
	}
	
	protected void registerAccept() {
		if(acceptDispatchKey != null) return;
		try {
			acceptDispatchKey = channel.register(
					dispatch.getSelector(),
					SelectionKey.OP_ACCEPT
				);
			acceptDispatchKey.attach(this);
		} catch (ClosedChannelException exc) {
			logger.info("ChannelDispatch {}: Caught exception registering channel for selection", name, exc);
			try {
				close();
			} catch (IOException exc2) {
				logger.error("ChannelDispatch {}: Caught exception closing channel after handling closed channel exception", name, exc);
				exc2.printStackTrace();
			}
			
			acceptDispatchKey = null;
		}
	}

	protected void unregisterAccept() {
		if(acceptDispatchKey == null) return;
		acceptDispatchKey.cancel();
		acceptDispatchKey = null;
	}

	protected synchronized void doAccept() {
		if(closed())              return;
		acceptStartTime = Util.currentTimeMillis();
		
		try {
			while(canAccept()) {
				SocketChannel peerChannel = channel.accept();
				logger.debug("ChannelDispatch {}: Accepted peer {}", name, peerChannel.getRemoteAddress().toString());
				if(acceptCallback != null) {
					acceptCallback.accept(peerChannel);
				}
			}
		} catch(IOException exc) {
			logger.error("ChannelDispatch {}: Caught exception accepting connection; closing", name, exc);
			try {
				close();
			} catch(IOException exc2) {
				logger.error("ChannelDispatch {}: Caught exception closing channel after handling exception accepting connection", name, exc2);
			}
		} finally {
			if(closed()) return;
			
			registerAccept();
		}
	}
	
	protected boolean canAccept() throws IOException {
		if(closed())                    return false;
		if(acceptCallback == null)      return false;
		
		long expiration = acceptStartTime + dispatch.maxWorkerThreadDuration();
		boolean expired = Util.currentTimeMillis() > expiration;
		if(expired)                     return false;
		
		return acceptSelector.selectNow() > 0;
	}
}
