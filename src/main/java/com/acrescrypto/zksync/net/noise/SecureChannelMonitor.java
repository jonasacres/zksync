package com.acrescrypto.zksync.net.noise;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.HashMap;
import java.util.LinkedList;

import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.Util;

public class SecureChannelMonitor {
	public interface ChannelReadCallback {
		void receivedData(ByteBuffer data);
	}
	
	protected class ChannelInfo {
		SelectionKey key;
		ChannelReadCallback callback;
		
		public ChannelInfo(SelectionKey key, ChannelReadCallback callback) throws IOException {
			this.key = key;
			this.callback = callback;
		}
	}
	
	protected HashMap<SecureChannel,ChannelInfo> channelInfo = new HashMap<>();
	protected GroupedThreadPool threadPool;
	protected Selector selector;
	protected boolean closed;
	
	public SecureChannelMonitor() throws IOException {
		threadPool = GroupedThreadPool.newWorkStealingThreadPool(
				Thread.currentThread().getThreadGroup(),
				"SecureChannelMonitor");
		selector = Selector.open();
	}
	
	public void addChannel(SecureChannel channel, ChannelReadCallback callback) throws IOException {
		channel.configureBlocking(false);
		SelectionKey key = channel.register(selector, SelectionKey.OP_READ);
		channelInfo.put(channel, new ChannelInfo(key, callback));
	}
	
	protected void monitorThread() {
		while(!closed) {
			try {
				checkChannels();
			} catch(IOException exc) {
				// TODO Noise: (exception) what to do about IOException in selector monitor thread?
				Util.sleep(10);
			}
		}
	}
	
	public void checkChannels() throws IOException {
		LinkedList<SecureChannel> toRemove = new LinkedList<>();
		
		selector.select();
		channelInfo.forEach((channel, info)->{
			if(!channel.isOpen()) {
				toRemove.add(channel);
			}
			if(!info.key.isReadable()) return;
			threadPool.submit(()->{
				ByteBuffer readBuf = ByteBuffer.allocate(SecureChannel.MAX_MESSAGE_LEN);
				try {
					channel.read(readBuf);
					if(readBuf.position() == 0) return;
					ByteBuffer limitedBuf = ByteBuffer.wrap(readBuf.array(), 0, readBuf.position());
					info.callback.receivedData(limitedBuf);
				} catch (IOException e) {
					// TODO Noise: (exception) what to do about IOException in callback?
				}
			});
		});
		
		for(SecureChannel channel : toRemove) {
			channelInfo.get(channel).key.cancel();
			channelInfo.remove(channel);
		}
	}
	
	public void close() throws IOException {
		closed = true;
		selector.close();
		for(SecureChannel channel : channelInfo.keySet()) {
			channel.close();
		}
	}
}
