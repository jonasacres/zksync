package com.acrescrypto.zksync.net;

import java.nio.ByteBuffer;

public class MessageSegment {
	int msgId;
	byte cmd, flags;
	ByteBuffer content;
	boolean delivered;
	
	public MessageSegment(int msgId, byte cmd, byte flags, ByteBuffer content) {
		this.msgId = msgId;
		this.cmd = cmd;
		this.flags = flags;
		this.content = content;
		addHeader();
	}
	
	protected void addHeader() {
		ByteBuffer headerBuf = ByteBuffer.wrap(content.array());
		headerBuf.putInt(msgId);
		headerBuf.putInt(content.limit() - PeerMessage.HEADER_LENGTH);
		headerBuf.put(cmd);
		headerBuf.put(flags);
		headerBuf.putShort((short) 0);
	}
	
	protected synchronized void delivered() {
		delivered = true;
		this.notifyAll();
	}
	
	protected synchronized void waitForDelivery() {
		while(!delivered) {
			try {
				this.wait();
			} catch (InterruptedException e) { }
		}
	}
}
