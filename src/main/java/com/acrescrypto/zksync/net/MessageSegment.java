package com.acrescrypto.zksync.net;

import java.nio.ByteBuffer;

public class MessageSegment {
	PeerMessageOutgoing msg;
	byte flags;
	ByteBuffer content;
	boolean delivered;
	
	public MessageSegment(PeerMessageOutgoing msg, byte flags, ByteBuffer content) {
		this.msg = msg;
		this.flags = flags;
		this.content = content;
		addHeader();
	}
	
	// Used for test purposes
	protected MessageSegment(int msgId, byte cmd, byte flags, ByteBuffer content) {
		this.msg = new PeerMessageOutgoing(null, msgId, cmd, flags, null);
		this.content = content;
		this.flags = flags;
		addHeader();
	}
	
	protected void addHeader() {
		ByteBuffer headerBuf = ByteBuffer.wrap(content.array());
		headerBuf.putInt(msg.msgId);
		headerBuf.putInt(content.limit() - PeerMessage.HEADER_LENGTH);
		headerBuf.put(msg.cmd);
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

	public void assignMsgId(int msgId) {
		msg.msgId = msgId;
		addHeader();
	}
}
