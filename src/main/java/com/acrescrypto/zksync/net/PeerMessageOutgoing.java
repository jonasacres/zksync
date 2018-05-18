package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.fs.zkfs.RefTag;

public class PeerMessageOutgoing extends PeerMessage {
	protected class MessageSegment {
		PeerMessageOutgoing msg;
		ByteBuffer content;
		boolean delivered;
		
		public MessageSegment(PeerMessageOutgoing msg, ByteBuffer content) {
			this.msg = msg;
			this.content = content;
			addHeader();
		}
		
		protected void addHeader() {
			ByteBuffer headerBuf = ByteBuffer.wrap(content.array());
			headerBuf.putInt(msgId);
			headerBuf.putInt(content.limit() - PeerMessage.HEADER_LENGTH);
			headerBuf.put(cmd);
			headerBuf.put((byte) (flags | (msg.txClosed() ? FLAG_FINAL : 0x00)));
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
	
	protected boolean txEOF;
	protected InputStream txPayload;
	protected RefTag refTag;
	protected Queue<Integer> chunkList = new LinkedList<Integer>();
	protected MessageSegment queuedSegment;
	private Logger logger = LoggerFactory.getLogger(PeerMessageOutgoing.class);
	
	public PeerMessageOutgoing(PeerConnection connection, byte cmd, InputStream txPayload) {
		this.connection = connection;
		this.cmd = cmd;
		this.txPayload = txPayload;
		this.msgId = connection.socket.issueMessageId();
		runTxThread();
	}

	public boolean txClosed() {
		return txEOF;
	}
	
	public int minPayloadBufferSize() {
		return 256;
	}
	
	public int maxPayloadBufferSize() {
		return PeerMessage.MESSAGE_SIZE;
	}

	protected void runTxThread() {
		new Thread(() -> {
			try {
				while(!txClosed()) {
					if(queuedSegment != null) queuedSegment.waitForDelivery();
					accumulateNext();
					
					if(connection.isPausable(cmd)) {
						connection.waitForUnpause();
					}
					
					if(queuedSegment != null) {
						sendNext();
					}
				}
			} catch(Exception exc) {
				logger.error("Outgoing message thread to {} caught exception", connection.socket.getAddress(), exc);
			}
		}).start();
	}
	
	protected void waitForSend() {
		if(queuedSegment == null) return;
		while(!queuedSegment.delivered) {
			try {
				synchronized(queuedSegment) {
					queuedSegment.wait();
				}
			} catch(InterruptedException exc) {}
		}
	}
	
	protected void accumulateNext() throws IOException {
		int startingSize = minPayloadBufferSize();
		try {
			startingSize = Math.min(maxPayloadBufferSize(), Math.max(startingSize, txPayload.available()));
		} catch(IOException exc) {
			txEOF = true;
		}
		
		ByteBuffer buffer = ByteBuffer.allocate(startingSize);
		buffer.position(HEADER_LENGTH);
		while(true) {
			if(!buffer.hasRemaining()) {
				if(buffer.capacity() == maxPayloadBufferSize()) break;
				int newCapacity = Math.min(2*buffer.capacity(), maxPayloadBufferSize());
				ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
				newBuffer.put(buffer.array());
				buffer = newBuffer;
			}
			
			int r;
			
			try {
				r = txPayload.read(buffer.array(), buffer.position(), buffer.remaining());
			} catch(IOException exc) {
				r = -1;
			}

			if(r < 0) {
				txEOF = true;
				break;
			}
			
			buffer.position(buffer.position() + r);
			if(txPayload.available() > 0) continue;
			try { Thread.sleep(1); } catch(InterruptedException exc) {}
			if(txPayload.available() == 0) break;
		}
		
		buffer.limit(buffer.position());
		buffer.rewind();
		
		queuedSegment = new MessageSegment(this, buffer);
	}
	
	protected void sendNext() throws IOException {
		connection.socket.dataReady(queuedSegment);
	}
}
