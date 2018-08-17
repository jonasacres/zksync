package com.acrescrypto.zksync.net;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.fs.zkfs.RefTag;

public class PeerMessageOutgoing extends PeerMessage {
	protected boolean txEOF, aborted;
	protected InputStream txPayload;
	protected RefTag refTag;
	protected Queue<Integer> chunkList = new LinkedList<Integer>();
	protected MessageSegment queuedSegment;
	private Logger logger = LoggerFactory.getLogger(PeerMessageOutgoing.class);
	
	public PeerMessageOutgoing(PeerConnection connection, int msgId, byte cmd, byte flags, InputStream txPayload) {
		this.connection = connection;
		this.cmd = cmd;
		this.flags = flags;
		this.txPayload = txPayload;
		this.msgId = msgId;
		if(connection != null) runTxThread(); // allow null spec for test purposes
	}
	
	public PeerMessageOutgoing(PeerConnection connection, byte cmd, InputStream txPayload) {
		this(connection, Integer.MIN_VALUE, cmd, (byte) 0, txPayload);
	}
	
	public synchronized void abort() {
		aborted = true;
		if(queuedSegment != null) {
			queuedSegment.delivered();
		}
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
		try {
			connection.socket.threadPool.submit(() -> {
				Thread.currentThread().setName("PeerMessageOutgoing tx thread cmd=" + cmd);
				try {
					while(!txClosed() && !aborted) {
						if(queuedSegment != null) queuedSegment.waitForDelivery();
						accumulateNext();
						
						if(connection.isPausable(cmd)) {
							connection.waitForUnpause();
						}
						
						if(queuedSegment != null && !aborted) {
							sendNext();
						}
					}
				} catch(Exception exc) {
					logger.error("Outgoing message thread to {} caught exception", connection.socket.getAddress(), exc);
				}
			});
		} catch(RejectedExecutionException exc) {
			logger.info("Canceled TX thread start because thread pool is shut down");
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
		while(!aborted) {
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

			if(r < 0 || txReachedEOF()) {
				txEOF = true;
				if(r < 0) break;
			}
			
			buffer.position(buffer.position() + r);
			
			// grab more data if available
			if(txPayload.available() > 0) continue;
			try { Thread.sleep(1); } catch(InterruptedException exc) {}
			if(txPayload.available() <= 0) break;
		}
		
		buffer.limit(buffer.position());
		buffer.rewind();
		
		byte segmentFlags = (byte) (flags | (txClosed() ? FLAG_FINAL : 0x00));
		synchronized(this) {
			if(aborted) return;
			queuedSegment = new MessageSegment(this, segmentFlags, buffer);
		}
	}
	
	protected boolean txReachedEOF() throws IOException {
		if(txEOF) return true;
		if(txPayload instanceof ByteArrayInputStream && txPayload.available() <= 0) return true;
		return false;
	}
	
	protected void sendNext() throws IOException {
		connection.socket.dataReady(queuedSegment);
	}
	
	public boolean equals(Object other) {
		if(other instanceof Integer) {
			return other.equals(msgId);
		}
		
		if(other instanceof PeerMessageOutgoing) {
			return ((PeerMessageOutgoing) other).msgId == msgId;
		}
		
		return false;
	}
}
