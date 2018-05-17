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
	protected ByteBuffer txBuf;
	protected boolean txEOF;
	protected InputStream txPayload;
	protected RefTag refTag;
	protected Queue<Integer> chunkList = new LinkedList<Integer>();
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

	protected void runTxThread() {
		new Thread(() -> {
			try {
				while(!txEOF) {
					resizeBuffer(txPayload.available());
					loadBytesFromStream();
					
					if(connection.isPausable(cmd)) {
						connection.waitForUnpause();
					}
					
					connection.socket.dataReady(this);
				}
			} catch(Exception exc) {
				logger.error("Outgoing message thread to {} caught exception", connection.socket.getAddress(), exc);
			}
		}).start();
	}
	
	protected void loadBytesFromStream() {
		if(txEOF) return;
		synchronized(this) {
			while(!txBuf.hasRemaining()) {
				try {
					this.wait();
				} catch(InterruptedException exc) {}
			}
		}
		
		int readLen;
		try {
			readLen = txPayload.read(txBuf.array(), txBuf.position(), txBuf.remaining());
		} catch(IOException exc) {
			readLen = -1;
		}
		
		if(readLen > 0) {
			synchronized(this) {
				txBuf.position(txBuf.position() + readLen);
				addTxHeader();
			}
		} else if(readLen < 0) {
			txEOF = true;
		}
	}
	
	protected void addTxHeader() {
		ByteBuffer headerBuf = ByteBuffer.wrap(txBuf.array());
		headerBuf.putInt(msgId);
		headerBuf.putInt(txBuf.position()-HEADER_LENGTH);
		headerBuf.put(cmd);
		headerBuf.put((byte) (flags | (txEOF ? FLAG_FINAL : 0x00)));
		headerBuf.putShort((short) 0);
	}
	
	protected int minPayloadBufferSize() {
		return 256;
	}
	
	protected int maxPayloadBufferSize() {
		return connection.socket.maxPayloadSize();
	}
	
	protected synchronized void resizeBuffer(int numBytesRequested) {
		// buffer has header + payload space. payload space is requested amount, with a hard minimum of 256 and max of socket's maxPayloadSize
		int bufferSize = (txBuf == null ? 0 : txBuf.position()) + numBytesRequested;
		bufferSize = Math.min(bufferSize, maxPayloadBufferSize()); // no bigger than max size
		bufferSize = Math.max(bufferSize, minPayloadBufferSize()); // no smaller than min size
		bufferSize += HEADER_LENGTH;
			
		// never shrink the buffer; start small but assume if we had big bursts before, we'll see big bursts again
		if(txBuf == null || txBuf.capacity() < bufferSize) {
			ByteBuffer newTxBuf = ByteBuffer.allocate(bufferSize);
			if(txBuf != null) newTxBuf.put(txBuf.array(), 0, txBuf.position());
			else newTxBuf.position(HEADER_LENGTH);
			txBuf = newTxBuf;
		}
	}
	
	protected synchronized void clearTxBuf() {
		txBuf.position(HEADER_LENGTH);
		this.notifyAll();
	}
}
