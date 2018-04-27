package com.acrescrypto.zksync.net;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.MetaInputStream;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;

public class PeerMessage {
	public final static byte FLAG_FINAL = 0x01; // no further messages should be sent/expected in this message ID
	
	protected PeerSocket socket;
	protected InputStream txPayload;
	protected MetaInputStream rxPayload;
	protected int msgId;
	protected byte cmd;
	protected byte flags;
	
	protected ByteBuffer txBuf;
	protected boolean txEOF;
	protected boolean isRequest;
	
	public static int headerLength() {
		return 4 + 4 + 1 + 1 + 2; // msgId[4] + payloadLen[4] + cmd[1] + flags[1] + reserved[2]
	}
	
	protected interface PeerMessageHandler {
		public void handle(ByteBuffer payload) throws ProtocolViolationException;
	}
	
	public PeerMessage(PeerSocket socket, byte cmd, InputStream txPayload) {
		this.socket = socket;
		this.cmd = cmd;
		this.txPayload = txPayload;
		this.msgId = socket.issueMessageId();
		this.isRequest = true;
		runTxThread();
	}
	
	public PeerMessage(PeerSocket socket, byte cmd, int msgId) {
		this.socket = socket;
		this.cmd = cmd;
		this.isRequest = false;
		txBuf = ByteBuffer.allocate(headerLength() + socket.maxPayloadSize());
	}
	
	public void addPayload(byte[] payload) {
		ByteArrayInputStream stream = new ByteArrayInputStream(payload);
		stream.skip(headerLength());
		rxPayload.add(stream);
		if((payload[5] & FLAG_FINAL) != 0) rxPayload.finish();
	}
	
	public synchronized boolean txReady() {
		return(txEOF || txBuf.remaining() == 0);
	}
	
	public boolean txClosed() {
		return txEOF;
	}
	
	public void runTxThread() {
		new Thread(() -> {
			while(!txEOF) {
				waitForTxBufCapacity();
				loadTxBuf();
				addTxHeader();
				socket.dataReady(this);
			}
		}).start();
	}
	
	public void waitForTxBufCapacity() {
		while(txBuf.remaining() == 0) {
			try {
				txBuf.wait();
			} catch (InterruptedException exc) {}
		}
	}
	
	public void loadTxBuf() {
		try {
			while(txBuf.remaining() > 0 && !txEOF) {
				int r = txPayload.read(txBuf.array(), txBuf.position(), txBuf.remaining());
				if(r == -1) {
					txEOF = true;
				} else {
					txBuf.position(txBuf.position() + r);
				}
			}
		} catch (IOException exc) {
			socket.ioexception(exc);
		}
	}
	
	public void addTxHeader() {
		ByteBuffer headerBuf = ByteBuffer.wrap(txBuf.array());
		headerBuf.putInt((byte) (msgId | (isRequest ? 0x00000000 : 0x80000000)));
		headerBuf.putInt(txBuf.position()-headerLength());
		headerBuf.put(cmd);
		headerBuf.put((byte) (flags | (txEOF ? FLAG_FINAL : 0x00)));
		headerBuf.putShort((short) 0);
	}
	
	public synchronized void clearTxBuf() {
		txBuf.position(headerLength());
		txBuf.notifyAll();
	}
	
	public void respond(InputStream payload) {
		respond((byte) 0, payload);
	}
	
	public void respond(byte flags, InputStream payload) {
		this.txPayload = payload;
		runTxThread();
	}
	
	public void respondUnsupported() {
		// TODO P2P: how to signal this?
	}
	
	/** Accumulate all data for a message in memory in a separate thread, and invoke handler when message finished. */
	public void await(PeerMessageHandler handler) {
		new Thread(() -> {
			ByteBuffer buf = ByteBuffer.allocate(64*1024);
			try {
				while(true) {
					int len = rxPayload.read(buf.array(), buf.position(), buf.remaining());
					if(len == -1) {
						buf.position(0);
						handler.handle(buf);
						return;
					}
					
					buf.position(buf.position() + len);
					if(!buf.hasRemaining() && rxPayload.available() > 0) {
						ByteBuffer newBuf = ByteBuffer.allocate(2*buf.capacity());
						newBuf.put(buf.array());
						buf = newBuf;
					}
				}
			} catch (IOException e) {
				return; // connection died before this message was completed
			} catch (ProtocolViolationException e) {
				socket.violation();
			}
		}).start();
	}
}
