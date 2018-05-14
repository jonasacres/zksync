package com.acrescrypto.zksync.net;

import java.io.EOFException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.ProtocolViolationException;

public class PeerMessageIncoming extends PeerMessage {
	protected DataBuffer rxBuf;
	protected PeerMessageHandler handler;
	protected final Logger logger = LoggerFactory.getLogger(PeerMessageIncoming.class);
	
	protected class DataBuffer {
		protected ByteBuffer buf = ByteBuffer.allocate(PeerMessage.MESSAGE_SIZE);
		protected ByteBuffer readBuf = ByteBuffer.wrap(buf.array());
		protected boolean eof;
		
		public synchronized void write(byte[] data) {
			if(buf.remaining() < data.length) {
				resizeBuffer(data.length);
			}
			
			buf.put(data);
			readBuf.limit(buf.position());
			this.notifyAll();
		}
		
		public boolean isEOF() {
			return eof;
		}
		
		public synchronized void setEOF() {
			eof = true;
			this.notifyAll();
		}
		
		public synchronized void waitForEOF() {
			while(!eof) {
				try {
					this.wait();
				} catch (InterruptedException e) {}
			}
		}
		
		public synchronized void requireEOF() throws ProtocolViolationException {
			if(buf.remaining() > 0) throw new ProtocolViolationException();
			waitForEOF();
			if(buf.remaining() > 0) throw new ProtocolViolationException();
		}
		
		public byte get() throws EOFException {
			return read(1)[0];
		}
		
		public void get(byte[] buf) throws EOFException {
			read(buf);
		}
		
		public int getInt() throws EOFException {
			return ByteBuffer.wrap(read(4)).getInt();
		}
		
		public short getShort() throws EOFException {
			return ByteBuffer.wrap(read(2)).getShort();
		}
		
		public long getLong() throws EOFException {
			return ByteBuffer.wrap(read(8)).getLong();
		}
		
		public byte[] read() throws EOFException {
			return read(new byte[readBuf.remaining()]);
		}
		
		public byte[] read(int length) throws EOFException {
			return read(new byte[length]);
		}
		
		public synchronized byte[] read(byte[] data) throws EOFException {
			while(readBuf.remaining() < data.length && !eof) {
				try {
					this.wait();
				} catch (InterruptedException e) {}
			}
			
			if(readBuf.remaining() < data.length) throw new EOFException();
			readBuf.get(data);
			return data;
		}
		
		protected void resizeBuffer(int additionalSpaceNeeded) {
			int totalSpaceNeeded = buf.position() - readBuf.position() + additionalSpaceNeeded;
			ByteBuffer newBuf = ByteBuffer.allocate(totalSpaceNeeded);
			newBuf.put(readBuf.array(), readBuf.position(), buf.position()-readBuf.position());
			buf = newBuf;
			readBuf = ByteBuffer.wrap(buf.array());
			readBuf.limit(buf.position());
		}
	}
	
	protected interface PeerMessageHandler {
		public void handle(int id, DataBuffer chunk, boolean isFinal) throws ProtocolViolationException, EOFException;
	}
	
	public PeerMessageIncoming(PeerConnection connection, byte cmd, byte flags, int msgId) {
		this.connection = connection;
		this.cmd = cmd;
		this.flags = flags;
		this.msgId = msgId;
		processThread();
	}
	
	public void receivedData(byte flags, byte[] data) {
		flags |= this.flags;
		boolean isFinal = (flags & FLAG_FINAL) != 0;
		rxBuf.write(data);
		if(isFinal) {
			rxBuf.setEOF();
		}
	}
	
	protected void processThread() {
		new Thread(() -> {
			try {
				connection.handle(this);
			} catch(ProtocolViolationException exc) {
				logger.warn("Peer message handlker for {} encountered protocol violation", connection.socket.getAddress(), exc);
				connection.socket.violation();
			} catch(Exception exc) {
				logger.error("Peer message handler thread for {} encountered exception", connection.socket.getAddress(), exc);
			}
		}).start();;
	}
}
