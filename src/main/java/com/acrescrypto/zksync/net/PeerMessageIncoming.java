package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.net.PeerConnection.PeerCapabilityException;
import com.acrescrypto.zksync.utility.Util;

public class PeerMessageIncoming extends PeerMessage {
	public interface PeerMessageIncomingReceivedDataCallback {
		void received(ByteBuffer data, boolean isEOF) throws IOException, ProtocolViolationException, PeerCapabilityException;
	}

	public interface PeerMessageIncomingReceivedDataSemiCallback {
		void received(ByteBuffer data) throws IOException, ProtocolViolationException, PeerCapabilityException;
	}
	
	public interface PeerMessageIncomingFinishedCallback {
		void finished() throws IOException, ProtocolViolationException;
	}

	protected class Expectation {
		protected PeerMessageIncomingReceivedDataCallback readCallback;
		protected ByteBuffer                              buffer;
		protected int                                     length;
		protected boolean                                 standing;
		
		public Expectation(
			int length,
			PeerMessageIncomingReceivedDataCallback readCallback)
		{
			this.readCallback = readCallback;
			this.length = length;
		}
		
		public boolean receivedData(ByteBuffer payload) throws IOException, ProtocolViolationException, PeerCapabilityException {
			if(buffer == null) {
				if(payload.remaining() >= length) {
					/* If we haven't read anything yet, and the incoming buffer has all
					 * our bytes, just pass it directly to save (allo + dupli)cation.
					 */
					boolean isEof = isFinished()
							     && expectations.size() == 1;
					this.readCallback.received(payload, isEof);
					return !standing;
				}
				
				// It's possible to get an empty frame; we can safely stop here if that happens
				if(!payload.hasRemaining()) return false;
				
				buffer = ByteBuffer.allocate(length);
			}
			
			buffer.put(payload);
			
			if(!buffer.hasRemaining()) {
				// EOF flag set at most once per message, on its final callback invocation.
				boolean isEof = isFinished()
						     && expectations.size() == 1;
				this.readCallback.received(buffer, isEof);
				this.buffer = null;
				return !standing;
			}
			
			return false;
		}
	}
	
	protected boolean                                 finished;
	protected long                                    lastSeen;
	protected int                                     bytesReceived;
	protected Queue<ByteBuffer>                       queuedBytes       = new LinkedList<>();
	protected Queue<Expectation>                      expectations      = new LinkedList<>();
	protected PeerMessageIncomingFinishedCallback     finishedCallback;

	protected Logger logger = LoggerFactory.getLogger(PeerMessageIncoming.class);

	public PeerMessageIncoming(
			PeerConnection connection,
			          byte cmd,
			          byte flags,
			           int msgId)
	{
		this.connection   = connection;
		this.cmd          = cmd;
		this.flags        = flags;
		this.msgId        = msgId;
		this.lastSeen     = Util.currentTimeMillis();
		processThread();
	}
	
	public void begin() throws ProtocolViolationException {
		connection.handle(this);
	}
	
	public void receivedData(
			          byte flags,
			    ByteBuffer payload)
	                throws IOException, ProtocolViolationException, PeerCapabilityException
	{
		bytesReceived    += payload.remaining();
		this.lastSeen     = Util.currentTimeMillis();
		flags            |= this.flags;
		boolean isFinal   = (flags & FLAG_FINAL) != 0;
		if(isFinal)         markFinished();
		
		do {
			Expectation exp   = expectations.peek();
			if(exp == null)     return;
			
			boolean done      = exp.receivedData(payload);
			if(done)            expectations.poll();
		} while(payload.hasRemaining());
		
		if(isFinal && finishedCallback != null) {
			finishedCallback.finished();
		}
	}
	
	public void expect(
			int length,
			PeerMessageIncomingReceivedDataCallback callback
	) {
		Expectation exp = new Expectation(length, callback);
		if(expectations.size() > 0 && expectations.peek().standing) {
			// expect after a keepExpecting preempts the keepExpecting
			((LinkedList<Expectation>) expectations).push(exp);
		} else {
			expectations.add(exp);
		}
	}
	
	public void expect(
			int length,
			PeerMessageIncomingReceivedDataSemiCallback callback
	) {
		expectations.add(new Expectation(length, (data, isEOF)->callback.received(data)));
	}
	
	public void keepExpecting(
			int length,
			PeerMessageIncomingReceivedDataCallback callback
	) {
		Expectation exp = new Expectation(length, callback);
		exp.standing    = true;
		
		expectations.add(exp);
	}

	public void keepExpecting(
			int length,
			PeerMessageIncomingReceivedDataSemiCallback callback
	) {
		Expectation exp = new Expectation(length, (data, isEOF)->callback.received(data));
		exp.standing    = true;
		
		expectations.add(exp);
	}
	
	public void requireFinish(PeerMessageIncomingFinishedCallback callback) throws IOException, ProtocolViolationException {
		if(isFinished()) {
			callback.finished();
			return;
		}
		
		expectations.clear();
		expect(0, (data, isEOF)->{
			if(data.hasRemaining()) throw new ProtocolViolationException();
			if(!isEOF)              throw new ProtocolViolationException();
			callback.finished();
		});
	}
	
	public void onFinish(PeerMessageIncomingFinishedCallback callback) throws IOException, ProtocolViolationException {
		if(isFinished()) {
			callback.finished();
		} else {
			finishedCallback = callback;
		}
	}
	
	public boolean isFinished() {
		return finished;
	}
	
	public void close() throws IOException, ProtocolViolationException {
		if(!isFinished()) {
			connection.getSocket().rejectMessage(msgId, cmd);
		}
		
		markFinished();
	}
	
	protected void markFinished() {
		finished = true;
		try {
			connection.socket.finishedMessage(this);
		} catch(IOException exc) {
			logger.warn("Caught exception marking message {} as finished", msgId, exc);
		}
	}
	
	protected void processThread() {
		try {
			connection.socket.threadPool.submit(()->{
				Util.setThreadName("PeerMessageIncoming process thread");
				try {
					connection.handle(this);
				} catch(ProtocolViolationException exc) {
					logger.warn("Peer message handler for {} encountered protocol violation", connection.socket.getAddress(), exc);
					connection.socket.violation();
				} catch(Exception exc) {
					logger.error("Peer message handler thread for {} encountered exception", connection.socket.getAddress(), exc);
				}
				
				markFinished();
			});
		} catch(RejectedExecutionException exc) {}
	}
}
