package com.acrescrypto.zksync.net.messaging;

import java.nio.ByteBuffer;
import java.util.HashMap;

import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.net.noise.SecureChannel;
import com.acrescrypto.zksync.net.noise.SecureChannelMonitor.ChannelReadCallback;

public class MessagingConnection implements ChannelReadCallback {
	protected SecureChannel channel;
	protected long nextOutgoingId, maxIncomingId;
	protected HashMap<Long,IncomingMessage> incoming = new HashMap<>();
	
	public interface ReceivedMessageHandler {
		void receivedMessage(IncomingMessage message);
	}
	
	public void registerHandler(String app, ReceivedMessageHandler handler) {
	}
	
	public long issueOutgoingId() {
		return nextOutgoingId++;
	}
	
	protected IncomingMessage incomingMessageWithId(long messageId) {
		IncomingMessage msg = incoming.get(messageId);
		if(msg != null) return msg;
		
		if(messageId > maxIncomingId) {
			// issue a new IncomingMessage object
			maxIncomingId = messageId;
			msg = new IncomingMessage(this, messageId);
			incoming.put(messageId, msg);
			return msg;
		}
		
		// cancelled message; we don't want to hear it
		// TODO Noise: (implement) tell them to cut it out
		return null;
	}

	@Override
	public void receivedData(ByteBuffer data) {
		try {
			assertState(data.remaining() >= 8);
			
			long messageId = data.getLong();
			long mask = Long.MAX_VALUE; // 7fff...
			boolean isFinal = (messageId & mask) != 0;
			messageId &= mask;
			
			IncomingMessage msg = incomingMessageWithId(messageId);
			if(msg == null) return;
			msg.notifyReceivedBytes(isFinal, data);
		} catch(ProtocolViolationException exc) {
			// TODO Noise: (implement) disconnect and blacklist
		}
	}
	
	protected void finishedMessage(IncomingMessage msg) {
		incoming.remove(msg.messageId);
	}
	
	protected void assertState(boolean condition) throws ProtocolViolationException {
		if(!condition) {
			throw new ProtocolViolationException();
		}
	}
}
