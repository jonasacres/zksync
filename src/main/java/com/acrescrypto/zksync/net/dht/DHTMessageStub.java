package com.acrescrypto.zksync.net.dht;

import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageCallback;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;

public class DHTMessageStub {
	protected DHTMessageCallback callback;
	protected SnoozeThread       expirationMonitor;
	protected DHTMessage         msg;
	protected int                responsesReceived;
	
	public DHTMessageStub(DHTMessage msg) {
		int messageRetryTimeMs = msg.peer.client.getMaster().getGlobalConfig().getInt("net.dht.messageRetryTimeMs");
		
		this.msg = msg;
		this.callback          = msg.callback;
		this.expirationMonitor = new SnoozeThread(
				                   messageRetryTimeMs,
				                   false,
				                   ()->retry());
	}
	
	public boolean matchesMessage(DHTMessage msg) {
		return this.msg.msgId ==    msg.msgId
			&& this.msg.cmd   ==    msg.cmd
			&& this.msg.peer.equals(msg.peer);
	}
	
	public void dispatchResponse(DHTMessage msg) throws ProtocolViolationException {
		this.expirationMonitor.cancel();
		
		responsesReceived++;
		if(responsesReceived >= msg.numExpected) {
			msg.isFinal = true;
		}
		
		if(callback != null) {
			callback.responseReceived(msg);
		}
	}
	
	public void retry() {
		msg.peer.client.getSocketManager().sendDatagram(msg.prepareRequestDatagram());
		int messageExpirationTimeMs = msg.peer.client.getMaster().getGlobalConfig().getInt("net.dht.messageExpirationTimeMs"),
			messageRetryTimeMs      = msg.peer.client.getMaster().getGlobalConfig().getInt("net.dht.messageRetryTimeMs");
		expirationMonitor = new SnoozeThread(
				messageExpirationTimeMs - messageRetryTimeMs,
				false,
				()->fail());
	}
	
	public void fail() {
		msg.peer.client.getProtocolManager().missedResponse(this);
	}
	
	public String toString() {
		return String.format("DHTMessageStub peer-%s, cmd=%d, msgId=%08x",
				Util.formatPubKey(msg.peer.getKey()),
				msg.cmd,
				msg.msgId);
	}
}
