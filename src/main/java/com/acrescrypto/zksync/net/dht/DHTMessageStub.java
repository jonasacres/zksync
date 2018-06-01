package com.acrescrypto.zksync.net.dht;

import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageCallback;
import com.acrescrypto.zksync.utility.SnoozeThread;

public class DHTMessageStub {
	DHTPeer peer;
	DHTMessageCallback callback;
	SnoozeThread expirationMonitor;
	
	byte cmd;
	int msgId;
	int responsesReceived;
	
	public DHTMessageStub(DHTMessage msg) {
		this.peer = msg.peer;
		this.cmd = msg.cmd;
		this.msgId = msg.msgId;
		this.expirationMonitor = new SnoozeThread(DHTClient.MESSAGE_EXPIRATION_TIME_MS, false, ()->{peer.client.missedResponse(this);});
		this.callback = msg.callback;
	}
	
	public boolean dispatchResponseIfMatches(DHTMessage msg) throws ProtocolViolationException {
		if(!(msgId == msg.msgId && cmd == msg.cmd && peer.equals(msg.peer))) return false;
		this.expirationMonitor.cancel();
		
		responsesReceived++;
		if(responsesReceived >= msg.numExpected) {
			msg.isFinal = true;
		}
		
		callback.responseReceived(msg);
		return true;
	}
}
