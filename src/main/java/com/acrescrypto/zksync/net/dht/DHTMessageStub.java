package com.acrescrypto.zksync.net.dht;

import java.net.DatagramPacket;

import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageCallback;
import com.acrescrypto.zksync.utility.SnoozeThread;

public class DHTMessageStub {
	DHTPeer peer;
	DHTMessageCallback callback;
	SnoozeThread expirationMonitor;
	DatagramPacket packet;
	
	byte cmd;
	int msgId;
	int responsesReceived;
	
	public DHTMessageStub(DHTMessage msg, DatagramPacket packet) {
		this.peer = msg.peer;
		this.cmd = msg.cmd;
		this.msgId = msg.msgId;
		this.packet = packet;
		this.expirationMonitor = new SnoozeThread(DHTClient.MESSAGE_RETRY_TIME_MS, false, ()->retry());
		this.callback = msg.callback;
	}
	
	public boolean dispatchResponseIfMatches(DHTMessage msg) throws ProtocolViolationException {
		if(!(msgId == msg.msgId && cmd == msg.cmd && peer.equals(msg.peer))) return false;
		this.expirationMonitor.cancel();
		
		responsesReceived++;
		if(responsesReceived >= msg.numExpected) {
			msg.isFinal = true;
		}
		
		if(callback != null) {
			callback.responseReceived(msg);
		}
		
		return true;
	}
	
	public void retry() {
		peer.client.sendDatagram(packet);
		expirationMonitor = new SnoozeThread(DHTClient.MESSAGE_EXPIRATION_TIME_MS - DHTClient.MESSAGE_RETRY_TIME_MS, false, ()->fail());
	}
	
	public void fail() {
		peer.client.missedResponse(this);
	}
}
