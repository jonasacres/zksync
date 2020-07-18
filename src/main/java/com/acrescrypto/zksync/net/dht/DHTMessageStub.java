package com.acrescrypto.zksync.net.dht;

import java.net.DatagramPacket;

import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageCallback;
import com.acrescrypto.zksync.utility.SnoozeThread;

public class DHTMessageStub {
	protected DHTPeer            peer;
	protected DHTMessageCallback callback;
	protected SnoozeThread       expirationMonitor;
	protected DatagramPacket     packet;
	
	protected byte               cmd;
	protected int                msgId;
	protected int                responsesReceived;
	
	public DHTMessageStub(DHTMessage msg, DatagramPacket packet) {
		int messageRetryTimeMs = msg.peer.client.getMaster().getGlobalConfig().getInt("net.dht.messageRetryTimeMs");
		
		this.peer              = msg.peer;
		this.cmd               = msg.cmd;
		this.msgId             = msg.msgId;
		this.callback          = msg.callback;
		this.packet            = packet;
		this.expirationMonitor = new SnoozeThread(
				                   messageRetryTimeMs,
				                   false,
				                   ()->retry());
	}
	
	public boolean matchesMessage(DHTMessage msg) {
		return msgId ==    msg.msgId
			&& cmd   ==    msg.cmd
			&& peer.equals(msg.peer);
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
		peer.client.getSocketManager().sendDatagram(packet);
		int messageExpirationTimeMs = peer.client.getMaster().getGlobalConfig().getInt("net.dht.messageExpirationTimeMs"),
			messageRetryTimeMs      = peer.client.getMaster().getGlobalConfig().getInt("net.dht.messageRetryTimeMs");
		expirationMonitor = new SnoozeThread(
				messageExpirationTimeMs - messageRetryTimeMs,
				false,
				()->fail());
	}
	
	public void fail() {
		peer.client.getProtocolManager().missedResponse(this);
	}
}
