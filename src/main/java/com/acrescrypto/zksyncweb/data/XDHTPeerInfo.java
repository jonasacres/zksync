package com.acrescrypto.zksyncweb.data;

import com.acrescrypto.zksync.net.dht.DHTPeer;

public class XDHTPeerInfo {
	private byte[]  id;
	private byte[]  pubKey;
	private Integer port;
	private String  address;
	
	private Integer missedMessages;
	private Long    lastSeen;
	private boolean pinned;
	
	private String status;
	
	public XDHTPeerInfo(DHTPeer peer) {
		this.id             = peer.getId().serialize();
		this.pubKey         = peer.getKey().getBytes();
		this.port           = peer.getPort();
		this.address        = peer.getAddress();
		this.missedMessages = peer.getMissedMessages();
		this.lastSeen       = peer.getLastSeen();
		this.pinned         = peer.isPinned();
		
		if       (peer.isBad()) {
			this.setStatus("bad");
		} else if(peer.isQuestionable()) {
			this.setStatus("questionable");
		} else {
			this.setStatus("good");
		}
	}
	
	public XDHTPeerInfo() {}

	public byte[] getId() {
		return id;
	}
	
	public void setId(byte[] id) {
		this.id = id;
	}

	public byte[] getPubKey() {
		return pubKey;
	}

	public void setPubKey(byte[] pubKey) {
		this.pubKey = pubKey;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public Integer getMissedMessages() {
		return missedMessages;
	}

	public void setMissedMessages(Integer missedMessages) {
		this.missedMessages = missedMessages;
	}

	public Long getLastSeen() {
		return lastSeen;
	}

	public void setLastSeen(Long lastSeen) {
		this.lastSeen = lastSeen;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public boolean isPinned() {
		return pinned;
	}

	public void setPinned(boolean pinned) {
		this.pinned = pinned;
	}
}
