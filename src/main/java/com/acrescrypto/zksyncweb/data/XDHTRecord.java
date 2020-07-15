package com.acrescrypto.zksyncweb.data;

import com.acrescrypto.zksync.net.dht.DHTRecordStore.StoreEntry;

public class XDHTRecord {
	protected byte[]       data;
	protected String       routingInfo;
	protected String       id;
	protected XDHTPeerInfo sender;
	protected long         timeReceived;
	protected long         timeExpires;
	
	public XDHTRecord(StoreEntry recordEntry) {
		this.id           = recordEntry.record().id(recordEntry.crypto());
		this.data         = recordEntry.record().serialize();
		this.routingInfo  = recordEntry.record().routingInfo();
		this.sender       = recordEntry.record().getSender() != null
				            ? new XDHTPeerInfo(recordEntry.record().getSender())
				            : null;
		this.timeReceived = recordEntry.receivedTime();
		this.timeExpires  = recordEntry.expirationTime();
	}
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public byte[] getData() {
		return data;
	}
	
	public void setData(byte[] data) {
		this.data = data;
	}
	
	public String getRoutingInfo() {
		return routingInfo;
	}
	
	public void setRoutingInfo(String routingInfo) {
		this.routingInfo = routingInfo;
	}
	
	public long getTimeReceived() {
		return timeReceived;
	}
	
	public void setTimeReceived(long timeReceived) {
		this.timeReceived = timeReceived;
	}
	
	public long getTimeExpires() {
		return timeExpires;
	}
	
	public void setTimeExpires(long timeExpires) {
		this.timeExpires = timeExpires;
	}
	
	public XDHTPeerInfo getSender() {
		return sender;
	}
	
	public void setSender(XDHTPeerInfo sender) {
		this.sender = sender;
	}
}
