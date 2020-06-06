package com.acrescrypto.zksyncweb.data;

import com.acrescrypto.zksync.net.dht.DHTPeer;

public class XDHTPeerFileEntry {
	private byte[]  id;
	private byte[]  pubKey;
	private Integer port;
	private String  address;

	public XDHTPeerFileEntry(DHTPeer peer) {
		this.id             = peer.getId().serialize();
		this.pubKey         = peer.getKey().getBytes();
		this.port           = peer.getPort();
		this.address        = peer.getAddress();
	}
	
	public XDHTPeerFileEntry() {}

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
}
