package com.acrescrypto.zksync.net.dht;

import com.acrescrypto.zksync.fs.zkfs.ZKArchive;

public class DHTEntry {
	protected String address;
	protected int port;
	protected int addressType;
	protected byte[] id;
	
	public static DHTEntry bootstrapEntry() {
		// TODO
		return null;
	}
	
	public String getAddress() {
		return address;
	}
	
	public int getPort() {
		return port;
	}
	
	public int getAddressType() {
		return addressType;
	}
	
	public byte[] getId() {
		return id;
	}
	
	public ZKArchive getArchive() {
		return null;
	}
}
