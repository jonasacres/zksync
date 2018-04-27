package com.acrescrypto.zksync.net.dht;

public class DHTPeerReference {
	protected String address;
	protected int port;
	protected int addressType;
	
	public DHTPeerReference(String address, int port, int addressType) {
		this.address = address;
		this.port = port;
		this.addressType = addressType;
	}
	
	public DHTPeerReference(byte[] serialized) {
		deserialize(serialized);
	}
	
	protected void deserialize(byte[] serialized) {
		// TODO P2P
	}
	
	protected byte[] serialize() {
		// TODO P2P
		return null;
	}
}
