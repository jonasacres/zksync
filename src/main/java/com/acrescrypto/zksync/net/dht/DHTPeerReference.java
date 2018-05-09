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
		// TODO DHT: (implement)
	}
	
	protected byte[] serialize() {
		// TODO DHT: (implement)
		return null;
	}
}
