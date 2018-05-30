package com.acrescrypto.zksync.net.dht;

public abstract class DHTRecord implements Sendable {
	public static DHTRecord recordForSerialization(byte[] serialized) {
		// TODO DHT: (implement) deserialize concrete
		return null; 
	}
	
	public abstract byte[] serialize();
	public abstract void deserialize(byte[] serialized);
	public abstract boolean validate();
	public abstract boolean comesFrom(DHTPeer peer);
}
