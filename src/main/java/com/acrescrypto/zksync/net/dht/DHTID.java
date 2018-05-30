package com.acrescrypto.zksync.net.dht;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class DHTID implements Comparable<DHTID>, Sendable {
	protected byte[] rawId;
	
	public DHTID(byte[] id) {
		this.rawId = id;
	}

	public DHTID calculateMidpoint(DHTID base) {
		// TODO DHT: (implement) (id - base)/2
		return null;
	}
	
	public DHTID xor(DHTID other) {
		assert(other.rawId.length == this.rawId.length);
		byte[] xored = new byte[rawId.length];
		for(int i = 0; i < rawId.length; i++) {
			xored[i] = (byte) (rawId[i] ^ other.rawId[i]);
		}
		
		return new DHTID(xored);
	}
	
	@Override
	public int hashCode() {
		return ByteBuffer.wrap(rawId).getInt();
	}

	@Override
	public int compareTo(DHTID other) {
		assert(other.rawId.length == this.rawId.length);
		for(int i = 0; i < rawId.length; i++) {
			if(rawId[i] < other.rawId[i]) return -1;
			if(rawId[i] > other.rawId[i]) return 1;
		}
		
		return 0;
	}
	
	@Override
	public byte[] serialize() {
		return rawId.clone();
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof DHTPeer) {
			return this.equals(((DHTPeer) o).id);
		}
		
		if(o instanceof DHTID) {
			return Arrays.equals(rawId, ((DHTID) o).rawId);
		}
		
		return false;
	}
}
