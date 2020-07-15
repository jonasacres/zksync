package com.acrescrypto.zksync.net.dht;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.utility.Util;

public class DHTID implements Comparable<DHTID>, Sendable {
	protected byte[] rawId;
	
	public DHTID(PublicDHKey key) {
		this(key.getCrypto().hash(key.getBytes()));
	}
	
	public DHTID(byte[] id) {
		this.rawId = id;
	}
	
	public DHTID flip() {
		byte[] ones = new byte[rawId.length];
		for(int i = 0; i < ones.length; i++) {
			ones[i] = (byte) 0xff;
		}
		
		return xor(new DHTID(ones));
	}

	public int order() { // position of MSB
		for(int i = 0; i < rawId.length; i++) {
			byte b = this.rawId[i];
			for(int j = 7; j >= 0; j--) {
				if(((b >> j) & 1) != 0) {
					return 8*(rawId.length-i-1) + j;
				}
			}
		}
		
		return -1;
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
			int a = Util.unsignByte(rawId[i]), b = Util.unsignByte(other.rawId[i]);
			if(a < b) return -1;
			if(a > b) return 1;
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
	
	@Override
	public String toString() {
		return this.toFullString() + " (" + order() + ")";
	}
	
	public String toFullString() {
		return Util.bytesToHex(this.rawId);
	}
	
	public String toShortString() {
		return toString().substring(0, 7);
	}
}
