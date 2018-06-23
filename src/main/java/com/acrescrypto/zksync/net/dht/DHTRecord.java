package com.acrescrypto.zksync.net.dht;

import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.utility.Util;

public abstract class DHTRecord implements Sendable {
	public final static byte RECORD_TYPE_ADVERTISEMENT = 0;
	
	public static DHTRecord deserializeRecordWithPeer(DHTPeer peer, ByteBuffer serialized) throws UnsupportedProtocolException {
		int type = Util.unsignByte(serialized.get());
		serialized.position(serialized.position()-1);
		
		switch(type) {
		case RECORD_TYPE_ADVERTISEMENT:
			return new DHTAdvertisementRecord(peer.client.crypto, serialized, peer.address, peer.port);
		default:
			throw new UnsupportedProtocolException();
		}
	}
	
	public static DHTRecord deserializeRecord(CryptoSupport crypto, ByteBuffer serialized) throws UnsupportedProtocolException {
		int type = Util.unsignByte(serialized.get());
		serialized.position(serialized.position()-1);
		
		switch(type) {
		case RECORD_TYPE_ADVERTISEMENT:
			return new DHTAdvertisementRecord(crypto, serialized);
		default:
			throw new UnsupportedProtocolException();
		}
	}
	
	public abstract byte[] serialize();
	public abstract void deserialize(ByteBuffer serialized) throws UnsupportedProtocolException;
	public abstract boolean isValid();
	public abstract boolean isReachable();
}
