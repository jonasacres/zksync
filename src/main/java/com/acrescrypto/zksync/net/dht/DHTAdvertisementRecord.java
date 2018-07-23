package com.acrescrypto.zksync.net.dht;

import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.net.PeerAdvertisement;
import com.acrescrypto.zksync.utility.Util;

public class DHTAdvertisementRecord extends DHTRecord {
	protected PeerAdvertisement ad;
	protected CryptoSupport crypto;
	
	public DHTAdvertisementRecord(CryptoSupport crypto, ByteBuffer serialized) throws UnsupportedProtocolException {
		this.crypto = crypto;
		deserialize(serialized);
	}
	
	public DHTAdvertisementRecord(CryptoSupport crypto, ByteBuffer serialized, String address, int port) throws UnsupportedProtocolException {
		this.crypto = crypto;
		deserialize(serialized, address, port);
	}
	
	public DHTAdvertisementRecord(CryptoSupport crypto, PeerAdvertisement ad) {
		this.crypto = crypto;
		this.ad = ad;
	}

	@Override
	public byte[] serialize() {
		byte[] serializedAd = ad.serialize();
		ByteBuffer buf = ByteBuffer.allocate(1+2+serializedAd.length);
		buf.put(RECORD_TYPE_ADVERTISEMENT);
		buf.putShort((short) serializedAd.length);
		buf.put(serializedAd);
		return buf.array();
	}

	@Override
	public void deserialize(ByteBuffer serialized) throws UnsupportedProtocolException {
		deserialize(serialized, null, 0);
	}
	
	public void deserialize(ByteBuffer serialized, String address, int port) throws UnsupportedProtocolException {
		byte type = serialized.get();
		if(type != RECORD_TYPE_ADVERTISEMENT) throw new UnsupportedProtocolException();
		if(serialized.remaining() < 2) throw new UnsupportedProtocolException();
		int expectedLen = Util.unsignShort(serialized.getShort());
		if(expectedLen > serialized.remaining()) throw new UnsupportedProtocolException();
		int expectedPos = serialized.position() + expectedLen;
		
		try {
			if(address == null || port <= 0) {
				this.ad = PeerAdvertisement.deserializeRecord(crypto, serialized);
			} else {
				this.ad = PeerAdvertisement.deserializeRecordWithAddress(crypto, serialized, address, port);
			}
			
			if(ad == null) {
				throw new UnsupportedProtocolException();
			}
			
			if(serialized.position() != expectedPos) {
				serialized.position(expectedPos);
				throw new UnsupportedProtocolException();
			}
		} catch (UnconnectableAdvertisementException exc) {
			serialized.position(expectedPos);
			throw new UnsupportedProtocolException();
		}
	}

	@Override
	public boolean isValid() {
		return true;
	}

	@Override
	public boolean isReachable() {
		return ad.isReachable();
	}

	@Override
	public boolean equals(Object o) {
		if(!(o instanceof DHTAdvertisementRecord)) return false;
		return ad.equals(((DHTAdvertisementRecord) o).ad);
	}
	
	public String toString() {
		return ad.toString();
	}
}
