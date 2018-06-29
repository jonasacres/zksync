package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;

public abstract class PeerAdvertisement {
	public final static int TYPE_TCP_PEER = 0;
	
	protected PublicDHKey pubKey;
	
	public static PeerAdvertisement deserializeRecord(CryptoSupport crypto, ByteBuffer serialized) throws UnconnectableAdvertisementException {
		byte type = serialized.get();
		serialized.position(serialized.position()-1);
		switch(type) {
		case TYPE_TCP_PEER:
			return new TCPPeerAdvertisement(crypto, serialized).resolve();
		}
		
		return null;
	}
	
	public static PeerAdvertisement deserializeRecordWithAddress(CryptoSupport crypto, ByteBuffer serialized, String address, int port) throws UnconnectableAdvertisementException {
		byte type = serialized.get();
		serialized.position(serialized.position()-1);
		switch(type) {
		case TYPE_TCP_PEER:
			return new TCPPeerAdvertisement(crypto, serialized, address, port);
		}
		
		return null;
	}

	public abstract void blacklist(Blacklist blacklist) throws IOException;
	public abstract boolean isBlacklisted(Blacklist blacklist) throws IOException;
	public abstract byte[] serialize();
	public abstract boolean matchesAddress(String address);
	public abstract byte getType();
	public abstract boolean isReachable();
	
	public PeerConnection connect(PeerSwarm swarm) throws UnsupportedProtocolException, IOException, ProtocolViolationException, BlacklistedException {
		return new PeerConnection(swarm, this);
	}
	
	public PublicDHKey getPubKey() {
		return pubKey;
	}
	
	public boolean equals(Object _other) {
		if(!(_other instanceof PeerAdvertisement)) return false;
		PeerAdvertisement other = (PeerAdvertisement) _other;
		
		return Arrays.equals(this.serialize(), other.serialize());
	}
}
