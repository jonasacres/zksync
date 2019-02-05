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
	private String senderHost;
	protected int failCount; // used by PeerSwarm to track connection attempts
	
	public static PeerAdvertisement deserializeRecord(CryptoSupport crypto, ByteBuffer serialized) throws UnconnectableAdvertisementException {
		byte type = serialized.get();
		serialized.position(serialized.position()-1);
		switch(type) {
		case TYPE_TCP_PEER:
			return new TCPPeerAdvertisement(crypto, serialized).resolve();
		}
		
		return null;
	}
	
	public static PeerAdvertisement deserializeRecordWithAddress(CryptoSupport crypto, ByteBuffer serialized, String address) throws UnconnectableAdvertisementException {
		byte type = serialized.get();
		serialized.position(serialized.position()-1);
		switch(type) {
		case TYPE_TCP_PEER:
			return new TCPPeerAdvertisement(crypto, serialized, address);
		}
		
		return null;
	}

	public abstract void blacklist(Blacklist blacklist) throws IOException;
	public abstract boolean isBlacklisted(Blacklist blacklist) throws IOException;
	public abstract byte[] serialize();
	public abstract boolean matchesAddress(String address);
	public abstract byte getType();
	public abstract boolean isReachable();
	public abstract String routingInfo();
	
	public PeerConnection connect(PeerSwarm swarm) throws UnsupportedProtocolException, IOException, ProtocolViolationException, BlacklistedException {
		return new PeerConnection(swarm, this);
	}
	
	public PublicDHKey getPubKey() {
		return pubKey;
	}
	
	public boolean equals(Object _other) {
		// TODO API: (coverage) method / consider if necessary
		if(!(_other instanceof PeerAdvertisement)) return false;
		PeerAdvertisement other = (PeerAdvertisement) _other;
		
		return Arrays.equals(this.serialize(), other.serialize());
	}

	public String getSenderHost() {
		return senderHost;
	}

	public void setSenderHost(String senderHost) {
		this.senderHost = senderHost;
	}
}
