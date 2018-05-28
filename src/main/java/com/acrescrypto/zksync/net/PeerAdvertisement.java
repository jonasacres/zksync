package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;

public abstract class PeerAdvertisement {
	public final static int TYPE_TCP_PEER = 0;
	
	public static PeerAdvertisement deserialize(byte[] serialized) throws UnconnectableAdvertisementException {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		byte type = buf.get();
		switch(type) {
		case TYPE_TCP_PEER:
			return new TCPPeerAdvertisement(serialized).resolve();
		}
		
		return null;
	}
	
	public static PeerAdvertisement deserializeWithPeer(byte[] serialized, PeerConnection connection) throws UnconnectableAdvertisementException {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		byte type = buf.get();
		switch(type) {
		case TYPE_TCP_PEER:
			return new TCPPeerAdvertisement(serialized, connection);
		}
		
		return null;
	}

	public abstract void blacklist(Blacklist blacklist) throws IOException;
	public abstract boolean isBlacklisted(Blacklist blacklist) throws IOException;
	public abstract byte[] serialize();
	public abstract boolean matchesAddress(String address);
	public abstract byte getType();
	
	public PeerConnection connect(PeerSwarm swarm) throws UnsupportedProtocolException, IOException, ProtocolViolationException, BlacklistedException {
		return new PeerConnection(swarm, this);
	}
	
	public boolean equals(Object _other) {
		if(!(_other instanceof PeerAdvertisement)) return false;
		PeerAdvertisement other = (PeerAdvertisement) _other;
		
		return Arrays.equals(this.serialize(), other.serialize());
	}
}
