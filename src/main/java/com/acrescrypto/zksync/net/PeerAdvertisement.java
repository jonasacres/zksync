package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;

public abstract class PeerAdvertisement {
	public final static int TYPE_TCP_PEER = 0;
	
	public static PeerAdvertisement deserialize(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		int type = buf.getInt();
		switch(type) {
		case TYPE_TCP_PEER:
			return new TCPPeerAdvertisement(serialized);
		}
		
		return null;
	}
	
	public static PeerAdvertisement deserializeWithPeer(byte[] serialized, PeerConnection connection) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		int type = buf.getInt();
		switch(type) {
		case TYPE_TCP_PEER:
			return new TCPPeerAdvertisement(serialized, connection);
		}
		
		return null;
	}

	public abstract boolean isBlacklisted(Blacklist blacklist) throws IOException;
	public abstract byte[] serialize();
	public abstract boolean matchesAddress(String address);
	public abstract int getType();
	
	public PeerConnection connect(PeerSwarm swarm) throws UnsupportedProtocolException, IOException, ProtocolViolationException, BlacklistedException {
		return new PeerConnection(swarm, this);
	}
}
