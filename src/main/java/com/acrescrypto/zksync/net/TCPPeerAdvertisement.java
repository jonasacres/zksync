package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.utility.Util;

public class TCPPeerAdvertisement extends PeerAdvertisement {
	protected byte[] pubKey;
	protected String host;
	protected int port;
	
	public TCPPeerAdvertisement(byte[] pubKey, String host, int port) {
		this.pubKey = pubKey;
		this.host = host;
		this.port = port;
	}
	
	public TCPPeerAdvertisement(byte[] serialized, PeerConnection connection) {
		this(serialized);
		this.host = connection.getSocket().getAddress();
	}
	
	public TCPPeerAdvertisement(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		
		int pubKeyLen = Util.unsignShort(buf.getShort());
		pubKey = new byte[pubKeyLen];
		buf.get(pubKey);
		
		int hostLen = Util.unsignShort(buf.getShort());
		byte[] hostBytes = new byte[hostLen];
		buf.get(hostBytes);
		host = new String(hostBytes);
		
		port = Util.unsignShort(buf.getShort());
	}

	@Override
	public byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(2 + pubKey.length + 2 + host.length() + 2);
		buf.putShort((short) pubKey.length);
		buf.put(pubKey);
		buf.putShort((short) host.length());
		buf.put(host.getBytes());
		buf.putShort((short) port);
		return buf.array();
	}

	@Override
	public boolean isBlacklisted(Blacklist blacklist) throws IOException {
		InetAddress address = InetAddress.getByName(host);
		return blacklist.contains(address.toString());
	}

	@Override
	public boolean matchesAddress(String address) {
		if(host.equals(address)) return true;
		try {
			InetAddress resolved = InetAddress.getByName(host);
			return host.equals(resolved.toString());
		} catch (UnknownHostException e) {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		return ByteBuffer.wrap(pubKey).getInt();
	}

	@Override
	public int getType() {
		return PeerAdvertisement.TYPE_TCP_PEER;
	}
}
