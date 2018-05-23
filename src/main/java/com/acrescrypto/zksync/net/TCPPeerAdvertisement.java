package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerAdvertisement extends PeerAdvertisement {
	protected byte[] pubKey;
	protected String host;
	protected int port;
	protected String ipAddress;
	
	// TODO P2P: (refactor) Not happy with the exceptions thrown from the constructor. Rethink this.
	public TCPPeerAdvertisement(byte[] pubKey, String host, int port) throws UnconnectableAdvertisementException {
		this.pubKey = pubKey;
		this.host = host;
		this.port = port;
		try {
			this.ipAddress = InetAddress.getByName(host).getHostAddress();
		} catch (UnknownHostException e) {
			throw new UnconnectableAdvertisementException();
		}
	}
	
	public TCPPeerAdvertisement(byte[] serialized, PeerConnection connection) throws UnconnectableAdvertisementException {
		this(serialized);
		this.ipAddress = this.host = connection.getSocket().getAddress();
	}
	
	public TCPPeerAdvertisement(byte[] serialized) throws UnconnectableAdvertisementException {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		
		int pubKeyLen = Util.unsignShort(buf.getShort());
		pubKey = new byte[pubKeyLen];
		buf.get(pubKey);
		
		int hostLen = Util.unsignShort(buf.getShort());
		byte[] hostBytes = new byte[hostLen];
		buf.get(hostBytes);
		host = new String(hostBytes);
		
		port = Util.unsignShort(buf.getShort());
		
		try {
			this.ipAddress = InetAddress.getByName(host).toString();
		} catch (UnknownHostException e) {
			throw new UnconnectableAdvertisementException();
		}
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
		return blacklist.contains(ipAddress.toString());
	}

	@Override
	public boolean matchesAddress(String address) {
		if(host.equals(address)) return true;
		if(ipAddress.equals(address)) return true;
		return false;
	}
	
	@Override
	public int hashCode() {
		String portStr = ""+port;
		ByteBuffer buf = ByteBuffer.allocate(pubKey.length + host.length() + portStr.length());
		buf.put(pubKey);
		buf.put(host.getBytes());
		buf.put(portStr.getBytes());
		return new String(buf.array()).hashCode();
	}

	@Override
	public int getType() {
		return PeerAdvertisement.TYPE_TCP_PEER;
	}
}
