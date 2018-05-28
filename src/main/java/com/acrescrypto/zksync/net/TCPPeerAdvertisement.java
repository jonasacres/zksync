package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerAdvertisement extends PeerAdvertisement {
	protected PublicDHKey pubKey;
	protected String host;
	protected int port;
	protected String ipAddress;
	
	public TCPPeerAdvertisement(PublicDHKey pubKey, String host, int port) {
		this.pubKey = pubKey;
		this.host = host;
		this.port = port;
	}
	
	public TCPPeerAdvertisement(byte[] serialized, PeerConnection connection) {
		this(serialized);
		this.ipAddress = this.host = connection.getSocket().getAddress();
	}
	
	public TCPPeerAdvertisement(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		assert(TYPE_TCP_PEER == buf.get());
		int pubKeyLen = Util.unsignShort(buf.getShort());
		byte[] pubKeyBytes = new byte[pubKeyLen];
		buf.get(pubKeyBytes);
		pubKey = new PublicDHKey(pubKeyBytes);
		
		int hostLen = Util.unsignShort(buf.getShort());
		byte[] hostBytes = new byte[hostLen];
		buf.get(hostBytes);
		host = new String(hostBytes);
		
		port = Util.unsignShort(buf.getShort());
	}

	public TCPPeerAdvertisement resolve() throws UnconnectableAdvertisementException {
		try {
			this.ipAddress = InetAddress.getByName(host).getHostAddress();
		} catch (UnknownHostException e) {
			throw new UnconnectableAdvertisementException();
		}
		
		return this;
	}

	@Override
	public byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(1 + 2 + pubKey.getBytes().length + 2 + host.length() + 2);
		buf.put(getType());
		buf.putShort((short) pubKey.getBytes().length);
		buf.put(pubKey.getBytes());
		buf.putShort((short) host.length());
		buf.put(host.getBytes());
		buf.putShort((short) port);
		return buf.array();
	}
	
	@Override
	public void blacklist(Blacklist blacklist) throws IOException {
		blacklist.add(ipAddress.toString(), Integer.MAX_VALUE);
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
		ByteBuffer buf = ByteBuffer.allocate(pubKey.getBytes().length + host.length() + portStr.length());
		buf.put(pubKey.getBytes());
		buf.put(host.getBytes());
		buf.put(portStr.getBytes());
		return new String(buf.array()).hashCode();
	}

	@Override
	public byte getType() {
		return PeerAdvertisement.TYPE_TCP_PEER;
	}
	
	public String toString() {
		return "TCPPeerAdvertisement: " + host + ":" + port;
	}
}
