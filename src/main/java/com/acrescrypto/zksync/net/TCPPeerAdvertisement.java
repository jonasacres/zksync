package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerAdvertisement extends PeerAdvertisement {
	protected PublicDHKey pubKey;
	protected String host;
	protected byte[] encryptedArchiveId;
	protected int port;
	protected int version;
	protected String ipAddress;
	
	public TCPPeerAdvertisement(PublicDHKey pubKey, String host, int port, byte[] encryptedArchiveId) {
		this(pubKey, host, port, encryptedArchiveId, 0);
	}
	
	public TCPPeerAdvertisement(PublicDHKey pubKey, String host, int port, byte[] encryptedArchiveId, int version) {
		this.pubKey = pubKey;
		this.host = host;
		this.port = port;
		this.encryptedArchiveId = encryptedArchiveId;
		this.version = version;
	}
	
	public TCPPeerAdvertisement(CryptoSupport crypto, ByteBuffer serialized, String address, int port) throws UnconnectableAdvertisementException {
		this(crypto, serialized);
		this.ipAddress = this.host = address;
		this.port = port;
	}
	
	public TCPPeerAdvertisement(CryptoSupport crypto, ByteBuffer serialized) throws UnconnectableAdvertisementException {
		if(TYPE_TCP_PEER != serialized.get() || 0 != serialized.getInt()) throw new UnconnectableAdvertisementException();
		
		int encryptedArchiveIdLen = Util.unsignShort(serialized.getShort());
		encryptedArchiveId = new byte[encryptedArchiveIdLen];
		serialized.get(encryptedArchiveId);
		
		int pubKeyLen = Util.unsignShort(serialized.getShort());
		byte[] pubKeyBytes = new byte[pubKeyLen];
		serialized.get(pubKeyBytes);
		pubKey = crypto.makePublicDHKey(pubKeyBytes);
		
		int hostLen = Util.unsignShort(serialized.getShort());
		byte[] hostBytes = new byte[hostLen];
		serialized.get(hostBytes);
		host = new String(hostBytes);
		
		port = Util.unsignShort(serialized.getShort());
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
		ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 2 + encryptedArchiveId.length + 2 + pubKey.getBytes().length + 2 + host.length() + 2);
		
		buf.put(getType());
		buf.putInt(version);
		
		buf.putShort((short) encryptedArchiveId.length);
		buf.put(encryptedArchiveId);
		
		buf.putShort((short) pubKey.getBytes().length);
		buf.put(pubKey.getBytes());
		
		buf.putShort((short) host.length());
		buf.put(host.getBytes());
		
		buf.putShort((short) port);
		
		return buf.array();
	}
	
	@Override
	public void blacklist(Blacklist blacklist) throws IOException {
		blacklist.add(ipAddress.toString(), Blacklist.DEFAULT_BLACKLIST_DURATION_MS);
	}

	@Override
	public boolean isBlacklisted(Blacklist blacklist) throws IOException {
		return blacklist.contains(ipAddress.toString());
	}
	
	@Override
	public boolean isReachable() {
		try {
			Socket socket = new Socket(host, port);
			socket.close();
		} catch (IOException e) {
			return false;
		}
		
		return true;
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
		ByteBuffer buf = ByteBuffer.allocate(pubKey.getBytes().length + host.length() + portStr.length() + encryptedArchiveId.length + 4);
		buf.put(pubKey.getBytes());
		buf.put(host.getBytes());
		buf.put(portStr.getBytes());
		buf.putInt(version);
		buf.put(encryptedArchiveId);
		return new String(Util.bytesToHex(buf.array())).hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if(!(o instanceof TCPPeerAdvertisement)) return false;
		TCPPeerAdvertisement other = (TCPPeerAdvertisement) o;
		
		if(!host.equals(other.host)) return false;
		if(port != other.port) return false;
		if(version != other.version) return false;
		if(!Arrays.equals(pubKey.getBytes(), other.pubKey.getBytes())) return false;
		if(!Arrays.equals(encryptedArchiveId, other.encryptedArchiveId)) return false;
		
		return true;
	}

	@Override
	public byte getType() {
		return PeerAdvertisement.TYPE_TCP_PEER;
	}
	
	public String getHost() {
		return host;
	}
	
	public int getPort() {
		return port;
	}
	
	public int getVersion() {
		return version;
	}
	
	public PublicDHKey getPubKey() {
		return pubKey;
	}
	
	public byte[] getEncryptedArchiveId() {
		return encryptedArchiveId;
	}
	
	public String toString() {
		return "TCPPeerAdvertisement: " + host + ":" + port;
	}
}
