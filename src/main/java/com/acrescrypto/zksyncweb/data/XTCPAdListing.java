package com.acrescrypto.zksyncweb.data;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.net.PeerSwarm;
import com.acrescrypto.zksync.net.TCPPeerAdvertisement;

public class XTCPAdListing {
	private byte[] pubKey;
	private byte[] encryptedArchiveId;
	private String host;
	private int port;
	private int version;
	
	public XTCPAdListing() {}
	
	public XTCPAdListing(TCPPeerAdvertisement ad) {
		this.pubKey = ad.getPubKey().getBytes();
		this.encryptedArchiveId = ad.getEncryptedArchiveId();
		this.host = ad.getHost();
		this.port = ad.getPort();
		this.version = ad.getVersion();
	}
	
	public XTCPAdListing(PeerSwarm swarm) throws UnconnectableAdvertisementException, UnknownHostException, SocketException {
		this(swarm
				.getConfig()
				.getMaster()
				.getTCPListener()
				.listenerForSwarm(swarm)
				.localAd());
		
		// taken from https://stackoverflow.com/questions/9481865/getting-the-ip-address-of-the-current-machine-using-java/38342964#38342964
		try(final DatagramSocket socket = new DatagramSocket()) {
		  socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
		  this.host = socket.getLocalAddress().getHostAddress();
		}
	}

	public byte[] getPubKey() {
		return pubKey;
	}

	public void setPubKey(byte[] pubKey) {
		this.pubKey = pubKey;
	}

	public byte[] getEncryptedArchiveId() {
		return encryptedArchiveId;
	}

	public void setEncryptedArchiveId(byte[] encryptedArchiveId) {
		this.encryptedArchiveId = encryptedArchiveId;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}
}
