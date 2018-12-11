package com.acrescrypto.zksync.net;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerAdvertisementListener {
	protected PeerSwarm swarm;
	protected CryptoSupport crypto;
	protected Logger logger = LoggerFactory.getLogger(TCPPeerAdvertisementListener.class);
	protected TCPPeerSocketListener listener;
	protected int version;
	
	public TCPPeerAdvertisementListener(PeerSwarm swarm, TCPPeerSocketListener listener) {
		this.swarm = swarm;
		this.crypto = swarm.config.getAccessor().getMaster().getCrypto();
		this.listener = listener;
		this.version = 0;
		if(listener.isListening()) {
			announce();
		}
	}
	
	public boolean matchesIdHash(byte[] hsHash, byte[] idHash) {
		byte[] thisIdHash = crypto.hash(Util.concat(hsHash, swarm.getConfig().getArchiveId()));
		return Arrays.equals(thisIdHash, idHash);
	}
	
	public TCPPeerAdvertisement localAd() throws UnconnectableAdvertisementException {
		byte[] encryptedArchiveId = swarm.config.getEncryptedArchiveId(swarm.getPublicIdentityKey().getBytes());
		
		// we could still be binding the socket, so wait for the port number to be non-zero before making the ad
		if(!Util.waitUntil(500, ()->listener.getPort() > 0)) throw new UnconnectableAdvertisementException();
		
		return new TCPPeerAdvertisement(swarm.getPublicIdentityKey(), "localhost", listener.getPort(), encryptedArchiveId, version).resolve(); // real hostname filled in by peers; use localhost as safe stand-in
	}
	
	public void announce() {
		new Thread(() -> {
			try {
				TCPPeerAdvertisement ad = localAd();
				swarm.advertiseSelf(ad);
			} catch(Exception exc) {
				logger.error("Announce thread caught exception", exc);
			}
		}).start();
	}
}