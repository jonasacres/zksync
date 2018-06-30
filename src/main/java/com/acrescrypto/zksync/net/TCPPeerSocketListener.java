package com.acrescrypto.zksync.net;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerSocketListener {
	public final static int MAX_RECENT_PROOFS = 128;
	protected int port, requestedPort;
	
	protected CryptoSupport crypto;
	protected Blacklist blacklist;
	protected ServerSocket listenSocket;
	protected ZKMaster master;
	protected Thread thread;
	protected Logger logger = LoggerFactory.getLogger(TCPPeerSocketListener.class);
	protected LinkedList<TCPPeerAdvertisementListener> adListeners;
	protected LinkedList<Long> recentProofs;
	protected boolean closed, established;
	
	public TCPPeerSocketListener(ZKMaster master, int port) throws IOException {
		this.crypto = master.getCrypto();
		this.blacklist = master.getBlacklist();
		this.requestedPort = port;
		this.master = master;
		this.port = port == 0 ? cachedPort() : port;
		this.adListeners = new LinkedList<>();
		this.recentProofs = new LinkedList<>();
		this.thread = new Thread( ()->listenThread() );
		this.thread.start();
	}
	
	public int getPort() {
		return port;
	}
	
	public void close() throws IOException {
		if(closed) return;
		closed = true;
		if(listenSocket != null) {
			listenSocket.close();
		}
	}
	
	public void advertise(PeerSwarm swarm) {
		adListeners.add(new TCPPeerAdvertisementListener(swarm, this));
	}
	
	public TCPPeerAdvertisementListener listenerForSwarm(PeerSwarm swarm) {
		for(TCPPeerAdvertisementListener listener : adListeners) {
			if(listener.swarm == swarm) return listener;
		}
		
		return null;
	}
		
	protected MutableSecureFile cachedPortFile() {
		// stealing blacklist's FS and key is a bit sneaky and un-kosher, but what the hell...
		return MutableSecureFile.atPath(blacklist.fs, "tcp-port", blacklist.key.derive(0, new byte[0]));
	}
	
	protected int cachedPort() {
		try {
			return ByteBuffer.wrap(cachedPortFile().read()).getInt();
		} catch(Exception exc) { 
			return 0;
		}
	}
	
	protected void cachePort() {
		try {
			cachedPortFile().write(ByteBuffer.allocate(4).putInt(port).array(), 0);
		} catch (IOException exc) {
			logger.warn("Caught exception attempting to write TCP port cache file", exc);
		}
	}
	
	protected void listenThread() {
		Thread.currentThread().setName("TCPPeerSocketListener listen thread");
		while(!closed) {
			try {
				checkSocketOpen();
				if(listenSocket != null) {
					Socket socket = listenSocket.accept();
					processIncomingPeer(socket);
				}
			} catch(Exception exc) {
				if(closed) {
					logger.info("Closed TCP socket on port {}", listenSocket.getLocalPort());
					break;
				}
				
				logger.error("TCP listen thread on port " + port + " caught exception", exc);
			}
		}
	}
	
	protected void checkSocketOpen() {
		if(listenSocket == null || listenSocket.isClosed()) {
			openSocket();
			if(listenSocket != null && listenSocket.getLocalPort() != port) {
				updatePortCache();
			}
		}
	}
	
	protected void processIncomingPeer(Socket socket) throws IOException {
		if(blacklist.contains(socket.getInetAddress().getHostAddress())) {
			logger.info("Rejected connection from blacklisted peer {}", socket.getInetAddress().getHostAddress());
			socket.close();
			return;
		}
		
		logger.info("Accepted TCP connection from peer {}", socket.getInetAddress().toString());
		new Thread( ()->peerThread(socket) ).start();
	}
	
	protected void openSocket() {
		try {
			listenSocket = new ServerSocket(port);
			listenSocket.setReuseAddress(true);
		} catch(IOException exc) {
			if(requestedPort != 0 || port == 0) {
				logger.warn("Caught exception requesting port {}; waiting to retry...", port, exc);
				try { Thread.sleep(1000); } catch(InterruptedException exc2) {}
				return;
			}
			
			logger.warn("Unable to re-acquire TCP port {}, requesting new port number...", port, exc);
			port = 0;
		}
	}
	
	protected void updatePortCache() {
		this.port = listenSocket.getLocalPort();
		cachePort();
		logger.info("Listening for peers on TCP port {}", port);
		for(TCPPeerAdvertisementListener listener : adListeners) {
			listener.announce();
		}
	}
	
	protected void peerThread(Socket peerSocketRaw) {
		long startTime = Util.currentTimeMillis();
		try {
			performServerHandshake(peerSocketRaw);
		} catch(EOFException | ProtocolViolationException exc) {
			logger.info("Peer {} sent illegal handshake", peerSocketRaw.getInetAddress().toString(), exc);
			long delay = startTime + TCPPeerSocket.socketCloseDelay - Util.currentTimeMillis();
			Util.delay(delay, ()->peerSocketRaw.close());
		} catch(IOException exc) {
			logger.info("Caught IOException on connection to peer {}", peerSocketRaw.getInetAddress().toString(), exc);
			long delay = startTime + TCPPeerSocket.socketCloseDelay - Util.currentTimeMillis();
			Util.delay(delay, ()->peerSocketRaw.close());
		} catch(Exception exc) {
			exc.printStackTrace();
			logger.error("Caught unexpected exception on connection to peer {}", peerSocketRaw.getInetAddress().toString(), exc);
			long delay = startTime + TCPPeerSocket.socketCloseDelay - Util.currentTimeMillis();
			Util.delay(delay, ()->peerSocketRaw.close());
		}
	}
	
	protected TCPPeerSocket performServerHandshake(Socket peerSocketRaw) throws IOException, ProtocolViolationException {
		Util.ensure(TCPPeerSocket.maxHandshakeTimeMillis, ()->established, ()->peerSocketRaw.close());
		
		if(adListeners.isEmpty()) {
			throw new ProtocolViolationException(); // not ready to accept peers
		}
		
		InputStream in = peerSocketRaw.getInputStream();
		OutputStream out = peerSocketRaw.getOutputStream();

		// This will need to be rethought if we ever have different crypto configurations between archives
		byte[] pubKeyRaw = new byte[adListeners.getFirst().crypto.asymPublicSigningKeySize()];
		byte[] keyHash = new byte[adListeners.getFirst().crypto.hashLength()];
		byte[] proof = new byte[adListeners.getFirst().crypto.symKeyLength()];
		byte[] staticKeyCiphertext = new byte[adListeners.getFirst().crypto.asymPublicDHKeySize() + adListeners.getFirst().crypto.symTagLength()];
		byte[] timeIndexBytes = new byte[4];
		
		IOUtils.readFully(in, pubKeyRaw);
		IOUtils.readFully(in, keyHash);
		IOUtils.readFully(in, timeIndexBytes); // TODO DHT: (redesign) Don't like this field being plaintext and mutable.
		IOUtils.readFully(in, proof);
		IOUtils.readFully(in, staticKeyCiphertext);
		
		PublicDHKey pubKey = crypto.makePublicDHKey(pubKeyRaw);
		TCPPeerAdvertisementListener ad = findMatchingAdvertisement(pubKey, keyHash);
		int timeIndex = ByteBuffer.wrap(timeIndexBytes).getInt();
		int diff = timeIndex - ad.swarm.config.getAccessor().timeSliceIndex();
		assertState(Math.abs(diff) <= 1);
		if(diff < 0) {
			// stated index is in past
			long expiration = ad.swarm.config.getAccessor().timeSlice(timeIndex) + ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS;
			long expiredFor = Util.currentTimeMillis() - expiration;
			assertState(0 <= expiredFor && expiredFor <= 10000);
		} else if(diff > 0) {
			// stated index is in future
			long startTime = ad.swarm.config.getAccessor().timeSlice(timeIndex);
			long startsIn = startTime - Util.currentTimeMillis();
			assertState(0 <= startsIn && startsIn <= 10000);
		}

		// TODO DHT: (test) Test proof tag caching
		byte[] tempSharedSecret = ad.swarm.identityKey.sharedSecret(crypto.makePublicDHKey(pubKeyRaw));
		checkProofAgainstReplays(tempSharedSecret, proof);
		
		Key clientStaticSymKeyText = new Key(crypto, tempSharedSecret).derive(0, new byte[0]);
		byte[] clientStaticKeyRaw = clientStaticSymKeyText.decryptUnpadded(new byte[crypto.symIvLength()], staticKeyCiphertext);
		PublicDHKey clientStaticKey = crypto.makePublicDHKey(clientStaticKeyRaw);
		byte[] staticSecret = ad.swarm.identityKey.sharedSecret(clientStaticKey);
		
		byte[] expectedProof = ad.swarm.config.getAccessor().temporalProof(timeIndex, 0, tempSharedSecret);
		byte[] responseProof;
		PrivateDHKey ephemeralKey = crypto.makePrivateDHKey();
		byte[] ephemeralSecret = ephemeralKey.sharedSecret(pubKey);
		
		ByteBuffer sharedSecretMaterial = ByteBuffer.allocate(2*crypto.asymDHSecretSize());
		sharedSecretMaterial.put(staticSecret);
		sharedSecretMaterial.put(ephemeralSecret);
		byte[] sharedSecret = crypto.hash(sharedSecretMaterial.array());		
		
		int peerType;
		if(Util.safeEquals(expectedProof, proof)) {
			peerType = PeerConnection.PEER_TYPE_FULL;
			responseProof = ad.swarm.config.getAccessor().temporalProof(timeIndex, 1, sharedSecret);
		} else {
			peerType = PeerConnection.PEER_TYPE_BLIND;
			responseProof = crypto.rng(crypto.symKeyLength());
		}
		
		out.write(ephemeralKey.publicKey().getBytes());
		out.write(ad.crypto.authenticate(sharedSecret, Util.serializeInt(timeIndex)));
		out.write(responseProof);
		
		established = true;
		return new TCPPeerSocket(ad.swarm, clientStaticKey, peerSocketRaw, sharedSecret, peerType);
	}
	
	protected TCPPeerAdvertisementListener findMatchingAdvertisement(PublicDHKey pubKey, byte[] keyHash) throws ProtocolViolationException {
		for(TCPPeerAdvertisementListener ad : adListeners) {
			if(ad.matchesKeyHash(pubKey, keyHash)) return ad;
		}
		
		throw new ProtocolViolationException();
	}
	
	protected void assertState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}
	
	protected void checkProofAgainstReplays(byte[] tempSharedSecret, byte[] proof) throws ProtocolViolationException {
		long proofTag = Util.shortTag(crypto.authenticate(tempSharedSecret, proof));
		assertState(!recentProofs.contains(proofTag));
		recentProofs.add(proofTag);
		pruneRecentProofs();
	}
	
	protected void pruneRecentProofs() {
		while(recentProofs.size() > MAX_RECENT_PROOFS) {
			recentProofs.removeFirst();
		}
	}
}
