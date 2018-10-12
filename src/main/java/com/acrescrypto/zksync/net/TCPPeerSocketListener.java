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
		this.thread = new Thread(master.getThreadGroup(), ()->listenThread() );
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
	
	public synchronized void advertise(PeerSwarm swarm) {
		adListeners.add(new TCPPeerAdvertisementListener(swarm, this));
		swarm.config.getAccessor().forceAdvertisement();
	}
	
	public synchronized TCPPeerAdvertisementListener listenerForSwarm(PeerSwarm swarm) {
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
		Util.setThreadName("TCPPeerSocketListener listen thread");
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
		new Thread(master.getThreadGroup(), ()->peerThread(socket) ).start();
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
	
	protected synchronized void updatePortCache() {
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
			logger.error("Caught unexpected exception on connection to peer {}", peerSocketRaw.getInetAddress().toString(), exc);
			long delay = startTime + TCPPeerSocket.socketCloseDelay - Util.currentTimeMillis();
			Util.delay(delay, ()->peerSocketRaw.close());
		}
	}
	
	protected TCPPeerSocket performServerHandshake(Socket peerSocketRaw) throws IOException, ProtocolViolationException {
		Util.ensure(TCPPeerSocket.maxHandshakeTimeMillis, 10, ()->established, ()->peerSocketRaw.close());
		
		if(adListeners.isEmpty()) {
			throw new ProtocolViolationException(); // not ready to accept peers
		}
		
		InputStream in = peerSocketRaw.getInputStream();
		OutputStream out = peerSocketRaw.getOutputStream();

		// This will need to be rethought if we ever have different crypto configurations between archives
		byte[] pubKeyRaw = new byte[crypto.asymPublicSigningKeySize()];
		byte[] keyHash = new byte[crypto.hashLength()];
		byte[] keyKnowledgeProof = new byte[crypto.symKeyLength()];
		byte[] staticKeyCiphertext = new byte[crypto.asymPublicDHKeySize() + crypto.symTagLength()];
		byte[] timeProof = new byte[crypto.hashLength()];
		byte[] encryptedPortNumber = new byte[2 + crypto.symTagLength()];
		
		IOUtils.readFully(in, pubKeyRaw);
		IOUtils.readFully(in, keyHash);
		IOUtils.readFully(in, timeProof);
		IOUtils.readFully(in, keyKnowledgeProof);
		IOUtils.readFully(in, staticKeyCiphertext);
		IOUtils.readFully(in, encryptedPortNumber);
		
		PublicDHKey pubKey = crypto.makePublicDHKey(pubKeyRaw);
		TCPPeerAdvertisementListener ad = findMatchingAdvertisement(pubKey, keyHash);

		byte[] tempSharedSecret = ad.swarm.identityKey.sharedSecret(crypto.makePublicDHKey(pubKeyRaw));
		checkProofAgainstReplays(tempSharedSecret, keyKnowledgeProof);
		
		Key clientStaticSymKeyText = new Key(crypto, tempSharedSecret).derive(0, new byte[0]);
		byte[] clientStaticKeyRaw = clientStaticSymKeyText.decryptUnpadded(crypto.symNonce(0), staticKeyCiphertext);
		int portNum = ByteBuffer.wrap(clientStaticSymKeyText.decryptUnpadded(crypto.symNonce(1), encryptedPortNumber)).getShort();
		PublicDHKey clientStaticKey = crypto.makePublicDHKey(clientStaticKeyRaw);
		byte[] staticSecret = ad.swarm.identityKey.sharedSecret(clientStaticKey);
		int timeIndex = Integer.MIN_VALUE;
		
		for(int diff : new int[] { 0, -1, 1 }) {
			int attemptedTimeIndex = ad.swarm.config.getAccessor().timeSliceIndex() + diff;
			byte[] expectedTimeProof = crypto.authenticate(tempSharedSecret, Util.serializeInt(attemptedTimeIndex));
			if(Util.safeEquals(expectedTimeProof, timeProof)) {
				timeIndex = attemptedTimeIndex;
			}
		}
		
		assertState(timeIndex > Integer.MIN_VALUE);
		int diff = timeIndex - ad.swarm.config.getAccessor().timeSliceIndex();
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

		
		byte[] expectedKeyKnowledgeProof = ad.swarm.config.getAccessor().temporalProof(timeIndex, 0, tempSharedSecret);
		byte[] responseKeyKnowledgeProof;
		PrivateDHKey ephemeralKey = crypto.makePrivateDHKey();
		byte[] ephemeralSecret = ephemeralKey.sharedSecret(pubKey);
		
		int peerType;
		if(Util.safeEquals(expectedKeyKnowledgeProof, keyKnowledgeProof)) {
			peerType = PeerConnection.PEER_TYPE_FULL;
			responseKeyKnowledgeProof = ad.swarm.config.getAccessor().temporalProof(timeIndex, 1, tempSharedSecret);
		} else {
			peerType = PeerConnection.PEER_TYPE_BLIND;
			responseKeyKnowledgeProof = crypto.rng(crypto.symKeyLength());
		}
		
		byte[] sharedSecretText = Util.concat(pubKeyRaw,
				keyHash,
				timeProof,
				keyKnowledgeProof,
				staticKeyCiphertext,
				ephemeralKey.publicKey().getBytes(),
				responseKeyKnowledgeProof,
				ad.swarm.config.getAccessor().temporalSeedId(timeIndex));
		byte[] sharedSecretRoot = crypto.authenticate(Util.concat(staticSecret, ephemeralSecret),
				sharedSecretText);
		byte[] sharedSecret = crypto.authenticate(sharedSecretRoot, Util.serializeInt(0));
		byte[] handshakeProofKey = crypto.authenticate(sharedSecretRoot, Util.serializeInt(1));
		
		out.write(ephemeralKey.publicKey().getBytes());
		out.write(responseKeyKnowledgeProof);
		out.write(crypto.authenticate(handshakeProofKey, ad.swarm.config.getAccessor().temporalSeedId(timeIndex)));
		
		established = true;
		return new TCPPeerSocket(ad.swarm, clientStaticKey, peerSocketRaw, sharedSecret, peerType, portNum);
	}
	
	protected synchronized TCPPeerAdvertisementListener findMatchingAdvertisement(PublicDHKey pubKey, byte[] keyHash) throws ProtocolViolationException {
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
