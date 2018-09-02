package com.acrescrypto.zksync.net;

import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
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
	protected boolean closed;
	
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
		
		if(adListeners == null || adListeners.isEmpty()) {
			try {
				logger.info("Rejected premature connection from peer {}", socket.getInetAddress().getHostAddress());
				socket.close();
			} catch (IOException exc) {
				logger.error("Caught exception closing socket", exc);
			}
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
	
	protected synchronized void updatePortCache() {
		this.port = listenSocket.getLocalPort();
		cachePort();
		logger.info("Listening for peers on TCP port {}", port);
		for(TCPPeerAdvertisementListener listener : adListeners) {
			listener.announce();
		}
	}
	
	protected void peerThread(Socket peerSocketRaw) {
		try {
			performServerHandshake(peerSocketRaw);
		} catch(EOFException | ProtocolViolationException exc) {
			logger.info("Peer {} sent illegal handshake", peerSocketRaw.getInetAddress().toString(), exc);
		} catch(IOException exc) {
			logger.info("Caught IOException on connection to peer {}", peerSocketRaw.getInetAddress().toString(), exc);
		} catch(Exception exc) {
			logger.error("Caught unexpected exception on connection to peer {}", peerSocketRaw.getInetAddress().toString(), exc);
		}
	}
	
	protected TCPPeerSocket performServerHandshake(Socket peerSocketRaw) throws IOException, ProtocolViolationException {
		TCPHandshakeContext handshake = new TCPHandshakeContext(this, peerSocketRaw);
		handshake.establishSecret();
		return new TCPPeerSocket(handshake);
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
