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
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.noise.CipherState;
import com.acrescrypto.zksync.net.noise.HandshakeState;
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
	protected boolean closed, established;
	protected PrivateDHKey identityKey;
	
	public TCPPeerSocketListener(ZKMaster master) throws IOException {
		this.crypto = master.getCrypto();
		this.blacklist = master.getBlacklist();
		this.master = master;
		this.adListeners = new LinkedList<>();
		this.identityKey = crypto.makePrivateDHKey(); // TODO Noise: cache static key to disk
	}
	
	public TCPPeerSocketListener(ZKMaster master, int port) throws IOException {
		this(master);
		startListening(port);
	}
	
	public void startListening(int port) {
		this.requestedPort = port;
		this.port = port == 0 ? cachedPort() : port;
		closed = false;
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
	
	public synchronized void stopAdvertising(PeerSwarm swarm) {
		// TODO API: (test) stopAdvertising
		adListeners.removeIf((listener)->listener.swarm == swarm);
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
			ServerSocket oldSocket = listenSocket;
			
			try {
				checkSocketOpen();
				if(listenSocket != null) {
					Socket socket = listenSocket.accept();
					processIncomingPeer(socket);
				}
			} catch(Exception exc) {
				if(closed || oldSocket != listenSocket) {
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
			performResponderHandshake(peerSocketRaw);
		} catch(EOFException | ProtocolViolationException exc) {
			logger.info("Peer {} sent illegal handshake", peerSocketRaw.getInetAddress().toString(), exc);
			long delay = startTime + TCPPeerSocket.socketCloseDelay - Util.currentTimeMillis();
			Util.delay(delay, ()->peerSocketRaw.close());
		} catch(IOException exc) {
			logger.info("Caught IOException on connection to peer {}", peerSocketRaw.getInetAddress().toString(), exc);
			long delay = startTime + TCPPeerSocket.socketCloseDelay - Util.currentTimeMillis();
			Util.delay(delay, ()->peerSocketRaw.close());
		} catch(SecurityException exc) {
			logger.info("Unable to handshake with peer {}", peerSocketRaw.getInetAddress().toString(), exc);
			long delay = startTime + TCPPeerSocket.socketCloseDelay - Util.currentTimeMillis();
			Util.delay(delay, ()->peerSocketRaw.close());
		} catch(Exception exc) {
			logger.error("Caught unexpected exception on connection to peer {}", peerSocketRaw.getInetAddress().toString(), exc);
			long delay = startTime + TCPPeerSocket.socketCloseDelay - Util.currentTimeMillis();
			Util.delay(delay, ()->peerSocketRaw.close());
		}
	}
	
	protected TCPPeerSocket performResponderHandshake(Socket peerSocketRaw) throws IOException, ProtocolViolationException {
		Util.ensure(TCPPeerSocket.maxHandshakeTimeMillis, 10, ()->established, ()->{
			peerSocketRaw.close();
		});
		
		if(adListeners.isEmpty()) {
			throw new ProtocolViolationException(); // not ready to accept peers
		}
		
		// TODO Noise: rate limit these
		InputStream in = peerSocketRaw.getInputStream();
		OutputStream out = peerSocketRaw.getOutputStream();
		class MutableAdListener {
			TCPPeerAdvertisementListener value;
		}
		
		MutableAdListener ad = new MutableAdListener();
		
		MutableInt peerType = new MutableInt(), portNum = new MutableInt();
		
		HandshakeState handshake = new HandshakeState(
				crypto,
				TCPPeerSocket.HANDSHAKE_PROTOCOL,
				TCPPeerSocket.HANDSHAKE_PATTERN,
				false,
				new byte[0],
				getIdentityKey(),
				null,
				null,
				null,
				null
				);
		
		handshake.setObfuscation(
				(key)->{
					Key sym = new Key(crypto, crypto.makeSymmetricKey(handshake.getRemoteEphemeralKey().getBytes()));
					return sym.encryptCBC(new byte[crypto.symBlockSize()], key.getBytes());
				},
				
				(inn)->{
					Key sym = new Key(crypto, crypto.makeSymmetricKey(identityKey.publicKey().getBytes()));
					byte[] ciphertext = IOUtils.readFully(inn, crypto.asymPublicDHKeySize());
					byte[] keyRaw = sym.decryptCBC(new byte[crypto.symBlockSize()], ciphertext);
					return new byte[][] { keyRaw, ciphertext };
				}
			);
		
		handshake.setPayload(
			(round)->{
				if(round != 4) return null;
				byte[] proof;
				if(peerType.intValue() == PeerConnection.PEER_TYPE_FULL) {
					proof = ad.value.swarm.getConfig().getAccessor().temporalProof(0, 1, handshake.getHash());
				} else {
					proof = crypto.rng(crypto.symKeyLength());
				}
				
				return proof;
			},
			
			(round, inn, decrypter)->{
				if(round != 3) return;
				int ctLen = crypto.hashLength() + crypto.symKeyLength() + 2 + crypto.symTagLength();
				byte[] idHash = new byte[crypto.hashLength()];
				byte[] proof = new byte[crypto.symKeyLength()];
				byte[] hsHash = handshake.getHash(); // need hash from before we call decrypt
				
				ByteBuffer payload = ByteBuffer.wrap(decrypter.decrypt(IOUtils.readFully(inn, ctLen)));
				payload.get(idHash);
				payload.get(proof);
				
				try {
					ad.value = findMatchingAdvertisement(hsHash, idHash);
				} catch (ProtocolViolationException e) {
					throw new SecurityException("no archive matching request");
				}
				
				handshake.setPsk(ad.value.swarm.config.getArchiveId());
				byte[] expectedProof = ad.value.swarm.config.getAccessor().temporalProof(0, 0, hsHash);
				if(Util.safeEquals(expectedProof, proof)) {
					peerType.setValue(PeerConnection.PEER_TYPE_FULL);
				} else {
					peerType.setValue(PeerConnection.PEER_TYPE_BLIND);
				}
			}
		);
		
		CipherState[] states = handshake.handshake(in, out);
		
		return new TCPPeerSocket(ad.value.swarm,
				handshake.getRemoteStaticKey(),
				peerSocketRaw,
				states,
				handshake.getHash(),
				peerType.intValue(),
				portNum.intValue());
	}
	
	protected PrivateDHKey getIdentityKey() {
		return identityKey;
	}

	protected synchronized TCPPeerAdvertisementListener findMatchingAdvertisement(byte[] hsHash, byte[] idHash) throws ProtocolViolationException {
		for(TCPPeerAdvertisementListener ad : adListeners) {
			if(ad.matchesIdHash(hsHash, idHash)) return ad;
		}
		
		throw new ProtocolViolationException();
	}
	
	protected void assertState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}
	

	public boolean isListening() {
		return listenSocket != null && !closed;
	}
}
