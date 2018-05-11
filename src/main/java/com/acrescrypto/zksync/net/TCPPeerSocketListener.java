package com.acrescrypto.zksync.net;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.curve25519.Curve25519;
import org.whispersystems.curve25519.Curve25519KeyPair;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerSocketListener {
	protected int port, requestedPort;
	protected Blacklist blacklist;
	protected ServerSocket listenSocket;
	protected Thread thread;
	protected Logger logger = LoggerFactory.getLogger(TCPPeerSocketListener.class);
	protected Curve25519 curve25519;
	protected LinkedList<TCPPeerAdvertisementListener> adListeners;
	protected boolean closed;
	
	protected class TCPPeerAdvertisementListener {
		protected Curve25519 curve25519;
		protected PeerSwarm swarm;
		protected CryptoSupport crypto;
		protected byte[] publicKey, privateKey;
		
		public TCPPeerAdvertisementListener(Curve25519 curve25519, PeerSwarm swarm, int port) {
			this.curve25519 = curve25519;
			this.swarm = swarm;
			this.crypto = swarm.config.getAccessor().getMaster().getCrypto();
			initKeys();
			announce(port);
		}
		
		public boolean matchesKeyHash(byte[] remotePubKey, byte[] keyHash) {
			ByteBuffer keyHashInput = ByteBuffer.allocate(2*crypto.symKeyLength());
			keyHashInput.put(remotePubKey);
			keyHashInput.put(publicKey);
			Key keyHashKey = swarm.config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_SEED);
			
			byte[] expectedKeyHash = keyHashKey.authenticate(keyHashInput.array());
			return Arrays.equals(expectedKeyHash, keyHash);
		}
		
		public byte[] sharedSecret(byte[] remotePubKey) {
			return curve25519.calculateAgreement(remotePubKey, privateKey);
		}
		
		public void announce(int port) {
			new Thread(() -> {
				try {
					TCPPeerAdvertisement ad = new TCPPeerAdvertisement(publicKey, "", port);
					swarm.advertiseSelf(ad);
				} catch(Exception exc) {
					logger.error("Announce thread caught exception", exc);
				}
			});
		}
		
		protected MutableSecureFile storedFile() throws IOException {
			FS fs = swarm.config.getArchive().getMaster().localStorageFsForArchiveId(swarm.config.getArchiveId());
			return MutableSecureFile.atPath(fs, "tcp-identity", swarm.config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_AD_IDENTITY));
		}
		
		protected void initKeys() {
			try {
				deserialize(storedFile().read());
			} catch(ENOENTException exc) {
				try {
					Curve25519KeyPair keyPair = curve25519.generateKeyPair();
					this.privateKey = keyPair.getPrivateKey();
					this.publicKey = keyPair.getPublicKey();
					storedFile().write(serialize(), 0);
				} catch (IOException e) {
					logger.error("Caught exception writing advertisement for archive {}", Util.bytesToHex(swarm.config.getArchiveId()), exc);
				}
			} catch (IOException exc) {
				logger.error("Caught exception opening stored advertisement for archive {}", Util.bytesToHex(swarm.config.getArchiveId()), exc);
			}
		}
		
		protected byte[] serialize() {
			ByteBuffer buf = ByteBuffer.allocate(privateKey.length + publicKey.length);
			buf.put(privateKey);
			buf.put(publicKey);
			assert(!buf.hasRemaining());
			return buf.array();
		}
		
		protected void deserialize(byte[] serialized) {
			ByteBuffer buf = ByteBuffer.wrap(serialized);
			this.privateKey = new byte[buf.getShort()];
			buf.get(privateKey);
			this.publicKey = new byte[buf.getShort()];
			buf.get(publicKey);
			assert(!buf.hasRemaining());
		}
	}

	public TCPPeerSocketListener(Blacklist blacklist, int port) throws IOException {
		this.blacklist = blacklist;
		this.requestedPort = port == 0 ? cachedPort() : port;
		this.curve25519 = Curve25519.getInstance(Curve25519.BEST);
		this.adListeners = new LinkedList<TCPPeerAdvertisementListener>();
		this.thread = new Thread( ()->listenThread() );
		this.thread.start();
	}
	
	public int getPort() {
		return port;
	}
	
	public void close() throws IOException {
		closed = true;
		listenSocket.close();
	}
	
	public void advertise(PeerSwarm swarm) {
		adListeners.add(new TCPPeerAdvertisementListener(curve25519, swarm, port));
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
		while(true) {
			try {
				if(listenSocket == null || listenSocket.isClosed()) {
					try {
						listenSocket = new ServerSocket(port);
					} catch(IOException exc) {
						if(requestedPort != 0 || port == 0) {
							logger.warn("Caught exception requesting port {}; waiting to retry...", port, exc);
							Thread.sleep(1000);
						} else {
							logger.warn("Unable to re-acquire TCP port {}, requesting new port number...", port, exc);
							port = 0;
						}
					}
					
					if(listenSocket.getLocalPort() != port) {
						this.port = listenSocket.getLocalPort();
						cachePort();
						logger.info("Listening for peers on TCP port {}", port);
						for(TCPPeerAdvertisementListener listener : adListeners) {
							listener.announce(port);
						}
					}
				}
				
				Socket peerSocket = listenSocket.accept();
				if(blacklist.contains(peerSocket.getInetAddress().toString())) {
					logger.info("Rejected connection from blacklisted peer {}", peerSocket.getInetAddress().toString());
					peerSocket.close();
				} else {
					logger.info("Accepted TCP connection from peer {}", peerSocket.getInetAddress().toString());
					new Thread( ()->peerThread(peerSocket) ).start();;
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
		if(adListeners.isEmpty()) throw new ProtocolViolationException(); // not ready to accept peers
		InputStream in = peerSocketRaw.getInputStream();
		OutputStream out = peerSocketRaw.getOutputStream();
		
		// This will need to be rethought if we ever have different crypto configurations between archives
		byte[] pubKey = new byte[adListeners.getFirst().crypto.asymPublicKeySize()];
		byte[] keyHash = new byte[adListeners.getFirst().crypto.hashLength()];
		byte[] proof = new byte[adListeners.getFirst().crypto.hashLength()];
		
		IOUtils.readFully(in, pubKey);
		IOUtils.readFully(in, keyHash);
		IOUtils.readFully(in, proof);
		
		TCPPeerAdvertisementListener ad = findMatchingAdvertisement(pubKey, keyHash);
		byte[] tempSharedSecret = curve25519.calculateAgreement(pubKey, ad.privateKey);
		byte[] expectedProof = ad.swarm.config.getAccessor().temporalProof(0, tempSharedSecret);
		
		int peerType;
		if(Arrays.equals(expectedProof, proof)) {
			peerType = PeerConnection.PEER_TYPE_FULL;
		} else {
			peerType = PeerConnection.PEER_TYPE_BLIND;
		}
		
		Curve25519KeyPair ephemeralKeyPair = curve25519.generateKeyPair();
		byte[] sharedSecret = curve25519.calculateAgreement(pubKey, ephemeralKeyPair.getPrivateKey());
		out.write(ephemeralKeyPair.getPublicKey());
		out.write(ad.crypto.authenticate(sharedSecret, tempSharedSecret));
		
		return new TCPPeerSocket(ad.swarm, peerSocketRaw, sharedSecret, peerType);
	}
	
	protected TCPPeerAdvertisementListener findMatchingAdvertisement(byte[] pubKey, byte[] keyHash) throws ProtocolViolationException {
		for(TCPPeerAdvertisementListener ad : adListeners) {
			if(ad.matchesKeyHash(pubKey, keyHash)) return ad;
		}
		
		throw new ProtocolViolationException();
	}
	
	protected void assertState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}
}
