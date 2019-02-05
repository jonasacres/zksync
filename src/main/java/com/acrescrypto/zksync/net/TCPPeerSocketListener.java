package com.acrescrypto.zksync.net;

import java.io.EOFException;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionToken;
import com.acrescrypto.zksync.net.noise.CipherState;
import com.acrescrypto.zksync.net.noise.SipObfuscator;
import com.acrescrypto.zksync.net.noise.VariableLengthHandshakeState;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.RateLimitedInputStream;
import com.acrescrypto.zksync.utility.RateLimitedOutputStream;
import com.acrescrypto.zksync.utility.Util;
import com.dosse.upnp.UPnP;

// I hate this implementation. Burn it.

public class TCPPeerSocketListener {
	public final static int MAX_RECENT_PROOFS = 128;
	protected int port;
	
	protected CryptoSupport crypto;
	protected Blacklist blacklist;
	protected ServerSocket listenSocket;
	protected ZKMaster master;
	protected Thread thread;
	protected Logger logger = LoggerFactory.getLogger(TCPPeerSocketListener.class);
	protected LinkedList<TCPPeerAdvertisementListener> adListeners;
	protected boolean closed, established;
	protected PrivateDHKey identityKey;
	protected LinkedList<SubscriptionToken<?>> subscriptions = new LinkedList<>();
	
	protected BandwidthMonitor bandwidthMonitorTx, bandwidthMonitorRx;
	
	public TCPPeerSocketListener(ZKMaster master) throws IOException {
		initMaster(master);
		setupSubscriptions();
		if(master.getGlobalConfig().getBool("net.swarm.enabled")) {
			startListening();
		}
	}
	
	protected void initMaster(ZKMaster master) {
		this.crypto = master.getCrypto();
		this.blacklist = master.getBlacklist();
		this.master = master;
		this.adListeners = new LinkedList<>();
		this.identityKey = crypto.makePrivateDHKey(); // TODO Noise: cache static key to disk
		this.bandwidthMonitorRx = new BandwidthMonitor(master.getBandwidthMonitorRx());
		this.bandwidthMonitorTx = new BandwidthMonitor(master.getBandwidthMonitorTx());
		
		logger.info("Swarm - {}: TCP listener public key: {}",
				Util.bytesToHex(identityKey.getBytes()));
	}
	
	protected void setupSubscriptions() {
		subscriptions.add(master.getGlobalConfig().subscribe("net.swarm.enabled").asBoolean((enabled)->{
			synchronized(this) {
				if(enabled == isListening()) return;
				if(enabled) {
					startListening();
				} else {
					try {
						close();
					} catch (IOException e) {}
				}
			}
		}));
		
		subscriptions.add(master.getGlobalConfig().subscribe("net.swarm.port").asInt((port)->{
			synchronized(this) {
				rebind();
			}
		}));
		
		subscriptions.add(master.getGlobalConfig().subscribe("net.swarm.upnp").asBoolean((enabled)->{
			if(enabled && isListening()) {
				if(!UPnP.isMappedTCP(port)) {
					UPnP.openPortTCP(port);
				}
			} else if(port != 0 && UPnP.isMappedTCP(port)) {
				UPnP.closePortTCP(port);
			}
		}));
	}
	
	protected void startListening() {
		closed = false;
		this.thread = new Thread(master.getThreadGroup(), ()->listenThread() );
		this.thread.start();
	}
	
	public int getPort() {
		return port;
	}
	
	public boolean ready() {
		return listenSocket != null && port != 0 && !listenSocket.isClosed() && !closed; 
	}
	
	protected void rebind() {
		int configPort = master.getGlobalConfig().getInt("net.swarm.port");
		if(isListening() && listenSocket.getLocalPort() != configPort && configPort != 0) {
			try {
				close();
			} catch (IOException e) {}
			startListening();
		}
	}
	
	public void close() throws IOException {
		if(closed) return;
		closed = true;
		for(SubscriptionToken<?> sub : subscriptions) {
			sub.close();
		}
		subscriptions.clear();
		
		if(listenSocket != null) {
			listenSocket.close();
			if(master.getGlobalConfig().getBool("net.swarm.upnp")) {
				UPnP.closePortTCP(port);
			}
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
				if(closed || oldSocket != listenSocket || listenSocket.isClosed()) {
					logger.info("Swarm - -: Closed TCP socket on port {}", listenSocket.getLocalPort());
					break;
				}
				
				logger.error("Swarm - -: TCP listen thread on port " + port + " caught exception", exc);
			}
		}
	}
	
	protected void checkSocketOpen() {
		if(listenSocket == null || listenSocket.isClosed()) {
			openSocket();
			if(listenSocket != null && listenSocket.getLocalPort() != port) {
				this.port = listenSocket.getLocalPort();
				announceListening();
			}
		}
	}
	
	protected void processIncomingPeer(Socket socket) throws IOException {
		if(blacklist.contains(socket.getInetAddress().getHostAddress())) {
			logger.info("Swarm - {}: Rejected connection from blacklisted peer", socket.getInetAddress().getHostAddress());
			socket.close();
			return;
		}
		
		logger.debug("Swarm - {}: Accepted TCP connection from peer",
				socket.getInetAddress().getHostAddress());
		new Thread(master.getThreadGroup(), ()->peerThread(socket) ).start();
	}
	
	protected void openSocket() {
		int lastPort = master.getGlobalConfig().getInt("net.swarm.lastport");
		int requestPort = master.getGlobalConfig().getInt("net.swarm.port");
		if(lastPort != 0 && port == 0) {
			logger.debug("Swarm - -: Attempting to require previously-bound TCP port {}", lastPort);
			requestPort = lastPort;
		}
		
		try {
			listenSocket = new ServerSocket(requestPort,
					master.getGlobalConfig().getInt("net.swarm.backlog"),
					InetAddress.getByName(master.getGlobalConfig().getString("net.swarm.bindaddress")));
			listenSocket.setReuseAddress(true);

			if(master.getGlobalConfig().getBool("net.swarm.upnp")) {
				UPnP.openPortTCP(listenSocket.getLocalPort());
			}
			
			logger.info("Swarm - -: Listening on TCP port {} with public key {}",
					listenSocket.getLocalPort(),
					Util.bytesToHex(identityKey.publicKey().getBytes()));
			master.getGlobalConfig().set("net.swarm.lastport", listenSocket.getLocalPort());
			rebind();
		} catch(IOException exc) {
			if(port == 0 && requestPort != 0) {
				logger.warn("Swarm - -: Unable to re-acquire TCP port {}, requesting new port number...", requestPort, exc);
				master.getGlobalConfig().set("net.swarm.lastport", 0);
			} else if(port == 0) {
				logger.warn("Swarm - -: Caught exception requesting random port; waiting to retry...", exc);
				try { Thread.sleep(1000); } catch(InterruptedException exc2) {}
				return;
			} else {
				logger.warn("Swarm - -: Unable to acquire configured TCP port {}; re-requesting with same port number...",
						port);
			}
		}
	}
	
	protected synchronized void announceListening() {
		for(TCPPeerAdvertisementListener listener : adListeners) {
			listener.announce();
		}
	}
	
	protected void peerThread(Socket peerSocketRaw) {
		long startTime = Util.currentTimeMillis();
		try {
			performResponderHandshake(peerSocketRaw);
		} catch(EOFException exc) {
			logger.debug("Swarm - {}: Peer disconnected during handshake; possibly a reachability probe.",
					peerSocketRaw.getInetAddress().getHostAddress());
		} catch(ProtocolViolationException exc) {
			logger.info("Swarm - {}: Peer sent illegal handshake", peerSocketRaw.getInetAddress().getHostAddress(), exc);
			long delay = startTime + TCPPeerSocket.socketCloseDelay - Util.currentTimeMillis();
			Util.delay(delay, ()->peerSocketRaw.close());
		} catch(IOException exc) {
			logger.info("Swarm - {}: Caught IOException on connection to peer", peerSocketRaw.getInetAddress().getHostAddress(), exc);
			long delay = startTime + TCPPeerSocket.socketCloseDelay - Util.currentTimeMillis();
			Util.delay(delay, ()->peerSocketRaw.close());
		} catch(SecurityException exc) {
			logger.info("Swarm - {}: Unable to handshake with peer", peerSocketRaw.getInetAddress().getHostAddress(), exc);
			long delay = startTime + TCPPeerSocket.socketCloseDelay - Util.currentTimeMillis();
			Util.delay(delay, ()->peerSocketRaw.close());
		} catch(Exception exc) {
			logger.error("Swarm - {}: Caught unexpected exception on connection to peer", peerSocketRaw.getInetAddress().getHostAddress(), exc);
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
		
		RateLimitedInputStream in = new RateLimitedInputStream(peerSocketRaw.getInputStream(),
				master.getBandwidthAllocatorRx(),
				bandwidthMonitorRx);
		RateLimitedOutputStream out = new RateLimitedOutputStream(peerSocketRaw.getOutputStream(),
				master.getBandwidthAllocatorTx(),
				bandwidthMonitorTx);
		
		class MutableAdListener {
			TCPPeerAdvertisementListener value;
		}
		
		SipObfuscator[] sip = new SipObfuscator[1];
		MutableAdListener ad = new MutableAdListener();
		MutableInt peerType = new MutableInt(), portNum = new MutableInt();
		VariableLengthHandshakeState handshake = new VariableLengthHandshakeState(
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

		handshake.setDerivationCallback((key)->{
			sip[0] = new SipObfuscator(key.derive(SipObfuscator.SIP_OBFUSCATOR_ASK_NAME).getRaw(), false);
		});
		
		handshake.setObfuscation(
				(key)->{
					Key sym = new Key(crypto, crypto.makeSymmetricKey(handshake.getRemoteEphemeralKey().getBytes()));
					return sym.encryptUnauthenticated(new byte[crypto.symIvLength()], key.getBytes());
				},
				
				(inn)->{
					Key sym = new Key(crypto, crypto.makeSymmetricKey(identityKey.publicKey().getBytes()));
					byte[] ciphertext = IOUtils.readFully(inn, crypto.asymPublicDHKeySize());
					byte[] keyRaw = sym.decryptUnauthenticated(new byte[crypto.symIvLength()], ciphertext);
					return new byte[][] { keyRaw, ciphertext };
				}
			);
		
		handshake.setSimplePayload(
			(round)->{
				int padding = crypto.defaultPrng().getInt(128);
				JsonObjectBuilder builder = Json
						.createObjectBuilder()
						.add("padding", Util.bytesToHex(new byte[padding]));
				
				if(round == 4) {
					byte[] proof;
					if(peerType.intValue() == PeerConnection.PEER_TYPE_FULL) {
						proof = ad.value.swarm.getConfig().getAccessor().temporalProof(0, 1, handshake.getHash());
					} else {
						proof = crypto.rng(crypto.symKeyLength());
					}
				
					builder.add("proof", Util.bytesToHex(proof));
				}
				
				return builder.build().toString().getBytes();
			},
			
			(round, payload)->{
				if(round != 3) return;
				
				JsonReader reader = Json.createReader(new StringReader(new String(payload)));
				JsonObject json = reader.readObject();
				byte[] idHash = Util.hexToBytes(json.getJsonString("idHash").getString());
				byte[] proof = Util.hexToBytes(json.getJsonString("proof").getString());
				portNum.setValue(json.getInt("port"));
				
				try {
					ad.value = findMatchingAdvertisement(handshake.getPreHash(), idHash);
				} catch (ProtocolViolationException e) {
					throw new SecurityException("no archive matching request");
				}
				
				handshake.setPsk(ad.value.swarm.config.getArchiveId());
				byte[] expectedProof = ad.value.swarm.config.getAccessor().temporalProof(0, 0, handshake.getPreHash());
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
				sip[0],
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
	
	public BandwidthMonitor getBandwidthMonitorRx() {
		return bandwidthMonitorRx;
	}
	
	public BandwidthMonitor getBandwidthMonitorTx() {
		return bandwidthMonitorTx;
	}
}
