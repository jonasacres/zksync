package com.acrescrypto.zksync.net;

import java.io.EOFException;
import java.io.IOException;
import java.io.StringReader;
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
import com.acrescrypto.zksync.net.noise.CipherState;
import com.acrescrypto.zksync.net.noise.SipObfuscator;
import com.acrescrypto.zksync.net.noise.VariableLengthHandshakeState;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.RateLimitedInputStream;
import com.acrescrypto.zksync.utility.RateLimitedOutputStream;
import com.acrescrypto.zksync.utility.Util;
import com.dosse.upnp.UPnP;

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
	
	protected BandwidthMonitor bandwidthMonitorTx, bandwidthMonitorRx;
	
	public TCPPeerSocketListener(ZKMaster master) throws IOException {
		initMaster(master);
		setupSubscriptions();
	}
	
	protected TCPPeerSocketListener(ZKMaster master, int port) {
		initMaster(master);
		startListening(port);
	}
	
	protected void initMaster(ZKMaster master) {
		this.crypto = master.getCrypto();
		this.blacklist = master.getBlacklist();
		this.master = master;
		this.adListeners = new LinkedList<>();
		this.identityKey = crypto.makePrivateDHKey(); // TODO Noise: cache static key to disk
		this.bandwidthMonitorRx = new BandwidthMonitor(master.getBandwidthMonitorRx());
		this.bandwidthMonitorTx = new BandwidthMonitor(master.getBandwidthMonitorTx());
	}
	
	protected void setupSubscriptions() {
		master.getGlobalConfig().subscribe("net.swarm.enabled").asBoolean(false, (enabled)->{
			if(enabled == isListening()) return;
			if(enabled) {
				startListening(master.getGlobalConfig().getInt("net.swarm.port", 0));
			} else {
				try {
					close();
				} catch (IOException e) {}
			}
		});
		
		master.getGlobalConfig().subscribe("net.swarm.port").asInt(0, (port)->{
			if(isListening() && listenSocket.getLocalPort() != port) {
				try {
					close();
				} catch (IOException e) {}
				startListening(master.getGlobalConfig().getInt("net.swarm.port", 0));
			}
		});
		
		master.getGlobalConfig().subscribe("net.swarm.upnp").asBoolean(false, (enabled)->{
			if(enabled && isListening()) {
				if(!UPnP.isMappedTCP(port)) {
					UPnP.openPortTCP(port);
				}
			} else if(port != 0 && UPnP.isMappedTCP(port)) {
				UPnP.closePortTCP(port);
			}
		});
	}
	
	protected void startListening(int port) {
		this.requestedPort = port;
		if(port == 0 && master != null && master.getGlobalConfig() != null) {
			this.port = master.getGlobalConfig().getInt("net.swarm.port", 0);
		} else {
			this.port = port;
		}
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
			listenSocket.getLocalPort();
			listenSocket.close();
			if(master.getGlobalConfig().getBool("net.swarm.upnp", false)) {
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
			if(master.getGlobalConfig().getBool("net.swarm.upnp", false)) {
				UPnP.openPortTCP(listenSocket.getLocalPort());
			}
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
		master.getGlobalConfig().set("net.swarm.port", this.port);
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
			sip[0] = new SipObfuscator(key.derive(0, SipObfuscator.SIP_OBFUSCATOR_ASK_NAME.getBytes()).getRaw(), false);
		});
		
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
