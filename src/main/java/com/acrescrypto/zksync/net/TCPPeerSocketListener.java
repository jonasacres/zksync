package com.acrescrypto.zksync.net;

import java.io.EOFException;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;

import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionToken;
import com.acrescrypto.zksync.net.noise.SipObfuscator;
import com.acrescrypto.zksync.net.noise.VariableLengthHandshakeState;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksync.utility.channeldispatcher.ChannelDispatchAcceptor;
import com.acrescrypto.zksync.utility.channeldispatcher.ChannelDispatchMonitor;
import com.dosse.upnp.UPnP;

// I hate this implementation. Burn it.

public class TCPPeerSocketListener {
	public final static int                                MAX_RECENT_PROOFS = 128;
	
	protected int                                          port;
	protected CryptoSupport                                crypto;
	protected Blacklist                                    blacklist;
	protected ServerSocket                                 listenSocket;
	protected ChannelDispatchAcceptor                      dispatchAcceptor;
	protected ZKMaster                                     master;
	protected LinkedList    <TCPPeerAdvertisementListener> adListeners;
	protected boolean                                      closed,
	                                                       established;
	protected PrivateDHKey                                 identityKey;
	protected LinkedList    <SubscriptionToken<?>>         subscriptions = new LinkedList<>();
	
	protected BandwidthMonitor                             bandwidthMonitorTx,
	                                                       bandwidthMonitorRx;

	protected Logger                                       logger = LoggerFactory.getLogger(TCPPeerSocketListener.class);

	public TCPPeerSocketListener(ZKMaster master) throws IOException {
		initMaster(master);
		setupSubscriptions();
		if(master.getGlobalConfig().getBool("net.swarm.enabled")) {
			startListening();
		}
	}
	
	protected void initMaster(ZKMaster master) {
		this.master             = master;
		this.crypto             = master.getCrypto();
		this.blacklist          = master.getBlacklist();
		this.identityKey        = crypto.makePrivateDHKey(); // TODO Noise: cache static key to disk
		this.adListeners        = new LinkedList<>();
		this.bandwidthMonitorRx = new BandwidthMonitor(master.getBandwidthMonitorRx());
		this.bandwidthMonitorTx = new BandwidthMonitor(master.getBandwidthMonitorTx());
		
		logger.info("Swarm - -: TCP listener public key: {}",
				Util.formatPubKey(identityKey.publicKey()));
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
		checkSocketOpen();
	}
	
	public int getPort() {
		return port;
	}
	
	public boolean ready() {
		return listenSocket != null && port != 0 && !listenSocket.isClosed() && !closed; 
	}
	
	public void close() throws IOException {
		if(closed) return;
		closed = true;
		
		if(dispatchAcceptor != null) {
			dispatchAcceptor.close();
		}
		
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
	
	protected void checkSocketOpen() {
		if(listenSocket == null || listenSocket.isClosed()) {
			openSocket();
			if(listenSocket != null && listenSocket.getLocalPort() != port) {
				this.port = listenSocket.getLocalPort();
				announceListening();
			}
		}
	}
	
	protected void openSocket() {
		int lastPort    = master.getGlobalConfig().getInt("net.swarm.lastport");
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
					Util.formatPubKey(identityKey.publicKey()));
			master.getGlobalConfig().set("net.swarm.lastport", listenSocket.getLocalPort());
			rebind();
			registerForDispatch();
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
	
	protected void rebind() {
		int configPort = master.getGlobalConfig().getInt("net.swarm.port");
		if(isListening() && listenSocket.getLocalPort() != configPort && configPort != 0) {
			try {
				close();
			} catch (IOException e) {}
			startListening();
		}
	}
	
	protected void registerForDispatch() throws IOException {
		if(dispatchAcceptor != null) {
			dispatchAcceptor.close();
		}
		
		dispatchAcceptor = new ChannelDispatchAcceptor(
				master.getChannelDispatch(),
				"TCPPeerSocketListener " + listenSocket.getLocalPort(),
				listenSocket.getChannel());
		dispatchAcceptor.setClosedCallback(()->dispatchSawClose());
		dispatchAcceptor.setAcceptCallback((peerChannel)->acceptPeer(peerChannel));
	}
	
	protected void dispatchSawClose() {
		if(closed) return;
		checkSocketOpen();
	}
	
	protected void acceptPeer(SocketChannel peerChannel) {
		try {
			processIncomingPeer(peerChannel);
		} catch(Exception exc) {
			String addr = peerChannel.socket().getInetAddress().getHostAddress();
			logger.info("Swarm - {}: Caught exception accepting peer", addr, exc);
		}
	}
	
	protected void processIncomingPeer(SocketChannel peerChannel) throws IOException {
		String addr = peerChannel.socket().getInetAddress().getHostAddress();
		
		if(blacklist.contains(addr)) {
			logger.info("Swarm - {}: Rejected connection from blacklisted peer", addr);
			peerChannel.close();
			return;
		}
		
		logger.debug("Swarm - {}: Accepted TCP connection from peer",
				peerChannel.socket().getInetAddress().getHostAddress());
		
		ChannelDispatchMonitor peer = new ChannelDispatchMonitor(
				master.getChannelDispatch(),
				"TCP Peer " + addr,
				peerChannel);
		connectPeer(peer);
	}
	
	protected synchronized void announceListening() {
		for(TCPPeerAdvertisementListener listener : adListeners) {
			listener.announce();
		}
	}
	
	protected void connectPeer(ChannelDispatchMonitor peer) {
		String addr      = peer.getChannel().socket().getInetAddress().getHostAddress();
		
		Util.ensure(TCPPeerSocket.maxHandshakeTimeMillis, 10, ()->established, ()->{
			logger.debug("Swarm - {}: Closing socket since handshake was not completed within {}ms",
					addr,
					TCPPeerSocket.maxHandshakeTimeMillis);
			peer.close();
		});
		
		if(adListeners.isEmpty()) {
			// not ready to accept peers
			logger.error("Swarm - {} attempted to connect before any listeners were ready", addr);
			// TODO: blacklist?
			return;
		}
		
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
				
				(ppeer, callback)->{
					Key sym = new Key(crypto, crypto.makeSymmetricKey(identityKey.publicKey().getBytes()));
					ppeer.expect(crypto.asymPublicDHKeySize(), (obfuscatedKey)->{
						byte[] plaintextKey  = sym.decryptUnauthenticated(new byte[crypto.symIvLength()], obfuscatedKey.array());
						callback.deobfuscated(plaintextKey, obfuscatedKey.array());
					});
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
		
		handshake.setExceptionHandler((exception)->{
			long expiration = Util.currentTimeMillis() + TCPPeerSocket.socketCloseDelay;
			
			try {
				throw(exception);
			} catch(EOFException exc) {
				logger.debug("Swarm - {}: Peer disconnected during handshake; possibly a reachability probe.", addr);
			} catch(ProtocolViolationException exc) {
				logger.info ("Swarm - {}: Peer sent illegal handshake", addr, exc);
				Util.delayUntil(expiration, ()->peer.close());
				// TODO: blacklist?
			} catch(IOException exc) {
				logger.info ("Swarm - {}: Caught IOException on connection to peer", addr, exc);
				Util.delayUntil(expiration, ()->peer.close());
			} catch(SecurityException exc) {
				logger.info ("Swarm - {}: Unable to handshake with peer", addr, exc);
				Util.delayUntil(expiration, ()->peer.close());
			} catch(Exception exc) {
				logger.error("Swarm - {}: Caught unexpected exception on connection to peer", addr, exc);
				Util.delayUntil(expiration, ()->peer.close());
			}
		});
		
		handshake.handshake(peer, (rx, tx)->{
			established = true;
			
			/* Creating this object automatically adds it to the peer swarm, via a wrapped
			 * PeerConnection, where it is subsequently monitored. The static analyzer does
			 * not see this and flags this as an unused variable.
			 * 
			 * 2020-06-09 JDK 11, Eclipse 2020-03
			 */
			@SuppressWarnings("unused")
			TCPPeerSocket socket = new TCPPeerSocket(ad.value.swarm,
					handshake.getRemoteStaticKey(),
					peer,
					rx,
					tx,
					sip[0],
					handshake.getHash(),
					peerType.intValue(),
					portNum.intValue());
		});
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
