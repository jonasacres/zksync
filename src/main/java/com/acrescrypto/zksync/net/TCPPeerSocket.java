package com.acrescrypto.zksync.net;

import java.io.EOFException;
import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.net.dht.BenignProtocolViolationException;
import com.acrescrypto.zksync.net.noise.CipherState;
import com.acrescrypto.zksync.net.noise.HandshakeState;
import com.acrescrypto.zksync.net.noise.SipObfuscator;
import com.acrescrypto.zksync.net.noise.VariableLengthHandshakeState;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksync.utility.channeldispatcher.ChannelDispatchMonitor;

public class TCPPeerSocket extends PeerSocket {
	public final static int MAX_MSG_LEN                       =   65504; // maximum plaintext length (not including tag)
	public final static int DEFAULT_MAX_HANDSHAKE_TIME_MILLIS = 60*1000; // 1 minute
	public final static int DEFAULT_SOCKET_CLOSE_DELAY        =  5*1000; // 5 seconds
	
	public       static int maxHandshakeTimeMillis = DEFAULT_MAX_HANDSHAKE_TIME_MILLIS; // maximum time to handshake before automatic disconnect
	public       static int socketCloseDelay       = DEFAULT_SOCKET_CLOSE_DELAY;
	
	protected static boolean disableMakeThreads; // test purposes
	
	public final static String HANDSHAKE_PATTERN = "XKpsk4+id+pskfromid:\n" + 
			"  <- s\n" + 
			"  ...\n" + 
			"  -> e, es\n" + 
			"  <- e, ee\n" + 
			"  -> s, se\n" + 
			"  <- psk";
	public final static String HANDSHAKE_PROTOCOL = "Noise_XKpsk4+id+pskfromid_25519_ChaChaPoly_BLAKE2b";

	protected ChannelDispatchMonitor  peer;
	protected boolean                 isLocalRoleClient;
	protected CryptoSupport           crypto;
	protected Key                     localChainKey,
	                                  remoteChainKey;
	protected ByteBuffer              remainingReadData;
	protected TCPPeerAdvertisement    ad;
	protected int                     peerType             = -1;
	protected CipherState             readState,
	                                  writeState;
	protected byte[]                  sharedSecret;
	protected SipObfuscator           sip;
	
	protected TCPPeerSocket() {}
	
	public TCPPeerSocket(PeerSwarm swarm, PublicDHKey remoteIdentityKey, ChannelDispatchMonitor peer, CipherState rx, CipherState tx, SipObfuscator sip, byte[] handshakeHash, int peerType, int portNum) throws IOException, ProtocolViolationException {
		this(swarm);
		this.sip               = sip;
		this.peer              = peer;
		this.isLocalRoleClient = false;
		this.peerType          = peerType;
		this.readState         = rx;
		this.writeState        = tx;
		this.sharedSecret      = handshakeHash;
		this.remoteIdentityKey = remoteIdentityKey;
		this.address           = peer.getChannel().socket().getInetAddress().getHostAddress();
		
		if(portNum != 0) {
			// TODO API: (coverage) branch coverage
			// TODO API: (test) Spookier realization: also need to test that ads are set iff port != 0!
			byte[] encryptedArchiveId = swarm.config.getEncryptedArchiveId(remoteIdentityKey.getBytes());
			this.ad = new TCPPeerAdvertisement(
					remoteIdentityKey,
					address,
					portNum,
					encryptedArchiveId
				);
		}
		
		setSocketParameters();
		swarm.openedConnection(new PeerConnection(this));
		
		logger.debug("Swarm {} {}:{}: TCPSocket opened from received connection",
				Util.formatArchiveId(swarm.config.getArchiveId()),
				this.address,
				peer.getChannel().socket().getPort());
	}
	
	public TCPPeerSocket(PeerSwarm swarm, TCPPeerAdvertisement ad) throws IOException, BlacklistedException {
		this(swarm);
		this.ad                = ad;
		this.swarm             = swarm;
		this.crypto            = swarm.config.getAccessor().getMaster().getCrypto();
		this.isLocalRoleClient = true;
		this.remoteIdentityKey = ad.pubKey;
		try {
			ad.resolve();
		} catch (UnconnectableAdvertisementException exc) {
			logger.warn("Swarm {} {}:{}: TCPSocket connect failed, to resolve host: {}",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					this.address,
					this.getPort(),
					ad.host);
			throw new IOException(exc);
		}
		if(ad.isBlacklisted(swarm.config.getAccessor().getMaster().getBlacklist())) throw new BlacklistedException(ad.host);
	}
	
	public void handshake(PeerConnection connection, PeerSocketHandshakeCallback callback) throws IOException {
		if(this.connection == null) connect(connection);		
		sendHandshake(callback);
	}
	
	protected void connect(PeerConnection connection) throws IOException {
		String effectiveHost = ad.host;
		if(effectiveHost.equals("127.0.0.1") && ad.getSenderHost() != null) {
			logger.info("Swarm {} {}:{}: TCPSocket connect overriding ad from {} to {}",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					this.address,
					this.getPort(),
					effectiveHost);
		}
		this.connection = connection;
		
		/* The socket gets closed when the ChannelDispatchMonitor does, but the static
		 * analyzer doesn't see that and throws a warning.
		 * 
		 * 2020-06-09, JDK 11, Eclipse 2020-03
		 * */
		@SuppressWarnings("resource")
		Socket socket   = new Socket(ad.host, ad.port);
		this.address    = socket.getInetAddress().getHostAddress();
		
		peer = new ChannelDispatchMonitor(
				swarm.getConfig().getMaster().getChannelDispatch(),
				"TCPPeerSocket to " + ad.host + ":" + ad.port,
				socket.getChannel()
			);
		
		setSocketParameters();
		logger.debug("Swarm {} {}:{}: TCPSocket connecting to ad (host={})",
				Util.formatArchiveId(swarm.config.getArchiveId()),
				this.address,
				ad.port,
				effectiveHost);
	}
	
	protected TCPPeerSocket(PeerSwarm swarm) throws IOException {
		super(swarm);
		this.swarm = swarm;
		this.crypto = swarm.config.getAccessor().getMaster().getCrypto();
	}
	
	protected HandshakeState setupHandshakeState() {
		VariableLengthHandshakeState handshake = new VariableLengthHandshakeState(crypto,
				HANDSHAKE_PROTOCOL,
				HANDSHAKE_PATTERN,
				true,
				new byte[0],
				swarm.getConfig().getMaster().getTCPListener().getIdentityKey(),
				null,
				ad.getPubKey(),
				null,
				swarm.getConfig().getArchiveId());
		
		handshake.setDerivationCallback((key)->{
			sip = new SipObfuscator(key.derive(SipObfuscator.SIP_OBFUSCATOR_ASK_NAME).getRaw(), true);
		});
		
		handshake.setObfuscation(
			(key)->{
				Key sym = new Key(crypto, crypto.makeSymmetricKey(handshake.getRemoteStaticKey().getBytes()));
				return sym.encryptUnauthenticated(new byte[crypto.symIvLength()], key.getBytes());
			},
			
			(peer, callback)->{
				Key sym = new Key(crypto, crypto.makeSymmetricKey(handshake.getLocalEphemeralKey().getBytes()));
				peer.expect(crypto.asymPublicDHKeySize(), (obfuscatedKey)->{
					byte[] plaintextKey  = sym.decryptUnauthenticated(new byte[crypto.symIvLength()], obfuscatedKey.array());
					callback.deobfuscated(plaintextKey, obfuscatedKey.array());
				});
			}
		);
		
		handshake.setSimplePayload(
			(round)->{
				JsonObjectBuilder builder = Json
						.createObjectBuilder()
						.add("padding", Util.bytesToHex(new byte[crypto.defaultPrng().getInt(128)]));
				
				if(round == 3) {
					byte[] id = crypto.hash(Util.concat(handshake.getHash(), swarm.config.getArchiveId()));
					byte[] proof = swarm.getConfig().getAccessor().temporalProof(0, 0, handshake.getHash());
					
					builder.add("idHash", Util.bytesToHex(id))
					       .add("proof", Util.bytesToHex(proof))
					       .add("port", swarm.getConfig().getMaster().getTCPListener().getPort());
				}
				
				return builder.build().toString().getBytes();
			},
			
			(round, payload)->{
				if(round != 4) return;
				byte[] expectedProof = swarm.config.getAccessor().temporalProof(0, 1, handshake.getPreHash());
				
				JsonReader reader = Json.createReader(new StringReader(new String(payload)));
				JsonObject json = reader.readObject();
				byte[] proof = Util.hexToBytes(json.getString("proof"));
				
				if(Util.safeEquals(expectedProof, proof)) {
					peerType = PeerConnection.PEER_TYPE_FULL;
				} else {
					peerType = PeerConnection.PEER_TYPE_BLIND;
				}
			}
		);
		
		return handshake;
	}
	
	protected void sendHandshake(PeerSocketHandshakeCallback callback) {
		logger.trace("Swarm {} {}:{}: TCPSocket sending handshake",
				Util.formatArchiveId(swarm.config.getArchiveId()),
				address,
				getPort());
		HandshakeState handshake = setupHandshakeState();
		handshake.handshake(peer, (rxState, txState)->{
			this.readState  = rxState;
			this.writeState = txState;
			
			if(connection == null) {
				this.connection = new PeerConnection(this);
			}
			
			this.sharedSecret = handshake.getHash();
			logger.debug("Swarm {} {}:{}: TCPSocket completed handshake",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					address,
					getPort());
			
			try {
				if(callback != null) callback.ready();
			} catch(Exception exc) {
				handleException(exc);
			}
		});
	}
	
	protected void peerChannelClosed() {
		logger.debug("Swarm {} {}:{}: PeerSocket received socket close notification, closed={}",
				Util.formatArchiveId(swarm.config.getArchiveId()),
				getAddress(),
				getPort(),
				isClosed());
		try {
			close();
		} catch (IOException exc) {
			logger.debug("Swarm {} {}:{}: PeerSocket encountered exception closing in response to socket close notification",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					getAddress(),
					getPort(),
					exc);
		}
	}
	
	@Override
	public synchronized void write(ByteBuffer data, PeerSocketWriteCallback callback) throws IOException {
		while(data.hasRemaining()) {
			int writeLen = Math.min(data.remaining(), MAX_MSG_LEN);
			logger.trace("Swarm {} {}:{}: TCPSocket sending {} bytes",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					address,
					getPort(),
					writeLen);
			byte[] ciphertext = writeState.encryptWithAssociatedData(
					null,
					data.array(),
					data.position(),
					writeLen);
			data.position(data.position() + writeLen);
			
			int obfLen = sip.write().obfuscate2(ciphertext.length);
			
			// TODO: put a callback on this, accept as an argument
			peer.send(Util.serializeShort((short) obfLen));
			peer.send(ByteBuffer.wrap(ciphertext), ()->{
				callback.sent();
			});
		}
	}
	
	@Override
	public void read(int length, PeerSocketReadCallback callback) {
		ByteBuffer readBytes = ByteBuffer.allocate(length);
		expectNextChunk(readBytes, callback);
	}
	
	public void expectNextChunk(ByteBuffer readBytes, PeerSocketReadCallback callback) {
		loadNextFrameFromWire((chunkData)->{
			readBytes.put(chunkData);
			
			logger.trace("Swarm {} {}:{}: TCPSocket read {} bytes from new buffer, {} total read, {} remaining in read operation",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					address,
					getPort(),
					chunkData.limit(),
					readBytes.position(),
					readBytes.remaining());
			
			if(!readBytes.hasRemaining()) {
				// we're already done!
				callback.received(readBytes);
				return;
			}
			
			expectNextChunk(readBytes, callback);
		});
	}
	
	public void loadNextFrameFromWire(PeerSocketReadCallback callback) {
		if(remainingReadData != null && remainingReadData.hasRemaining()) {
			// if we have leftover data from the last frame, use it up first
			try {
				callback.received(remainingReadData);
			} catch(Exception exc) {
				handleException(exc);
			}
			return;
		}
		
		peer.expect(2, (msgLenBuf)->{
			// deobfuscate the message length and then get the frame payload
			int obfMsgLen = msgLenBuf.getShort();
			int msgLen    = sip.read().obfuscate2(obfMsgLen);
			int maxMsgLen = MAX_MSG_LEN
					      + crypto.symBlockSize()
					      + crypto.symTagLength();
			
			assertState(     0 <  msgLen   );
			assertState(msgLen <= maxMsgLen);
			
			peer.expect(msgLen, (ciphertextBuf)->{
				byte[] plaintext  = readState.decryptWithAssociatedData(new byte[0], ciphertextBuf.array());
				remainingReadData = ByteBuffer.wrap(plaintext);
				callback.received(remainingReadData);
			});
		});
	}
	
	@Override
	public boolean isLocalRoleClient() {
		return isLocalRoleClient;
	}

	@Override
	public void _close() throws IOException {
		if(peer != null && !peer.closed()) {
			logger.trace("Swarm {} {}:{}: TCPSocket closing",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					address,
					getPort());
			peer.close();
		}

		if(connection != null) {
			connection.close();
		}
	}

	@Override
	public boolean isClosed() {
		return peer != null && peer.closed();
	}

	@Override
	public byte[] getSharedSecret() {
		return sharedSecret;
	}
	
	@Override
	public TCPPeerAdvertisement getAd() {
		return ad;
	}
	
	@Override
	public int getPeerType() {
		return peerType;
	}
	
	@Override
	public int getPort() {
		return peer == null ? -1 : peer.getChannel().socket().getPort();
	}
	
	@Override
	public BandwidthMonitor getMonitorRx() {
		return peer.getBandwidthMonitorRx();
	}
	
	@Override
	public BandwidthMonitor getMonitorTx() {
		return peer.getBandwidthMonitorTx();
	}
	
	@Override
	public boolean matchesAddress(String address) {
		if(this.address.equals(address)) return true;
		
		try {
			InetAddress addr = InetAddress.getByName(address);
			return this.address.equals(addr);
		} catch(IOException exc) {
			return false;
		}
	}
	
	protected void setSocketParameters() throws IOException {
		peer.getChannel().socket().setSoTimeout (0);
		peer.getChannel().socket().setKeepAlive (true);
		peer.getChannel().socket().setTcpNoDelay(true);
		peer.setClosedCallback( () -> peerChannelClosed() );
		peer.setDefaultExceptionHandler( (exc) -> handleException(exc) );
	}
	
	protected void handleException(Exception exc) {
		try {
			throw(exc);
		} catch(SocketException|EOFException xx) { // socket closed; just ignore it
			logger.debug("Swarm {} {}:{}: PeerSocket unable to read socket, closed={}",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					getAddress(),
					getPort(),
					isClosed(),
					xx);
			safeClose();
		} catch(BenignProtocolViolationException xx) {
			logger.info("Swarm {} {}:{}: PeerSocket caught suspicious (but possibly benign) protocol violation; closing socket",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					getAddress(),
					getPort(),
					xx);
			safeClose();
		} catch(ProtocolViolationException xx) {
			logger.info("Swarm {} {}:{}: PeerSocket caught unacceptable (possibly malicious) protocol violation; blacklisting peer",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					getAddress(),
					getPort(),
					xx);
			violation();
		} catch(SecurityException xx) {
			logger.trace("Swarm {} {}:{}: TCPSocket failed to decrypt ciphertext; closing socket",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					address,
					getPort());
			safeClose();
		} catch(Exception xx) {
			logger.info("Swarm {} {}:{}: PeerSocket caught unexpected exception; blacklisting peer",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					getAddress(),
					getPort(),
					xx);
			violation();
		}
	}

	protected void assertState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}
}
