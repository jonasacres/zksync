package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;

import org.apache.commons.io.IOUtils;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.SocketClosedException;
import com.acrescrypto.zksync.net.noise.CipherState;
import com.acrescrypto.zksync.net.noise.HandshakeState;
import com.acrescrypto.zksync.net.noise.SipObfuscator;
import com.acrescrypto.zksync.net.noise.VariableLengthHandshakeState;
import com.acrescrypto.zksync.utility.RateLimitedOutputStream;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.RateLimitedInputStream;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerSocket extends PeerSocket {
	public final static int MAX_MSG_LEN = 65504; // maximum plaintext length (not including tag)
	public final static int DEFAULT_MAX_HANDSHAKE_TIME_MILLIS = 60*1000; // 1 minute
	public final static int DEFAULT_SOCKET_CLOSE_DELAY = 5*1000; // 5 seconds
	public static int maxHandshakeTimeMillis = DEFAULT_MAX_HANDSHAKE_TIME_MILLIS; // maximum time to handshake before automatic disconnect
	public static int socketCloseDelay = DEFAULT_SOCKET_CLOSE_DELAY;
	protected static boolean disableMakeThreads; // test purposes
	
	public final static String HANDSHAKE_PATTERN = "XKpsk4+id+pskfromid:\n" + 
			"  <- s\n" + 
			"  ...\n" + 
			"  -> e, es\n" + 
			"  <- e, ee\n" + 
			"  -> s, se\n" + 
			"  <- psk";
	public final static String HANDSHAKE_PROTOCOL = "Noise_XKpsk4+id+pskfromid_25519_AESOCB_BLAKE2b";

	protected Socket socket;
	protected RateLimitedOutputStream out;
	protected RateLimitedInputStream in;
	protected boolean isLocalRoleClient;
	protected CryptoSupport crypto;
	protected Key localChainKey, remoteChainKey;
	protected ByteBuffer remainingReadData;
	protected TCPPeerAdvertisement ad;
	protected int peerType = -1;
	protected CipherState readState, writeState;
	protected byte[] sharedSecret;
	protected SipObfuscator sip;
	
	protected TCPPeerSocket() {}
	
	public TCPPeerSocket(PeerSwarm swarm, PublicDHKey remoteIdentityKey, Socket socket, CipherState[] states, SipObfuscator sip, byte[] handshakeHash, int peerType, int portNum) throws IOException {
		this(swarm);
		this.address = socket.getInetAddress().getHostAddress();
		this.socket = socket;
		this.isLocalRoleClient = false;
		this.readState = states[0];
		this.writeState = states[1];
		this.sip = sip;
		this.peerType = peerType;
		this.remoteIdentityKey = remoteIdentityKey;
		this.sharedSecret = handshakeHash;
		
		if(portNum != 0) {
			// TODO API: (coverage) branch coverage
			// TODO API: (test) Spookier realization: also need to test that ads are set iff port != 0!
			byte[] encryptedArchiveId = swarm.config.getEncryptedArchiveId(remoteIdentityKey.getBytes());
			this.ad = new TCPPeerAdvertisement(remoteIdentityKey, socket.getInetAddress().getHostAddress(), portNum, encryptedArchiveId);
		}
		
		makeStreams();
		swarm.openedConnection(new PeerConnection(this));
		makeThreads();
	}
	
	public TCPPeerSocket(PeerSwarm swarm, TCPPeerAdvertisement ad) throws IOException, BlacklistedException {
		this(swarm);
		this.ad = ad;
		this.swarm = swarm;
		this.crypto = swarm.config.getAccessor().getMaster().getCrypto();
		this.isLocalRoleClient = true;
		this.remoteIdentityKey = ad.pubKey;
		if(ad.isBlacklisted(swarm.config.getAccessor().getMaster().getBlacklist())) throw new BlacklistedException(ad.host);
	}
	
	public void handshake(PeerConnection connection) throws ProtocolViolationException, IOException {
		if(this.connection == null) connect(connection);
		
		try {
			sendHandshake();
		} catch(SecurityException exc) {
			this.violation();
			throw exc;
		}
	}
	
	protected void connect(PeerConnection connection) throws IOException {
		this.connection = connection;
		this.socket = new Socket(ad.host, ad.port);
		this.address = socket.getInetAddress().getHostAddress();
		makeStreams();
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
			
			(in)->{
				Key sym = new Key(crypto, crypto.makeSymmetricKey(handshake.getLocalEphemeralKey().getBytes()));
				byte[] ciphertext = IOUtils.readFully(in, crypto.asymPublicDHKeySize());
				byte[] keyRaw = sym.decryptUnauthenticated(new byte[crypto.symIvLength()], ciphertext);
				return new byte[][] { keyRaw, ciphertext };
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
	
	protected void sendHandshake() throws IOException {
		HandshakeState handshake = setupHandshakeState();
		CipherState[] states = handshake.handshake(in, out);				
		readState = states[0];
		writeState = states[1];
		makeThreads();
		if(connection == null) {
			this.connection = new PeerConnection(this);
		}
		
		this.sharedSecret = handshake.getHash();
	}
	
	@Override
	public synchronized void write(byte[] data, int offset, int length) throws IOException {
		ByteBuffer buf = ByteBuffer.wrap(data, offset, length);
		
		while(buf.hasRemaining()) {
			int writeLen = Math.min(buf.remaining(), MAX_MSG_LEN);
			byte[] ciphertext = writeState.encryptWithAssociatedData(null, buf.array(), buf.position(), writeLen);
			buf.position(buf.position() + writeLen);
			
			int obfLen = sip.write().obfuscate2(ciphertext.length);
			out.write(Util.serializeShort((short) obfLen), 0, 2);
			out.write(ciphertext);
		}
	}
	
	@Override
	public int read(byte[] data, int offset, int length) throws IOException, ProtocolViolationException {
		if(remainingReadData != null && remainingReadData.hasRemaining()) {
			int readLen = Math.min(length, remainingReadData.remaining());
			remainingReadData.get(data, offset, readLen);
			if(readLen == length) return readLen;
			
			// TODO API: (coverage) branch coverage
			offset += readLen;
			length -= readLen;
		}
		
		ByteBuffer lenBuf = ByteBuffer.allocate(2);
		IOUtils.readFully(in, lenBuf.array());
		int obfMsgLen = lenBuf.getShort();
		int msgLen = sip.read().obfuscate2(obfMsgLen);
		assertState(0 < msgLen && msgLen <= MAX_MSG_LEN + crypto.symBlockSize() + crypto.symTagLength());
		
		byte[] ciphertext = new byte[msgLen];
		IOUtils.readFully(in, ciphertext);
		
		byte[] plaintext = readState.decryptWithAssociatedData(new byte[0], ciphertext); 
		remainingReadData = ByteBuffer.wrap(plaintext);
		int readLen = Math.min(length, remainingReadData.remaining());
		remainingReadData.get(data, offset, readLen);
		return readLen;
	}
	
	@Override
	public boolean isLocalRoleClient() {
		return isLocalRoleClient;
	}

	@Override
	public void _close() throws IOException {
		if(socket != null) {
			socket.close();
		}

		if(connection != null) {
			connection.close();
		}
	}

	@Override
	public boolean isClosed() {
		return socket != null && socket.isClosed();
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
		return socket.getPort();
	}
	
	@Override
	public BandwidthMonitor getMonitorRx() {
		return in.getMonitor();
	}
	
	@Override
	public BandwidthMonitor getMonitorTx() {
		return out.getMonitor();
	}
	
	@Override
	public boolean matchesAddress(String address) {
		if(socket.getInetAddress().toString().equals(address)) return true;
		try {
			InetAddress addr = InetAddress.getByName(address);
			return socket.getInetAddress().equals(addr);
		} catch(IOException exc) {
			return false;
		}
	}
	
	protected void makeStreams() throws IOException {
		this.out = new RateLimitedOutputStream(socket.getOutputStream(),
				swarm.getBandwidthAllocatorTx(),
				swarm.getBandwidthMonitorTx());
		this.in = new RateLimitedInputStream(socket.getInputStream(),
				swarm.getBandwidthAllocatorRx(),
				swarm.getBandwidthMonitorRx());
	}

	protected void makeThreads() {
		if(disableMakeThreads) return;
		sendThread();
		recvThread();
	}

	protected void assertState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}
	
	protected byte[] readRaw(int expectedLen) throws IOException, ProtocolViolationException {
		int numRead = 0;
		byte[] incoming = new byte[expectedLen];
		while(numRead < expectedLen) {
			int r = in.read(incoming, numRead, incoming.length - numRead);
			if(r <= 0) {
				throw new SocketClosedException();
			}
			numRead += r;
		}
		return incoming;
	}
}
