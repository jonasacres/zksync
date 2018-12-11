package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.SocketClosedException;
import com.acrescrypto.zksync.net.noise.CipherState;
import com.acrescrypto.zksync.net.noise.HandshakeState;
import com.acrescrypto.zksync.utility.RateLimitedOutputStream;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.RateLimitedInputStream;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerSocket extends PeerSocket {
	public final static int MAX_MSG_LEN = 65536; // maximum ciphertext length; largest buffer a peer needs to hold in memory at once
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
	
	protected TCPPeerSocket() {}
	
	public TCPPeerSocket(PeerSwarm swarm, PublicDHKey remoteIdentityKey, Socket socket, CipherState[] states, byte[] handshakeHash, int peerType, int portNum) throws IOException {
		this(swarm);
		this.address = socket.getInetAddress().getHostAddress();
		this.socket = socket;
		this.isLocalRoleClient = false;
		this.readState = states[0];
		this.writeState = states[1];
		this.peerType = peerType;
		this.remoteIdentityKey = remoteIdentityKey;
		this.sharedSecret = handshakeHash;
		
		if(portNum != 0) {
			// TODO API: (coverage) branch coverage
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
		HandshakeState handshake = new HandshakeState(crypto,
				HANDSHAKE_PROTOCOL,
				HANDSHAKE_PATTERN,
				true,
				new byte[0],
				swarm.getConfig().getMaster().getTCPListener().getIdentityKey(),
				null,
				ad.getPubKey(),
				null,
				swarm.getConfig().getArchiveId());
		
		handshake.setObfuscation(
			(key)->{
				return key.getBytes(); // TODO: obfuscate with CBC
			},
			
			(in)->{
				byte[] keyRaw = IOUtils.readFully(in, crypto.asymPublicDHKeySize());
				return new byte[][] { keyRaw, keyRaw };
			}
		);
		
		handshake.setPayload(
			(round)->{
				if(round != 3) return null;
				byte[] id = crypto.hash(Util.concat(handshake.getHash(), swarm.config.getArchiveId()));
				byte[] proof = swarm.getConfig().getAccessor().temporalProof(0, 0, handshake.getHash());
				byte[] all = Util.concat(id, proof, Util.serializeShort((short) swarm.config.getMaster().getTCPListener().port));
				return all;
			},
			
			(round, in, decrypter)->{
				if(round != 4) return;
				byte[] hsHash = handshake.getHash();
				byte[] proofCt = IOUtils.readFully(in, crypto.symKeyLength() + crypto.symTagLength());
				byte[] proof = decrypter.decrypt(proofCt);
				byte[] expectedProof = swarm.config.getAccessor().temporalProof(0, 1, hsHash);
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
		
		// TODO Noise: Plaintext lengths must DIE!!!
		while(buf.hasRemaining()) {
			int writeLen = Math.min(buf.remaining(), MAX_MSG_LEN);
			byte[] ciphertext = writeState.encryptWithAssociatedData(null, buf.array(), buf.position(), writeLen);
			buf.position(buf.position() + writeLen);
			out.write(Util.serializeInt(ciphertext.length), 0, 4);
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
		
		ByteBuffer lenBuf = ByteBuffer.allocate(4);
		IOUtils.readFully(in, lenBuf.array());
		int msgLen = lenBuf.getInt();
		assertState(0 < msgLen && msgLen <= MAX_MSG_LEN + 2*crypto.symBlockSize()); // add some grace in for padding + built-in overhead in ciphertext
		
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
