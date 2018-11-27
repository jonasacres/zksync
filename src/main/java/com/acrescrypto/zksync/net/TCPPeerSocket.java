package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.SocketClosedException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerSocket extends PeerSocket {
	public final static int MAX_MSG_LEN = 65536; // maximum ciphertext length; largest buffer a peer needs to hold in memory at once
	public final static int DEFAULT_MAX_HANDSHAKE_TIME_MILLIS = 60*1000; // 1 minute
	public final static int DEFAULT_SOCKET_CLOSE_DELAY = 5*1000; // 5 seconds
	public static int maxHandshakeTimeMillis = DEFAULT_MAX_HANDSHAKE_TIME_MILLIS; // maximum time to handshake before automatic disconnect
	public static int socketCloseDelay = DEFAULT_SOCKET_CLOSE_DELAY;
	protected static boolean disableMakeThreads; // test purposes

	protected Socket socket;
	protected OutputStream out;
	protected InputStream in;
	protected boolean isLocalRoleClient;
	protected CryptoSupport crypto;
	protected Key localChainKey, remoteChainKey;
	protected PrivateDHKey dhPrivateKey;
	protected byte[] sharedSecret;
	protected ByteBuffer remainingReadData;
	protected TCPPeerAdvertisement ad;
	protected int peerType = -1;
	
	public TCPPeerSocket(PeerSwarm swarm, PublicDHKey remoteIdentityKey, Socket socket, byte[] sharedSecret, int peerType, int portNum) throws IOException {
		this(swarm);
		this.address = socket.getInetAddress().getHostAddress();
		this.socket = socket;
		this.isLocalRoleClient = false;
		this.sharedSecret = sharedSecret;
		this.peerType = peerType;
		this.remoteIdentityKey = remoteIdentityKey;
		
		if(portNum != 0) {
			// TODO API: (coverage) branch coverage
			byte[] encryptedArchiveId = swarm.config.getEncryptedArchiveId(remoteIdentityKey.getBytes());
			this.ad = new TCPPeerAdvertisement(remoteIdentityKey, socket.getInetAddress().getHostAddress(), portNum, encryptedArchiveId);
		}
		
		makeStreams();
		initKeys();
		swarm.openedConnection(new PeerConnection(this));
		makeThreads();
	}
	
	public TCPPeerSocket(PeerSwarm swarm, TCPPeerAdvertisement ad) throws IOException, BlacklistedException {
		this(swarm);
		this.ad = ad;
		this.swarm = swarm;
		this.crypto = swarm.config.getAccessor().getMaster().getCrypto();
		this.dhPrivateKey = crypto.makePrivateDHKey();
		this.isLocalRoleClient = true;
		this.remoteIdentityKey = ad.pubKey;
		if(ad.isBlacklisted(swarm.config.getAccessor().getMaster().getBlacklist())) throw new BlacklistedException(ad.host);
	}
	
	public void handshake(PeerConnection connection) throws ProtocolViolationException, IOException {
		this.connection = connection;
		this.socket = new Socket(ad.host, ad.port);
		this.address = socket.getInetAddress().getHostAddress();
		makeStreams();
		try {
			sendHandshake(ad.pubKey);
		} catch(ProtocolViolationException exc) {
			this.violation();
			throw exc;
		}
	}
	
	protected TCPPeerSocket(PeerSwarm swarm) throws IOException {
		super(swarm);
		this.swarm = swarm;
		this.crypto = swarm.config.getAccessor().getMaster().getCrypto();
		this.dhPrivateKey = crypto.makePrivateDHKey();
	}
	
	@Override
	public synchronized void write(byte[] data, int offset, int length) throws IOException {
		ByteBuffer buf = ByteBuffer.wrap(data, offset, length);
		byte[] msgIv = new byte[crypto.symIvLength()];
		
		while(buf.hasRemaining()) {
			int writeLen = Math.min(buf.remaining(), MAX_MSG_LEN);
			byte[] ciphertext = nextLocalMessageKey().encrypt(msgIv, buf.array(), buf.position(), writeLen, 0);
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
		
		remainingReadData = ByteBuffer.wrap(nextRemoteMessageKey().decrypt(new byte[crypto.symIvLength()], ciphertext));
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
	public boolean matchesAddress(String address) {
		if(socket.getInetAddress().toString().equals(address)) return true;
		try {
			InetAddress addr = InetAddress.getByName(address);
			return socket.getInetAddress().equals(addr);
		} catch(IOException exc) {
			return false;
		}
	}
	
	protected void initKeys() {
		if(isLocalRoleClient) {
			localChainKey = makeChainRoot(0);
			remoteChainKey = makeChainRoot(1);
		} else {
			localChainKey = makeChainRoot(1);
			remoteChainKey = makeChainRoot(0);
		}
	}

	protected void makeStreams() throws IOException {
		this.out = socket.getOutputStream();
		this.in = socket.getInputStream();
	}

	protected void makeThreads() {
		if(disableMakeThreads) return;
		sendThread();
		recvThread();
	}

	protected Key makeChainRoot(int index) {
		return new Key(crypto, crypto.expand(sharedSecret, crypto.symKeyLength(), new byte[] { (byte) index }, "zksync".getBytes()));
	}

	protected void sendHandshake(PublicDHKey remotePubKey) throws IOException, ProtocolViolationException {
		Util.ensure(TCPPeerSocket.maxHandshakeTimeMillis, 10, ()->peerType >= 0, ()->close());
		ByteBuffer keyHashInput = ByteBuffer.allocate(2*crypto.asymPublicDHKeySize()+crypto.hashLength()+4);
		keyHashInput.put(dhPrivateKey.publicKey().getBytes());
		keyHashInput.put(remotePubKey.getBytes());
		keyHashInput.put(swarm.config.getArchiveId());
		keyHashInput.putInt(0);
		Key keyHashKey = swarm.config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_SEED);
		
		byte[] tempSharedSecret = dhPrivateKey.sharedSecret(remotePubKey);
		byte[] staticSecret = swarm.identityKey.sharedSecret(remotePubKey);
		Key staticSymKeyText = new Key(crypto, tempSharedSecret).derive(0, new byte[0]);
		
		int timeIndex = swarm.config.getAccessor().timeSliceIndex();
		int port = 0;
		try {
			port = swarm.config.getMaster().getTCPListener().getPort();
		} catch(NullPointerException exc) {}
		byte[] keyKnowledgeProof = swarm.config.getAccessor().temporalProof(timeIndex, 0, tempSharedSecret);
		byte[] keyHash = keyHashKey.authenticate(keyHashInput.array());
		byte[] staticKeyCiphertext = staticSymKeyText.encrypt(crypto.symNonce(0), swarm.identityKey.publicKey().getBytes(), -1);
		byte[] encryptedPortNum = staticSymKeyText.encrypt(crypto.symNonce(1), Util.serializeShort((short) port), -1);
		
		byte[] timeProof = crypto.authenticate(tempSharedSecret, Util.serializeInt(timeIndex));
		
		// TODO Handshake: (refactor) Conceal client eph public key in handshake?
		out.write(dhPrivateKey.publicKey().getBytes());
		out.write(keyHash);
		out.write(timeProof);
		out.write(keyKnowledgeProof);
		out.write(staticKeyCiphertext);
		out.write(encryptedPortNum);
		
		// TODO Handshake: (refactor) Conceal server eph public key in handshake?
		PublicDHKey remoteEphemeralPubKey = crypto.makePublicDHKey(readRaw(crypto.asymPublicDHKeySize()));
		byte[] ephemeralSecret = dhPrivateKey.sharedSecret(remoteEphemeralPubKey);
		
		byte[] remoteKeyKnowledgeProof = readRaw(crypto.symKeyLength());
		byte[] handshakeProof = readRaw(crypto.hashLength());
		
		byte[] expectedRemoteProof = swarm.config.getAccessor().temporalProof(timeIndex, 1, tempSharedSecret);
		if(Util.safeEquals(expectedRemoteProof, remoteKeyKnowledgeProof)) {
			peerType = PeerConnection.PEER_TYPE_FULL;
		} else {
			peerType = PeerConnection.PEER_TYPE_BLIND;
		}

		byte[] sharedSecretText = Util.concat(dhPrivateKey.publicKey().getBytes(),
				keyHash,
				timeProof,
				keyKnowledgeProof,
				staticKeyCiphertext,
				remoteEphemeralPubKey.getBytes(),
				remoteKeyKnowledgeProof,
				swarm.config.getAccessor().temporalSeedId(timeIndex));
		byte[] sharedSecretRoot = crypto.authenticate(Util.concat(staticSecret, ephemeralSecret),
				sharedSecretText);
		sharedSecret = crypto.authenticate(sharedSecretRoot, Util.serializeInt(0));
		byte[] handshakeProofKey = crypto.authenticate(sharedSecretRoot, Util.serializeInt(1));
		byte[] expectedHandshakeProof = crypto.authenticate(handshakeProofKey, swarm.config.getAccessor().temporalSeedId(timeIndex));

		assertState(Util.safeEquals(handshakeProof, expectedHandshakeProof));

		initKeys();
		makeThreads();
		
		if(connection == null) {
			this.connection = new PeerConnection(this);
		}
	}
	
	protected Key nextLocalMessageKey() {
		Key newChainKey = localChainKey.derive(0, new byte[0]);
		Key msgKey = localChainKey.derive(1, new byte[0]);
		localChainKey = newChainKey;
		return msgKey;
	}
	
	protected Key nextRemoteMessageKey() {
		Key newChainKey = remoteChainKey.derive(0, new byte[0]);
		Key msgKey = remoteChainKey.derive(1, new byte[0]);
		remoteChainKey = newChainKey;
		return msgKey;
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
