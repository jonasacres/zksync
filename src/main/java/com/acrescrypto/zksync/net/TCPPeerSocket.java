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
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerSocket extends PeerSocket {
	public final static int MAX_MSG_LEN = 65536; // maximum ciphertext length; largest buffer a peer needs to hold in memory at once
	public final static int DEFAULT_MAX_HANDSHAKE_TIME_MILLIS = 60*1000; // 1 minute
	public static int maxHandshakeTimeMillis = DEFAULT_MAX_HANDSHAKE_TIME_MILLIS; // maximum time to handshake before automatic disconnect
	protected static boolean disableMakeThreads; // test purposes

	protected Socket socket;
	protected OutputStream out;
	protected InputStream in;
	protected boolean isLocalRoleClient;
	protected CryptoSupport crypto;
	protected Key localChainKey, remoteChainKey;
	protected PrivateDHKey dhPrivateKey;
	protected ByteBuffer remainingReadData;
	protected TCPPeerAdvertisement ad;
	protected int peerType = -1;
	protected byte[] identifier;
	
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
			TCPHandshakeContext handshake = new TCPHandshakeContext(swarm, socket, ad.pubKey);
			Key secret = handshake.establishSecret();
			this.identifier = handshake.identifier;
			initKeys(secret);
			makeThreads();
		} catch(ProtocolViolationException exc) {
			this.violation();
		}
	}
	
	protected TCPPeerSocket(PeerSwarm swarm) throws IOException {
		this.swarm = swarm;
		this.crypto = swarm.config.getAccessor().getMaster().getCrypto();
		this.dhPrivateKey = crypto.makePrivateDHKey();
	}
	
	protected TCPPeerSocket(TCPHandshakeContext handshake) throws IOException {
		this(handshake.swarm);
		this.socket = handshake.socket;
		this.address = socket.getInetAddress().getHostAddress();
		this.socket = handshake.socket;
		this.isLocalRoleClient = false;
		this.peerType = handshake.peerType;
		this.remoteIdentityKey = handshake.remoteStaticKey;
		this.identifier = handshake.identifier;
		
		if(handshake.remotePort != 0) {
			byte[] encryptedArchiveId = swarm.config.getEncryptedArchiveId(remoteIdentityKey.getBytes());
			this.ad = new TCPPeerAdvertisement(remoteIdentityKey, socket.getInetAddress().getHostAddress(), handshake.remotePort, encryptedArchiveId);
		}
		
		makeStreams();
		initKeys(handshake.currentKey);
		swarm.openedConnection(new PeerConnection(this));
		makeThreads();
	}
	
	@Override
	public synchronized void write(byte[] data, int offset, int length) throws IOException {
		ByteBuffer buf = ByteBuffer.wrap(data, offset, length);
		byte[] msgIv = new byte[crypto.symIvLength()];
		
		while(buf.hasRemaining()) {
			int writeLen = Math.min(buf.remaining(), MAX_MSG_LEN);
			byte[] ciphertext = nextLocalMessageKey().encrypt(msgIv, buf.array(), buf.position(), writeLen, 0);
			buf.position(buf.position() + writeLen);
			
			// TODO DHT: (rewrite) VERY BAD, NO, MUST NOT HAVE. Completely distinguishable.
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
	
	protected void initKeys(Key secret) {
		if(isLocalRoleClient) {
			localChainKey = makeChainRoot(secret, 0);
			remoteChainKey = makeChainRoot(secret, 1);
		} else {
			localChainKey = makeChainRoot(secret, 1);
			remoteChainKey = makeChainRoot(secret, 0);
		}
		
		secret.destroy();
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

	protected Key makeChainRoot(Key secret, int index) {
		return secret.derive(index, "zksync net chain root".getBytes());
	}

	protected Key nextLocalMessageKey() {
		Key newChainKey = localChainKey.derive(0, new byte[0]);
		Key msgKey = localChainKey.derive(1, new byte[0]);
		localChainKey.destroy();
		localChainKey = newChainKey;
		return msgKey;
	}
	
	protected Key nextRemoteMessageKey() {
		Key newChainKey = remoteChainKey.derive(0, new byte[0]);
		Key msgKey = remoteChainKey.derive(1, new byte[0]);
		remoteChainKey.destroy();
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
			assertState(r > 0);
			numRead += r;
		}
		return incoming;
	}
	
	@Override
	public byte[] getIdentifier() {
		return identifier;
	}
}
