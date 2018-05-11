package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.whispersystems.curve25519.Curve25519;
import org.whispersystems.curve25519.Curve25519KeyPair;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;

public class TCPPeerSocket extends PeerSocket {
	public final static int MAX_MSG_LEN = 65536; // maximum ciphertext length; largest buffer a peer needs to hold in memory at once

	protected Socket socket;
	protected OutputStream out;
	protected InputStream in;
	protected boolean isClient;
	protected CryptoSupport crypto;
	protected Key localChainKey, remoteChainKey;
	protected Curve25519 curve25519;
	protected Curve25519KeyPair keyPair;
	protected byte[] sharedSecret;
	protected ByteBuffer remainingReadData;
	protected TCPPeerAdvertisement ad;
	
	public TCPPeerSocket(PeerSwarm swarm, Socket socket, byte[] sharedSecret, int peerType) throws IOException {
		this(swarm);
		this.socket = socket;
		this.isClient = false;
		this.sharedSecret = sharedSecret;
		makeStreams();
		initKeys();
		swarm.openedConnection(new PeerConnection(this));
	}
	
	public TCPPeerSocket(PeerSwarm swarm, TCPPeerAdvertisement ad) throws IOException, ProtocolViolationException, BlacklistedException {
		this(swarm);
		this.swarm = swarm;
		this.crypto = swarm.config.getAccessor().getMaster().getCrypto();
		this.curve25519 = Curve25519.getInstance(Curve25519.BEST);
		this.keyPair = curve25519.generateKeyPair();
		this.isClient = true;
		if(ad.isBlacklisted(swarm.config.getAccessor().getMaster().getBlacklist())) throw new BlacklistedException(ad.host);
		this.socket = new Socket(ad.host, ad.port);
		makeStreams();
		try {
			sendHandshake(ad.pubKey);
		} catch(ProtocolViolationException exc) {
			this.violation();
			throw exc;
		}
	}
	
	protected TCPPeerSocket(PeerSwarm swarm) throws IOException {
		this.swarm = swarm;
		this.crypto = swarm.config.getAccessor().getMaster().getCrypto();
		this.curve25519 = Curve25519.getInstance(Curve25519.BEST);
		this.keyPair = curve25519.generateKeyPair();
	}
	
	protected void makeStreams() throws IOException {
		this.out = socket.getOutputStream();
		this.in = socket.getInputStream();
	}
	
	protected void initKeys() {
		if(isClient) {
			localChainKey = makeChainRoot(0);
			remoteChainKey = makeChainRoot(1);
		} else {
			localChainKey = makeChainRoot(1);
			remoteChainKey = makeChainRoot(0);
		}
	}
	
	protected Key makeChainRoot(int index) {
		return new Key(crypto, crypto.expand(sharedSecret, crypto.symKeyLength(), new byte[] { (byte) index }, "zksync".getBytes()));
	}
	
	@Override
	public synchronized void write(byte[] data, int offset, int length) throws IOException, ProtocolViolationException {
		ByteBuffer buf = ByteBuffer.wrap(data, offset, length);
		byte[] msgIv = new byte[crypto.symIvLength()];
		
		while(buf.hasRemaining()) {
			int writeLen = Math.min(buf.remaining(), MAX_MSG_LEN);
			byte[] ciphertext = nextLocalMessageKey().encrypt(msgIv, buf.array(), buf.position(), writeLen, 0);
			buf.position(buf.position() + writeLen);
			out.write(ByteBuffer.allocate(4).putInt(ciphertext.length).array(), 0, 4);
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
		in.read(lenBuf.array(), 0, lenBuf.remaining());
		int msgLen = lenBuf.getInt();
		assertState(0 < msgLen && msgLen <= MAX_MSG_LEN + crypto.symBlockSize());
		
		byte[] ciphertext = new byte[msgLen];
		int numRead = 0;
		while(numRead < msgLen) {
			int r = in.read(ciphertext, numRead, ciphertext.length - numRead);
			assertState(r > 0);
			numRead += r;
		}
		
		remainingReadData = ByteBuffer.wrap(nextRemoteMessageKey().decrypt(new byte[crypto.symIvLength()], ciphertext));
		int readLen = Math.min(length, remainingReadData.remaining());
		remainingReadData.get(data, offset, readLen);
		return readLen;
	}
	
	@Override
	public boolean isClient() {
		return isClient;
	}

	@Override
	public void close() throws IOException {
		socket.close();
	}

	@Override
	public boolean isClosed() {
		return socket.isClosed();
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
	public boolean matchesAddress(String address) {
		if(socket.getInetAddress().toString().equals(address)) return true;
		try {
			InetAddress addr = InetAddress.getByName(address);
			return socket.getInetAddress().equals(addr);
		} catch(IOException exc) {
			return false;
		}
	}
	
	protected void sendHandshake(byte[] remotePubKey) throws IOException, ProtocolViolationException {
		byte[] tempSharedSecret = curve25519.calculateAgreement(remotePubKey, keyPair.getPrivateKey());
		byte[] proof = swarm.config.getAccessor().temporalProof(0, tempSharedSecret);
		byte[] keyHash = crypto.authenticate(remotePubKey, keyPair.getPublicKey());
		
		out.write(keyPair.getPublicKey());
		out.write(keyHash);
		out.write(proof);
		
		byte[] remoteEphemeralPubKey = readRaw(remotePubKey.length);
		this.sharedSecret = curve25519.calculateAgreement(remoteEphemeralPubKey, keyPair.getPrivateKey());
		byte[] remoteAuth = readRaw(crypto.hashLength());
		byte[] expectedAuth = crypto.authenticate(this.sharedSecret, tempSharedSecret);
		assertState(Arrays.equals(remoteAuth, expectedAuth));
		
		initKeys();
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
			assertState(r > 0);
			numRead += r;
		}
		return incoming;
	}
}
