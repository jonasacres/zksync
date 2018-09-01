package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;

/**
 * Produces a shared secret in a forward-extensible manner. There is purposefully no attempt to foresee/permit
 * cryptographic algorithm choices, or future choices, as this appears to be a repeated cause of security
 * issues.
 * 
 * All traffic is indistinguishable from random to an observer lacking the archive seed key.
 * 
 * All traffic other than the initial bootstrap message is indistinguishable from random to an
 * observer lacking either the client or server's ephemeral and static keypairs.
 * 
 * Tampering with any of the traffic, even with perfect knowledge of the keys, results in a failure
 * to arrive at a shared secret. This failure is detectable to one or both legitimate parties.
 * 
 * Handshake is 1-RTT.
 * 
 * Although this is in service of a p2p protocol where both parties have identical responsibilities,
 * and either may act as the initiator or responder, we use "client" to refer to the peer initiating the
 * handshake, and "server" to refer to the responding peer.
 *
 * Client:
 *   rng[32]
 *   bootstrap[fixed, key <- client_bootstrap_key, nonce=0]
 *     ct_len[2]
 *     pad_len[2]
 *     client_eph_pubkey
 *     zero[16]
 *   ciphertext[variable, key <- client_handshake_key, nonce=0]
 *     auth_record
 *       type[2]
 *       length[2]
 *       time_index[4]
 *       listen_tcp_port[2]
 *       client_static_pubkey
 *       key_proof
 *     future records...
 *   random padding
 *   
 * Server:
 *   bootstrap[fixed, key <- server_bootstrap_key, nonce=0]
 *     ct_len[2]
 *     pad_len[2]
 *     server_eph_pubkey
 *     zero[16]
 *   ciphertext[variable, key <- server_handshake_key, nonce=0]
 *     auth_record
 *       type[2]
 *       length[2]
 *       time_index[4]
 *       listen_tcp_port[2]
 *       server_static_pubkey
 *       key_proof
 *     future records...
 *   random padding
 *   secret_confirmation[32]
 * 
 * client_bootstrap_key: H(server_static || seed_key || archive_id || rng)
 * client_handshake_key: H(client_bootstrap_key || client_bootstrap_ciphertext || DH(client_eph, server_static) || client_eph_pubkey || server_static_pubkey)
 * server_bootstrap_key: H(client_handshake_key || client_handshake_ciphertext || client_padding || DH(static) || client_static_pubkey || server_static_pubkey)
 * server_handshake_key: H(server_bootstrap_key || server_bootstrap_ciphertext || DH(eph) || client_eph_pubkey || server_eph_pubkey)
 * 
 * secret: H(server_handshake_key || server_handshake_ciphertext || server_padding)
 * secret_confirmation: A(secret, client_key_proof || server_key_proof)
 */

public class TCPHandshakeContext {
	public final static int BOOTSTRAP_RESERVED_LEN = 16;
	public final static int MAX_HANDSHAKE_ELEMENT_SIZE = 16384;
	public final static int MAX_PADDING_LEN = 1024;
	public final static int TIMESLICE_EXPIRATION_GRACE_MS = 10*1000;
	
	public final static int HANDSHAKE_TIMEOUT_MS_DEFAULT = 10*10000;
	public static int handshakeTimeoutMs = HANDSHAKE_TIMEOUT_MS_DEFAULT;
	
	public final static short RECORD_TYPE_AUTH = 0;
	
	PeerSwarm swarm;
	PublicDHKey remoteStaticKey, remoteEphKey;
	PrivateDHKey localStaticKey, localEphKey;
	Key currentKey;
	CryptoSupport crypto;
	TCPPeerSocketListener listener;
	
	Socket socket;
	InputStream in;
	OutputStream out;
	SnoozeThread timeout;
	
	byte[] secret, clientProof, serverProof;
	boolean roleIsClient;
	int remotePort, peerType;
	
	public TCPHandshakeContext(PeerSwarm swarm, Socket socket, PublicDHKey serverStaticKey) throws IOException {
		// act as client
		this.roleIsClient = true;
		this.socket = socket;
		this.crypto = swarm.config.getCrypto();
		this.remoteStaticKey = serverStaticKey;
		this.localStaticKey = swarm.identityKey;
		this.swarm = swarm;
		initStreams();
	}
	
	public TCPHandshakeContext(TCPPeerSocketListener listener, Socket socket) throws IOException {
		// act as server
		this.listener = listener;
		this.socket = socket;
		this.crypto = listener.crypto;
		initStreams();
	}
	
	public Key establishSecret() throws IOException, ProtocolViolationException {
		timeout = new SnoozeThread(handshakeTimeoutMs, false, ()->{
			scrub();
			try {
				if(!socket.isClosed()) {
					socket.close();
				}
			} catch (IOException exc) {
				exc.printStackTrace();
			}
		});
		
		try {
			try {
				if(roleIsClient) {
					doClientHandshake();
				} else {
					doServerHandshake();
				}
			} catch(SecurityException exc) {
				throw new ProtocolViolationException();
			}
		} catch(IOException exc) {
			socket.close();
			throw exc;
		} catch(ProtocolViolationException exc) {
			Blacklist blacklist;
			if(swarm != null) {
				blacklist = swarm.config.getMaster().getBlacklist();
			} else {
				blacklist = listener.master.getBlacklist();
			}
			
			blacklist.add(socket.getInetAddress().getHostAddress(), Blacklist.DEFAULT_BLACKLIST_DURATION_MS);
			socket.close();
			throw exc;
		} finally {
			scrub();
			timeout.cancel();
		}
		
		return currentKey;
	}
	
	protected void scrub() {
		if(remoteEphKey != null) remoteEphKey.destroy();
		if(localEphKey != null) localEphKey.destroy();	
	}
	
	protected void doClientHandshake() throws IOException, ProtocolViolationException {
		sendClientRequest();
		receiveServerResponse();
	}
	
	protected void doServerHandshake() throws IOException, ProtocolViolationException {
		receiveClientRequest();
		sendServerResponse();
	}

	protected int bootstrapPlaintextLen() {
		return 2 + 2 + crypto.asymPublicDHKeySize() + BOOTSTRAP_RESERVED_LEN;
	}

	protected int authRecordPlaintextLen() {
		return 2 + 2 + 4 + 2 + crypto.asymPublicDHKeySize() + crypto.symKeyLength();
	}

	protected void sendClientRequest() throws IOException {
		this.localEphKey = crypto.makePrivateDHKey();
		byte[] clientRandom = crypto.rng(crypto.hashLength());
		int ctLen = crypto.symUnpaddedCiphertextSize(authRecordPlaintextLen()),
				padLen = crypto.defaultPrng().getInt(MAX_PADDING_LEN);
		updateKey(remoteStaticKey.getBytes(),
				swarm.config.getAccessor().getSeedRoot().getRaw(),
				swarm.config.getArchiveId(),
				clientRandom); // client_bootstrap_key
		byte[] clientBootstrapCiphertext = currentKey.encrypt(crypto.symNonce(0),
				assembleBootstrapPlaintext(ctLen, padLen),
				-1);
		
		byte[] halfEphSecret = localEphKey.sharedSecret(remoteStaticKey);
		updateKey(currentKey.getRaw(),
				clientBootstrapCiphertext,
				halfEphSecret,
				localEphKey.publicKey().getBytes(),
				remoteStaticKey.getBytes()); // client_handshake_key
		Util.blank(halfEphSecret);
		
		byte[] clientAuthCiphertext = currentKey.encrypt(crypto.symNonce(0),
				assembleAuthPlaintext(),
				-1);
		byte[] clientPadding = crypto.rng(padLen);
		
		byte[] staticSecret = localStaticKey.sharedSecret(remoteStaticKey);
		updateKey(currentKey.getRaw(),
				clientAuthCiphertext,
				clientPadding,
				staticSecret,
				localStaticKey.publicKey().getBytes(),
				remoteStaticKey.getBytes()); // server_bootstrap_key
		Util.blank(staticSecret);
		
		byte[] payload = Util.concat(clientRandom, clientBootstrapCiphertext, clientAuthCiphertext, clientPadding);
		out.write(payload);
	}
	
	protected void sendServerResponse() throws IOException {
		this.localEphKey = crypto.makePrivateDHKey();
		int ctLen = crypto.symUnpaddedCiphertextSize(authRecordPlaintextLen()),
				padLen = crypto.defaultPrng().getInt(MAX_PADDING_LEN);
		byte[] serverBootstrapCiphertext = currentKey.encrypt(crypto.symNonce(0),
				assembleBootstrapPlaintext(ctLen, padLen),
				-1);
		
		byte[] ephSecret = localEphKey.sharedSecret(remoteEphKey);
		updateKey(currentKey.getRaw(),
				serverBootstrapCiphertext,
				ephSecret,
				remoteEphKey.getBytes(),
				localEphKey.publicKey().getBytes()); // server_handshake_key
		Util.blank(ephSecret);
		
		byte[] serverAuthCiphertext = currentKey.encrypt(crypto.symNonce(0),
				assembleAuthPlaintext(),
				-1);
		byte[] serverPadding = crypto.rng(padLen);
		updateKey(currentKey.getRaw(),
				serverAuthCiphertext,
				serverPadding); // secret
		byte[] secretConfirmation = currentKey.authenticate(Util.concat(clientProof, serverProof));
		byte[] payload = Util.concat(serverBootstrapCiphertext, serverAuthCiphertext, serverPadding, secretConfirmation);
		out.write(payload);
	}

	protected void receiveClientRequest() throws IOException, ProtocolViolationException {
		byte[] clientEphKeyRaw = new byte[crypto.asymPublicDHKeySize()];
		byte[] clientStaticKeyRaw = new byte[crypto.asymPublicDHKeySize()];
		byte[] clientRandom = IOUtils.readFully(in, crypto.hashLength());
		byte[] clientBootstrapCiphertext = IOUtils.readFully(in, crypto.symUnpaddedCiphertextSize(bootstrapPlaintextLen()));
		clientProof = new byte[crypto.symKeyLength()];
		
		ByteBuffer clientBootstrapPlaintext = decipherBootstrap(clientBootstrapCiphertext, clientRandom);
		
		int ctLen = Util.unsignShort(clientBootstrapPlaintext.getShort());
		int padLen = Util.unsignShort(clientBootstrapPlaintext.getShort());
		assertState(ctLen <= MAX_HANDSHAKE_ELEMENT_SIZE && padLen <= MAX_HANDSHAKE_ELEMENT_SIZE);
		
		clientBootstrapPlaintext.get(clientEphKeyRaw);
		remoteEphKey = crypto.makePublicDHKey(clientEphKeyRaw);
		Util.blank(clientBootstrapPlaintext.array());
		
		byte[] halfEphSecret = localStaticKey.sharedSecret(remoteEphKey);
		updateKey(currentKey.getRaw(),
				clientBootstrapCiphertext,
				halfEphSecret,
				remoteEphKey.getBytes(),
				localStaticKey.publicKey().getBytes()
				); // client_handshake_key
		Util.blank(halfEphSecret);
		
		byte[] clientAuthCiphertext = IOUtils.readFully(in, ctLen);
		ByteBuffer clientAuthPlaintext = ByteBuffer.wrap(currentKey.decryptUnpadded(crypto.symNonce(0), clientAuthCiphertext));
		assertState(clientAuthPlaintext.remaining() >= authRecordPlaintextLen());
		assertState(clientAuthPlaintext.getShort() == 0); // first record MUST be auth
		int length = Util.unsignShort(clientAuthPlaintext.getShort());
		assertState(length <= clientAuthPlaintext.remaining());
		
		int timeslice = clientAuthPlaintext.getInt();
		assertValidTimeslice(timeslice);
		
		remotePort = Util.unsignShort(clientAuthPlaintext.getShort());

		clientAuthPlaintext.get(clientStaticKeyRaw);
		remoteStaticKey = crypto.makePublicDHKey(clientStaticKeyRaw);
		
		clientAuthPlaintext.get(clientProof);
		byte[] expectedProof = swarm.config.getAccessor().temporalProof(timeslice, 0, currentKey.getRaw());
		if(Util.safeEquals(clientProof, expectedProof)) {
			peerType = PeerConnection.PEER_TYPE_FULL;
		} else {
			peerType = PeerConnection.PEER_TYPE_BLIND;
		}
		
		Util.blank(clientAuthPlaintext.array());

		byte[] clientPadding = IOUtils.readFully(in, padLen);
		byte[] staticSecret = localStaticKey.sharedSecret(remoteStaticKey);
		updateKey(currentKey.getRaw(),
				clientAuthCiphertext,
				clientPadding,
				staticSecret,
				remoteStaticKey.getBytes(),
				localStaticKey.publicKey().getBytes()); // server_bootstrap_key
		Util.blank(staticSecret);
	}
	
	protected void receiveServerResponse() throws IOException, ProtocolViolationException {
		byte[] serverStaticKeyRaw = new byte[crypto.asymPublicDHKeySize()];
		serverProof = new byte[crypto.symKeyLength()];
		byte[] serverBootstrapCiphertext = new byte[crypto.symUnpaddedCiphertextSize(bootstrapPlaintextLen())];
		byte[] remoteEphKeyRaw = new byte[crypto.asymPublicDHKeySize()];
		IOUtils.readFully(in, serverBootstrapCiphertext);
		
		ByteBuffer serverBootstrapPlaintext = ByteBuffer.wrap(currentKey.decryptUnpadded(crypto.symNonce(0), serverBootstrapCiphertext));
		int ctLen = Util.unsignShort(serverBootstrapPlaintext.getShort());
		int padLen = Util.unsignShort(serverBootstrapPlaintext.getShort());
		serverBootstrapPlaintext.get(remoteEphKeyRaw);
		remoteEphKey = crypto.makePublicDHKey(remoteEphKeyRaw);
		Util.blank(serverBootstrapPlaintext.array());
		
		assertState(ctLen <= MAX_HANDSHAKE_ELEMENT_SIZE && padLen <= MAX_HANDSHAKE_ELEMENT_SIZE);
		
		byte[] ephSecret = localEphKey.sharedSecret(remoteEphKey);
		updateKey(currentKey.getRaw(),
				serverBootstrapCiphertext,
				ephSecret,
				localEphKey.publicKey().getBytes(),
				remoteEphKey.getBytes()); // server_handshake_key
		Util.blank(ephSecret);
		
		byte[] serverAuthCiphertext = IOUtils.readFully(in, ctLen);
		ByteBuffer serverAuthPlaintext = ByteBuffer.wrap(currentKey.decryptUnpadded(crypto.symNonce(0), serverAuthCiphertext));
		assertState(serverAuthPlaintext.remaining() >= authRecordPlaintextLen());
		assertState(serverAuthPlaintext.getShort() == 0); // first record MUST be auth
		int length = Util.unsignShort(serverAuthPlaintext.getShort());
		assertState(length <= serverAuthPlaintext.remaining());
		
		int timeslice = serverAuthPlaintext.getInt();
		assertValidTimeslice(timeslice);
		
		int port = Util.unsignShort(serverAuthPlaintext.getShort());
		assertState(port == socket.getPort());
		
		serverAuthPlaintext.get(serverStaticKeyRaw);
		assertState(Util.safeEquals(remoteStaticKey.getBytes(), serverStaticKeyRaw));
		
		serverAuthPlaintext.get(serverProof);
		byte[] expectedProof = swarm.config.getAccessor().temporalProof(timeslice, 1, currentKey.getRaw());
		if(Util.safeEquals(serverProof, expectedProof)) {
			peerType = PeerConnection.PEER_TYPE_FULL;
		} else {
			peerType = PeerConnection.PEER_TYPE_BLIND;
		}

		Util.blank(serverAuthPlaintext.array());
		byte[] serverPadding = IOUtils.readFully(in, padLen);
		updateKey(currentKey.getRaw(), serverAuthCiphertext, serverPadding); // secret
		
		byte[] expectedConfirmation = currentKey.authenticate(Util.concat(clientProof, serverProof));
		byte[] secretConfirmation = IOUtils.readFully(in, crypto.hashLength());
		assertState(Util.safeEquals(expectedConfirmation, secretConfirmation));
	}
	
	protected byte[] assembleBootstrapPlaintext(int ctLen, int padLen) {
		ByteBuffer buf = ByteBuffer.allocate(bootstrapPlaintextLen());
		buf.putShort((short) ctLen);
		buf.putShort((short) padLen);
		buf.put(localEphKey.publicKey().getBytes());
		// reserved section is just zeros
		return buf.array();
	}
	
	protected byte[] assembleAuthPlaintext() {
		int timeSlice = swarm.config.getAccessor().timeSliceIndex();
		byte[] proof = swarm.config.getAccessor().temporalProof(timeSlice,
				roleIsClient ? 0 : 1,
				currentKey.getRaw());
		
		if(roleIsClient) {
			clientProof = proof;
		} else {
			serverProof = proof;
		}
		
		ByteBuffer buf = ByteBuffer.allocate(authRecordPlaintextLen());
		buf.putShort((short) RECORD_TYPE_AUTH);
		buf.putShort((short) (buf.capacity()-4)); // don't count type or length fields to total length
		buf.putInt(timeSlice);
		buf.putShort((short) localTCPPort());
		buf.put(localStaticKey.publicKey().getBytes());
		buf.put(roleIsClient ? clientProof : serverProof);
		
		return buf.array();
	}
	
	protected ByteBuffer decipherBootstrap(byte[] clientBootstrapCiphertext, byte[] clientRandom) throws ProtocolViolationException {
		for(TCPPeerAdvertisementListener adListener : listener.adListeners) {
			byte[] material = Util.concat(adListener.swarm.identityKey.publicKey().getBytes(),
					adListener.swarm.config.getAccessor().getSeedRoot().getRaw(),
					adListener.swarm.config.getArchiveId(),
					clientRandom);
			Key candidateKey = new Key(crypto, crypto.makeSymmetricKey(material));
			try {
				byte[] plaintext = candidateKey.decryptUnpadded(crypto.symNonce(0), clientBootstrapCiphertext);
				this.localStaticKey = adListener.swarm.identityKey;
				this.swarm = adListener.swarm;
				currentKey = candidateKey;
				return ByteBuffer.wrap(plaintext);
			} catch(SecurityException exc) {
				// bad decrypt means we're trying the wrong ad
			}
		}
		
		throw new ProtocolViolationException();
	}
	
	protected void initStreams() throws IOException {
		in = socket.getInputStream();
		out = socket.getOutputStream();
	}
	
	protected Key updateKey(byte[]... elements) {
		byte[] material = Util.concat(elements);
		Key newKey = new Key(crypto, crypto.makeSymmetricKey(material));
		Util.blank(material);
		if(currentKey != null) {
			currentKey.destroy();
		}
		
		return currentKey = newKey;
	}
	
	protected int localTCPPort() {
		try {
			if(swarm.config.getMaster().getTCPListener().listenerForSwarm(swarm) == null) return 0;
			return swarm.config.getMaster().getTCPListener().getPort();
		} catch(NullPointerException exc) {
			return 0;
		}
	}
	
	protected void assertValidTimeslice(int timeslice) throws ProtocolViolationException {
		int diff = timeslice - swarm.config.getAccessor().timeSliceIndex();
		if(diff < 0) {
			// stated index is in past
			long expiration = swarm.config.getAccessor().timeSlice(timeslice) + ArchiveAccessor.TEMPORAL_SEED_KEY_INTERVAL_MS;
			long expiredFor = Math.abs(Util.currentTimeMillis() - expiration);
			assertState(0 <= expiredFor && expiredFor <= TIMESLICE_EXPIRATION_GRACE_MS);
		} else if(diff > 0) {
			// stated index is in future
			long startTime = swarm.config.getAccessor().timeSlice(timeslice);
			long startsIn = Math.abs(startTime - Util.currentTimeMillis());
			assertState(0 <= startsIn && startsIn <= TIMESLICE_EXPIRATION_GRACE_MS);
		}
	}
	
	protected void assertState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}
}
