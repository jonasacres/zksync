package com.acrescrypto.zksync.net.dht;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.utility.Util;

public class DHTMessage {
	public final static byte CMD_PING          = 0;
	public final static byte CMD_FIND_NODE     = 1;
	public final static byte CMD_ADD_RECORD    = 2;
	
	public final static byte FLAG_RESPONSE     = 0x01;
	
	public interface DHTMessageCallback {
		void responseReceived(DHTMessage response) throws ProtocolViolationException;
	}
	
	protected DHTMessageCallback                        callback;
	protected DHTPeer                                   peer;
	protected int                                       msgId;
	protected long                                      timestamp;
	protected byte                                      cmd,
	                                                    flags,
	                                                    numExpected;
	protected byte[]                                    payload,
	                                                    authTag;
	protected boolean                                   isFinal;
	protected ArrayList<Collection<? extends Sendable>> itemLists;
	
	private Logger logger = LoggerFactory.getLogger(DHTMessage.class);
	
	public DHTMessage(DHTPeer recipient, byte cmd, byte[] payload, DHTMessageCallback callback) {
		this(recipient, cmd, ByteBuffer.wrap(payload), callback);
	}
	
	public DHTMessage(DHTPeer recipient, byte cmd, ByteBuffer payloadBuf, DHTMessageCallback callback) {
		this.peer      = recipient;
		this.cmd       = cmd;
		this.flags     = 0;
		this.authTag   = recipient.remoteAuthTag;
		this.callback  = callback;
		this.msgId     = recipient.client.crypto.defaultPrng().getInt();
		this.timestamp = Util.currentTimeMillis();
		this.payload   = new byte[payloadBuf.remaining()];

		payloadBuf.get(payload);
	}
	
	public DHTMessage(DHTClient client, String senderAddress, int senderPort, ByteBuffer serialized) throws ProtocolViolationException {
		deserialize(client, senderAddress, senderPort, serialized);
		if(hasValidAuthTag()) {
			peer.markVerified();
		}
	}
	
	public DHTMessage(DHTPeer recipient, byte cmd, int msgId, Collection<? extends Sendable> items) {
		this.peer      = recipient;
		this.cmd       = cmd;
		this.flags     = FLAG_RESPONSE;
		this.authTag   = recipient.localAuthTag();
		this.itemLists = new ArrayList<>();
		this.msgId     = msgId;
		this.timestamp = Util.currentTimeMillis();

		if(items != null) this.itemLists.add(new ArrayList<>(items));
	}
	
	public boolean isResponse() {
		return (flags & FLAG_RESPONSE) != 0;
	}
	
	public DHTMessage makeResponse(Collection<? extends Sendable> items) {
		return new DHTMessage(peer, cmd, msgId, items);
	}
	
	public DHTMessage addItemList(Collection<? extends Sendable> items) {
		this.itemLists.add(new ArrayList<>(items));
		return this;
	}
	
	public void send() {
		if(isResponse()) {
			sendItems();
		} else {
			DatagramPacket packet = prepareRequestDatagram();
			peer.client.getProtocolManager().watchForResponse(this);
			sendDatagram(packet);
		}
	}
	
	protected DatagramPacket prepareRequestDatagram() {
		return prepareDatagram(1, ByteBuffer.wrap(payload));
	}
	
	protected DatagramPacket sendItems() {
		int numPackets = 1;
		ByteBuffer sendBuf = ByteBuffer.allocate(maxPayloadSize());
		
		if(itemLists != null && !itemLists.isEmpty()) {
			numPackets = numPacketsNeeded();
			
			for(int i = 0; i < itemLists.size(); i++) {
				for(Sendable item : itemLists.get(i)) {
					byte[] serialized = item.serialize();
					int itemLen = 1 + 2 + serialized.length;
					if(sendBuf.capacity() < itemLen) continue;
					if(sendBuf.remaining() < itemLen) {
						sendBuf.limit(sendBuf.position());
						sendBuf.position(0);
						sendDatagram(prepareDatagram(numPackets, sendBuf));
						sendBuf.clear();
					}
					
					sendBuf.put((byte) i);
					sendBuf.putShort((short) serialized.length);
					sendBuf.put(serialized);
				}
			}
		}
		
		sendBuf.limit(sendBuf.position());
		sendBuf.position(0);
		return sendDatagram(prepareDatagram(numPackets, sendBuf));
	}
	
	protected int numPacketsNeeded() {
		int numNeeded = 0, bytesRemaining = 0;
		for(Collection<? extends Sendable> list : itemLists) {
			for(Sendable item : list) {
				int len = 1 + 2 + item.serialize().length;
				if(len > bytesRemaining) {
					numNeeded++;
					bytesRemaining = maxPayloadSize();
				}
				bytesRemaining -= len;
			}
		}
		
		return numNeeded;
	}
	
	protected DatagramPacket prepareDatagram(int numPackets, ByteBuffer sendBuf) {
		sendBuf.position(0);
		InetAddress address;
		try {
			address = InetAddress.getByName(peer.address);
		} catch (UnknownHostException exc) {
			return null;
		}
		byte[] serialized = serialize(numPackets, sendBuf);
		return new DatagramPacket(
				serialized,
				serialized.length,
				address,
				peer.port);
	}
	
	protected DatagramPacket sendDatagram(DatagramPacket packet) {
		logger.trace(
				"DHT {}:{}: sending {} bytes, cmd={}, flags=0x{}, msgId={}",
				packet.getAddress().getHostAddress(),
				packet.getPort(),
				packet.getData().length,
				cmd,
				String.format("%02x", flags),
				msgId);
		peer.client.getSocketManager().sendDatagram(packet);
		return packet;
	}
	
	protected byte[] serialize(int numPackets, ByteBuffer sendBuf) {
		/*  ('||' operator indicates concatenation throughout this comment)
		 * serialized = rnd
		 *           || encrypt_u(k0, eph_pubkey)                    // ct0
		 *           || encrypt_u(k1, local_static_pubkey)           // ct1
		 *           || encrypt_a(k2, header || payload || padding)
		 *    rnd: 8 rng bytes
		 *     k0: HKDF(peer_static_pubkey,       rnd ||               networkId)
		 *     k1: HKDF(peer_static_pubkey, k0 || rnd || ct0 ||        DH(eph_privkey, peer_static_pubkey))
		 *     k2: HKDF(peer_static_pubkey, k1 || rnd || ct0 || ct1 || DH(local_static_privkey, peer_static_pubkey))
		 * padding: null bytes added to end of plaintext, of arbitrary length less than maximum allowed
		 * 
		 * encrypt_u (key,  plaintext): encrypt without authentication, fixed IV
		 * encrypt_a (key,  plaintext): encrypt with    authentication, fixed IV
		 * HKDF      (salt, ikm)      : RFC 5869, length is symmetric key size, info field is 0-byte string
		 * DH        (pubkey, privkey): Diffie-Hellman shared secret derivation
		 * 
		 * objectives:
		 *   - secret info required to learn local_static_pubkey, header and/or payload
		 *       (specifically: (local_static_privkey and eph_privkey) or remote_static_privkey) 
		 *   - non-trivial to distinguish serialized bytes from random
		 *       (observer must know networkId || peer_static_pubkey and attempt symmetric crypto operation,
		 *        which significant complicates the task of distinguishing from random)
         *   - to recipient possessing peer_static_privkey, proves message was written by someone
         *     possessing local_static_privkey
         * 
         * Notably, this design lacks other desirable features, such as:
         *   - cryptographically-enforced replay protection
         *   - perfect forward secrecy
         * 
         * Adding an extra round trip per message, and/or maintaining connection state on
         * a per-DHT peer basis would allow the remote peer to provide its own ephemeral
         * public key, at the considerable expense of adding 1RTT and/or significantly
         * complicating the state information that must be maintained for each peer.
         * This offers the benefit of perfect forward secrecy and replay protection.
         * 
         * Replay protection can also be attained by other means, such as a set of
         * recently-seen 64-bit salts, combined with a check against a timestamp field
         * in the header.
         * 
         * The cost of failing to ensure forward secrecy is mitigated by the fact that DHT
         * peers possess very limited information to begin with. An adversary may be able to
         * collect ciphertexts sent to a peer, then compromise that peer or force disclosure
         * of the private static key. This will reveal DHTIDs that have been looked up via
         * the compromised peer, as well as records stored on it. These records in turn
         * reveal peers listening for peers for a given DHTID. However, this information
         * cannot be meaningfully acted upon without additional information, such as
         * knowledge of the secret generating the DHTID, or its associated seed key, or some
         * confirmation that a DHTID belongs to some item of interest.
         * 
         * Since DHTIDs rotate every several hours, and are unpredictable without knowledge
         * of the seed secret that generates them, a compromise of a DHT peer is of limited
         * (though not necessarily zero) utility.
		 */
		
		CryptoSupport crypto             = peer.client.crypto;
		
		PrivateDHKey  ephPrivkey         = crypto.makePrivateDHKey(),
			          localStaticPrivkey = peer.client.getPrivateKey();
		
		PublicDHKey   localStaticPubkey  = peer.client.getPublicKey(),
				      remoteStaticPubkey = peer.getKey(),
				      ephPubkey          = ephPrivkey.publicKey();
		
		byte[]        rnd                = crypto.rng(8),
		              blankIv            = new byte[crypto.symIvLength()],
		              networkId          = peer.client.getNetworkId(),
		              ephSharedSecret    = ephPrivkey        .sharedSecret(peer.getKey()),
		              staticSharedSecret = localStaticPrivkey.sharedSecret(peer.getKey());
		
		Key[]         keys               = new Key[3];
		              keys[0]            = new Key(crypto, crypto.expandAndDestroy(
											Util.concat(
													rnd,                 // 1
													networkId),
											crypto.symKeyLength(),
											remoteStaticPubkey.getBytes(),
											new byte[0]));
		byte[]        obfuscatedEphKey   = keys[0].encryptUnauthenticated(blankIv, ephPubkey.getBytes());
		              keys[1]            = new Key(crypto, crypto.expandAndDestroy(
				                            Util.concat(
				                            		keys[0].getRaw(),
				                            		rnd,                 // 1
				                            		obfuscatedEphKey,    // 2
				                            		ephSharedSecret),
				                            crypto.symKeyLength(),
				                            remoteStaticPubkey.getBytes(),
				                            new byte[0]));
		byte[]        encryptedStaticKey = keys[1].encryptUnauthenticated(blankIv, localStaticPubkey.getBytes());
		              keys[2]            = new Key(crypto, crypto.expandAndDestroy(
                                            Util.concat(
                                            		keys[1].getRaw(),
                                            		rnd,                 // 1
                                            		obfuscatedEphKey,    // 2
                                            		encryptedStaticKey,  // 3
                                            		staticSharedSecret),
                                            crypto.symKeyLength(),
                                            remoteStaticPubkey.getBytes(),
                                            new byte[0]));
		
		ByteBuffer plaintext             = ByteBuffer.allocate(
				                             headerSize()
				                           + sendBuf.remaining());
		plaintext.put    (authTag);
		plaintext.putInt (msgId);
		plaintext.putLong(timestamp);
		plaintext.put    (cmd);
		plaintext.put    (flags);
		plaintext.put    ((byte) numPackets);
		plaintext.put    (sendBuf);
		
		// pad the plaintext to obfuscate our true length somewhat
		int    baseLength         = 8
				                  + obfuscatedEphKey.length
                                  + encryptedStaticKey.length
                                  + crypto.symPaddedCiphertextSize(plaintext.position()),
		       maxLength          = peer.client.getMaster().getGlobalConfig().getInt("net.dht.maxDatagramSize"),
		       padLength          = crypto.defaultPrng().getInt(maxLength - baseLength),
		       paddedSize         = plaintext.position()
		                          + padLength;
		byte[] ciphertext         = keys[2].encrypt(
				                      blankIv,
				                      plaintext.array(),
				                      paddedSize),
		       serialized         = Util.concat(
		    		                  rnd,
		    		                  obfuscatedEphKey,
		    		                  encryptedStaticKey,
		    		                  ciphertext);
		
		// This is of dubious effectiveness in Java, but even attempting key hygiene is virtuous.
		Util.zero(ephSharedSecret);
		Util.zero(staticSharedSecret);
		ephPrivkey.destroy();
		for(Key k : keys) k.destroy();
		
		return serialized;
	}
	
	protected void deserialize(DHTClient client, String senderAddress, int senderPort, ByteBuffer serialized) throws ProtocolViolationException {
		try {
			deserializeActual(client, senderAddress, senderPort, serialized);
		} catch(Exception exc) {
			throw new BenignProtocolViolationException();
		}
	}
	
	protected void deserializeActual(DHTClient client, String senderAddress, int senderPort, ByteBuffer serialized) throws ProtocolViolationException {
		CryptoSupport crypto             = client.crypto;
		assertStateWithoutBlacklist(
				  serialized.remaining()
			    >= 8
				 + 2 * crypto.asymPublicDHKeySize()
				 + crypto.symPaddedCiphertextSize(headerSize()));
		
		PrivateDHKey  localStaticPrivkey = client.getPrivateKey();
		PublicDHKey   localStaticPubkey  = client.getPublicKey();
		
		byte[]        networkId          = client.getNetworkId(),
				      rnd                = new byte[8],
				      blankIv            = new byte[crypto.symIvLength()],
				      obfuscatedEphKey   = new byte[crypto.asymPublicDHKeySize()],
				      encryptedStaticKey = new byte[crypto.asymPublicDHKeySize()];
		
		serialized.get(rnd);
		serialized.get(obfuscatedEphKey);
		serialized.get(encryptedStaticKey);
		
		/* Ensure that this salt (rnd) hasn't been seen recently, and if not, ensure it is
		 * recorded so we don't let it get reused. Combined with timestamp validation, this
		 * guards against replay attacks. */
		assertStateWithoutBlacklist(client.getProtocolManager().recordMessageRnd(rnd));
		
		Key[]         keys               = new Key[3];
		              keys[0]            = new Key(crypto, crypto.expandAndDestroy(
											Util.concat(
													rnd,                 // 1
													networkId),
											crypto.symKeyLength(),
											localStaticPubkey.getBytes(),
											new byte[0]));
		byte[]        ephPubkeyRaw       = keys[0].decryptUnauthenticated(blankIv, obfuscatedEphKey);
		PublicDHKey   ephPubkey          = crypto.makePublicDHKey(ephPubkeyRaw);
		byte[]        ephSharedSecret    = localStaticPrivkey.sharedSecret(ephPubkey);
		
		              keys[1]            = new Key(crypto, crypto.expandAndDestroy(
				                            Util.concat(
				                            		keys[0].getRaw(),
				                            		rnd,                 // 1
				                            		obfuscatedEphKey,    // 2
				                            		ephSharedSecret),
				                            crypto.symKeyLength(),
				                            localStaticPubkey.getBytes(),
				                            new byte[0]));
		byte[]        staticPubkeyRaw    = keys[1].decryptUnauthenticated(blankIv, encryptedStaticKey);
		PublicDHKey   remoteStaticPubkey = crypto.makePublicDHKey(staticPubkeyRaw);
		byte[]        staticSharedSecret = localStaticPrivkey.sharedSecret(remoteStaticPubkey);
		
		              keys[2]            = new Key(crypto, crypto.expandAndDestroy(
                                            Util.concat(
                                            		keys[1].getRaw(),
                                            		rnd,                 // 1
                                            		obfuscatedEphKey,    // 2
                                            		encryptedStaticKey,  // 3
                                            		staticSharedSecret),
                                            crypto.symKeyLength(),
                                            localStaticPubkey.getBytes(),
                                            new byte[0]));
		
		byte[]        plaintextRaw       = keys[2].decrypt(
		                                     blankIv,
		                                     serialized.array(),
		                                     serialized.position(),
		                                     serialized.remaining());
		
		// This is of dubious effectiveness in Java, but even attempting key hygiene is virtuous.
		Util.zero(ephSharedSecret);
		Util.zero(staticSharedSecret);
		for(Key k : keys) k.destroy();
		
		ByteBuffer plaintext             = ByteBuffer.wrap(plaintextRaw);
		this.authTag                     = new byte[DHTClient.AUTH_TAG_SIZE];
		plaintext.get(authTag);
		
		this.msgId                       = plaintext.getInt();
		this.timestamp                   = plaintext.getLong();
		this.cmd                         = plaintext.get();
		this.flags                       = plaintext.get();
		this.numExpected                 = plaintext.get();
		
		// Verify that this message was not timestamped earlier than the reach of our salt cache.
		long timeDelta = Util.currentTimeMillis() - timestamp;
		assertStateWithoutBlacklist(timeDelta < client.getMaster().getGlobalConfig().getLong("net.dht.maxTimestampDelta"));
		assertStateWithoutBlacklist(timeDelta > client.getMaster().getGlobalConfig().getLong("net.dht.minTimestampDelta"));
		
		this.payload                     = new byte[plaintext.remaining()];
		plaintext.get(payload);
		try {
			this.peer                    = client.routingTable.peerForMessage(
			                                  senderAddress,
			                                  senderPort,
			                                  remoteStaticPubkey);
			if((flags & FLAG_RESPONSE) != 0) {
				this.peer.remoteAuthTag  = this.authTag;
			}
		} catch(UnknownHostException exc) {
			// This shouldn't happen because the senderAddress is guaranteed to be an IP
			throw new BenignProtocolViolationException();
		}
	}
	
	protected int headerSize() {
		return DHTClient.AUTH_TAG_SIZE  // authTag
			 + 4                        // msgId
			 + 8                        // timestamp
			 + 1                        // cmd
			 + 1                        // flags
			 + 1;                       // numPackets
	}
	
	protected int maxPayloadSize() {
		return peer.client.crypto.symPadToReachSize(
				   peer.client.getMaster().getGlobalConfig().getInt("net.dht.maxDatagramSize")
				 - 8                                        // random
			     - peer.client.crypto.asymPublicDHKeySize() // ephemeral pubkey
				 - peer.client.crypto.asymPublicDHKeySize() // static pubkey
				 - headerSize()                             // header in plaintext
			);
	}
	
	public boolean hasValidAuthTag() {
		return Arrays.equals(peer.localAuthTag(), authTag);
	}
	
	/** Raise a ProtocolViolationException unless the auth tag for this message is appropriate for the associated remote peer.
	 * This exception is intended to halt message processing and prohibit further communication with the remote peer. */ 
	protected void assertValidAuthTag() throws ProtocolViolationException {
		if(!hasValidAuthTag()) throw new ProtocolViolationException();
	}
	
	/** Raise a ProtocolViolationException if given false value, which is intended to halt message processing and prohibit further communication with the remote peer. */
	protected void assertState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}
	
	/** Raise a BenignProtocolViolationException if given false value, which is intended to halt message processing without prohibiting further communication with the remote peer. */
	protected void assertStateWithoutBlacklist(boolean state) throws BenignProtocolViolationException {
		if(!state) throw new BenignProtocolViolationException();
	}
}
