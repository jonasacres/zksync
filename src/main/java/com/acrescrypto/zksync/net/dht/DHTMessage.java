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

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;

public class DHTMessage {
	public final static byte CMD_PING = 0;
	public final static byte CMD_FIND_NODE = 1;
	public final static byte CMD_ADD_RECORD = 2;
	
	public final static byte FLAG_RESPONSE = 0x01;
	
	public interface DHTMessageCallback {
		void responseReceived(DHTMessage response) throws ProtocolViolationException;
	}
	
	DHTMessageCallback callback;
	DHTPeer peer;
	int msgId;
	byte cmd, flags, numExpected;
	byte[] payload, authTag;
	boolean isFinal;
	ArrayList<Collection<? extends Sendable>> itemLists;
	Logger logger = LoggerFactory.getLogger(DHTMessage.class);
	
	public DHTMessage(DHTPeer recipient, byte cmd, byte[] payload, DHTMessageCallback callback) {
		this(recipient, cmd, ByteBuffer.wrap(payload), callback);
	}
	
	public DHTMessage(DHTPeer recipient, byte cmd, ByteBuffer payloadBuf, DHTMessageCallback callback) {
		this.peer = recipient;
		this.cmd = cmd;
		this.flags = 0;
		this.authTag = recipient.remoteAuthTag;
		this.callback = callback;
		this.payload = new byte[payloadBuf.remaining()];
		payloadBuf.get(payload);
		this.msgId = recipient.client.crypto.defaultPrng().getInt();
	}
	
	public DHTMessage(DHTClient client, String senderAddress, int senderPort, ByteBuffer serialized) throws ProtocolViolationException {
		deserialize(client, senderAddress, senderPort, serialized);
	}
	
	public DHTMessage(DHTPeer recipient, byte cmd, int msgId, Collection<? extends Sendable> items) {
		this.peer = recipient;
		this.cmd = cmd;
		this.flags = FLAG_RESPONSE;
		this.authTag = recipient.localAuthTag();
		this.itemLists = new ArrayList<>();
		if(items != null) this.itemLists.add(new ArrayList<>(items));
		this.msgId = msgId;
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
			peer.client.watchForResponse(this, sendPayload());
		}
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
						sendDatagram(numPackets, sendBuf);
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
		return sendDatagram(numPackets, sendBuf);
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
	
	protected DatagramPacket sendPayload() {
		return sendDatagram(1, ByteBuffer.wrap(payload));
	}
	
	protected DatagramPacket sendDatagram(int numPackets, ByteBuffer sendBuf) {
		sendBuf.position(0);
		InetAddress address;
		try {
			address = InetAddress.getByName(peer.address);
		} catch (UnknownHostException exc) {
			return null;
		}
		byte[] serialized = serialize(numPackets, sendBuf);
		DatagramPacket packet = new DatagramPacket(serialized, serialized.length, address, peer.port);
		logger.debug("DHT: sending {} bytes to {}:{}, cmd={}, flags=0x{}, msgId={}",
				packet.getData().length,
				packet.getAddress().getHostAddress(),
				packet.getPort(),
				cmd,
				String.format("%02x", flags),
				msgId);
		peer.client.sendDatagram(packet);
		return packet;
	}
	
	protected byte[] serialize(int numPackets, ByteBuffer sendBuf) {
		PrivateDHKey ephKey = peer.client.crypto.makePrivateDHKey();
		ByteBuffer keyMaterial = ByteBuffer.allocate(peer.client.networkId.length+peer.client.crypto.asymDHSecretSize());
		keyMaterial.put(peer.client.networkId);
		keyMaterial.put(ephKey.sharedSecret(peer.key));
		byte[] symKeyRaw = peer.client.crypto.makeSymmetricKey(keyMaterial.array());
		Key symKey = new Key(peer.client.crypto, symKeyRaw);
		
		ByteBuffer plaintext = ByteBuffer.allocate(headerSize() + sendBuf.remaining());
		plaintext.put(peer.client.key.publicKey().getBytes());
		plaintext.put(authTag);
		plaintext.putInt(msgId);
		plaintext.put(cmd);
		plaintext.put(flags);
		plaintext.put((byte) numPackets);
		plaintext.put(sendBuf);
		
		byte[] ciphertext = symKey.encrypt(new byte[peer.client.crypto.symIvLength()], plaintext.array(), 0);
		
		ByteBuffer serialized = ByteBuffer.allocate(ephKey.getBytes().length + ciphertext.length);
		serialized.put(ephKey.publicKey().getBytes());
		serialized.put(ciphertext);
		return serialized.array();
	}
	
	protected void deserialize(DHTClient client, String senderAddress, int senderPort, ByteBuffer serialized) throws ProtocolViolationException {
		byte[] keyBytes = new byte[client.crypto.asymPublicDHKeySize()];

		assertState(serialized.remaining() > keyBytes.length);
		serialized.get(keyBytes);
		
		PublicDHKey pubKey = new PublicDHKey(client.crypto, keyBytes);
		ByteBuffer keyMaterial = ByteBuffer.allocate(client.networkId.length+client.crypto.asymDHSecretSize());
		keyMaterial.put(client.networkId);
		keyMaterial.put(client.key.sharedSecret(pubKey));
		Key key = new Key(client.crypto, client.crypto.makeSymmetricKey(keyMaterial.array()));
		
		try {
			byte[] plaintext = key.decrypt(new byte[client.crypto.symIvLength()], serialized.array(), serialized.position(), serialized.remaining());
			serialized = ByteBuffer.wrap(plaintext);
		} catch(SecurityException exc) {
			logger.error("DHT: Cannot decrypt message from peer {}:{}",
					peer.address,
					peer.port,
					exc);
			throw new ProtocolViolationException();
		}
		
		assertState(serialized.remaining() >= keyBytes.length + DHTClient.AUTH_TAG_SIZE + 4 + 1 + 1 + 1);
		serialized.get(keyBytes);

		this.authTag = new byte[DHTClient.AUTH_TAG_SIZE];
		serialized.get(authTag);

		this.peer = client.routingTable.peerForMessage(senderAddress, senderPort, client.crypto.makePublicDHKey(keyBytes));
		this.msgId = serialized.getInt();
		this.cmd = serialized.get();
		this.flags = serialized.get();
		this.numExpected = serialized.get();
		this.payload = new byte[serialized.remaining()];
		serialized.get(payload);
	}
	
	protected int headerSize() {
		return peer.client.crypto.asymPublicDHKeySize() + DHTClient.AUTH_TAG_SIZE + 4 + 1 + 1 + 1;
	}
	
	protected int maxPayloadSize() {
		return peer.client.crypto.symPadToReachSize(DHTClient.MAX_DATAGRAM_SIZE-headerSize()-peer.client.crypto.asymPublicDHKeySize());
	}
	
	protected void assertValidAuthTag() throws ProtocolViolationException {
		if(!Arrays.equals(peer.localAuthTag(), authTag)) throw new ProtocolViolationException();
	}
	
	protected void assertState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}
}
