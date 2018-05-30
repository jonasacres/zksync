package com.acrescrypto.zksync.net.dht;

import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Collection;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;

public class DHTMessage {
	public final static byte CMD_PING = 0;
	public final static byte CMD_FIND_NODE = 1;
	public final static byte CMD_GET_RECORDS = 2;
	public final static byte CMD_ADD_RECORD = 3;
	
	public final static byte FLAG_RESPONSE = 0x01;
	
	public interface DHTMessageCallback {
		void responseReceived(DHTMessage response) throws ProtocolViolationException;
	}
	
	DHTMessageCallback callback;
	DHTPeer peer;
	int msgId;
	byte cmd, flags, numExpected;
	byte[] payload;
	boolean isFinal;
	Collection<? extends Sendable> items;
	
	static DHTMessage pingMessage(DHTPeer recipient, DHTMessageCallback callback) {
		return new DHTMessage(recipient, CMD_PING, new byte[0], callback);
	}
	
	static DHTMessage findNodeMessage(DHTPeer recipient, DHTID id, DHTMessageCallback callback) {
		return new DHTMessage(recipient, CMD_FIND_NODE, id.rawId, callback);
	}
	
	static DHTMessage getRecordsMessage(DHTPeer recipient, DHTID id, DHTMessageCallback callback) {
		return new DHTMessage(recipient, CMD_GET_RECORDS, id.rawId, callback);
	}
	
	static DHTMessage addRecordMessage(DHTPeer recipient, DHTID id, DHTRecord record, DHTMessageCallback callback) {
		byte[] serializedRecord = record.serialize();
		ByteBuffer buf = ByteBuffer.allocate(id.rawId.length + serializedRecord.length + recipient.remoteAuthTag.length);
		buf.put(recipient.remoteAuthTag);
		buf.put(id.rawId);
		buf.put(record.serialize());
		return new DHTMessage(recipient, CMD_ADD_RECORD, buf.array(), callback);
	}
	
	public DHTMessage(DHTPeer recipient, byte cmd, byte[] payload, DHTMessageCallback callback) {
		this(recipient, cmd, ByteBuffer.wrap(payload), callback);
	}
	
	public DHTMessage(DHTPeer recipient, byte cmd, ByteBuffer payloadBuf, DHTMessageCallback callback) {
		this.peer = recipient;
		this.cmd = cmd;
		this.flags = 0;
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
		this.items = items;
		this.msgId = msgId;
	}
	
	public boolean isResponse() {
		return (flags & FLAG_RESPONSE) != 0;
	}
	
	public DHTMessage makeResponse(Collection<? extends Sendable> items) {
		return new DHTMessage(peer, cmd, msgId, items);
	}
	
	public void send() {
		ByteBuffer sendBuf = ByteBuffer.allocate(maxPayloadSize());
		int numPackets = 0;
		peer.client.watchForResponse(this);
		
		if(items != null) {
			int totalLen = 0;
			
			for(Sendable item : items) {
				totalLen += item.serialize().length + 2;
			}
			
			numPackets = (int) Math.ceil(((double) totalLen)/maxPayloadSize());
			
			for(Sendable item : items) {
				byte[] serialized = item.serialize();
				int itemLen = 2 + serialized.length;
				if(sendBuf.capacity() < itemLen) continue;
				if(sendBuf.remaining() < itemLen) {
					sendDatagram(numPackets, sendBuf);
					sendBuf.clear();
				}
				
				sendBuf.putShort((short) serialized.length);
				sendBuf.put(serialized);
			}
		}
		
		sendDatagram(numPackets, sendBuf);
	}
	
	protected void sendDatagram(int numPackets, ByteBuffer sendBuf) {
		sendBuf.position(0);
		InetAddress address;
		try {
			address = InetAddress.getByName(peer.address);
		} catch (UnknownHostException exc) {
			return;
		}
		byte[] serialized = serialize(numPackets, sendBuf);
		DatagramPacket packet = new DatagramPacket(serialized, serialized.length, address, peer.port);
		peer.client.sendDatagram(packet);
	}
	
	protected byte[] serialize(int numPackets, ByteBuffer sendBuf) {
		PrivateDHKey ephKey = peer.client.crypto.makePrivateDHKey();
		byte[] symKeyRaw = peer.client.crypto.makeSymmetricKey(ephKey.sharedSecret(peer.key));
		Key symKey = new Key(peer.client.crypto, symKeyRaw);

		ByteBuffer plaintext = ByteBuffer.allocate(headerSize() + payload.length);
		plaintext.put(peer.client.key.publicKey().getBytes());
		plaintext.putInt(msgId);
		plaintext.put(cmd);
		plaintext.put(flags);
		plaintext.put((byte) numPackets);
		plaintext.put(sendBuf);
		
		byte[] ciphertext = symKey.encrypt(new byte[peer.client.crypto.symIvLength()], plaintext.array(), 0);
		
		ByteBuffer serialized = ByteBuffer.allocate(ephKey.getBytes().length + ciphertext.length);
		serialized.put(ephKey.getBytes());
		serialized.put(ciphertext);
		return serialized.array();
	}
	
	protected void deserialize(DHTClient client, String senderAddress, int senderPort, ByteBuffer serialized) throws ProtocolViolationException {
		byte[] keyBytes = new byte[client.crypto.asymPublicDHKeySize()];

		assertState(serialized.remaining() > keyBytes.length);
		serialized.get(keyBytes);
		
		PublicDHKey pubKey = new PublicDHKey(client.crypto, keyBytes);
		byte[] keyMaterial = client.key.sharedSecret(pubKey);
		Key key = new Key(client.crypto, client.crypto.makeSymmetricKey(keyMaterial));
		
		byte[] ciphertext = new byte[serialized.remaining()];
		byte[] plaintext = key.decrypt(new byte[client.crypto.symIvLength()], ciphertext);
		serialized = ByteBuffer.wrap(plaintext);
		
		assertState(serialized.remaining() >= keyBytes.length + 4 + 1 + 1);
		serialized.get(keyBytes);
		
		this.peer = new DHTPeer(client, senderAddress, senderPort, keyBytes);
		this.msgId = serialized.getInt();
		this.cmd = serialized.get();
		this.flags = serialized.get();
		this.numExpected = serialized.get();
		this.payload = new byte[serialized.remaining()];
		serialized.get(payload);
	}
	
	protected int headerSize() {
		return peer.client.crypto.asymPublicDHKeySize() + 4 + 1 + 1 + 1;
	}
	
	protected int maxPayloadSize() {
		return DHTClient.MAX_DATAGRAM_SIZE - headerSize();
	}
	
	protected void assertState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}
}
