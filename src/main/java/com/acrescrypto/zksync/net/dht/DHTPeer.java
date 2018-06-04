package com.acrescrypto.zksync.net.dht;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.utility.Util;

public class DHTPeer implements Sendable {
	interface DHTFindNodeCallback {
		void response(Collection<DHTPeer> references, boolean isFinal);
	}
	
	interface DHTGetRecordsCallback {
		void response(Collection<DHTRecord> records, boolean isFinal);
	}
	
	protected DHTClient client;
	protected DHTID id;
	protected String address;
	protected int port;
	int missedMessages;
	protected PublicDHKey key;
	protected byte[] remoteAuthTag = new byte[DHTClient.AUTH_TAG_SIZE];
	
	public DHTPeer(DHTClient client, String address, int port, byte[] key) {
		this(client, address, port, client.crypto.makePublicDHKey(key));
	}
	
	public DHTPeer(DHTClient client, String address, int port, PublicDHKey key) {
		this.client = client;
		this.address = address;
		this.port = port;

		this.key = key;
		this.id = new DHTID(key);
	}
	
	public DHTPeer(DHTClient client, ByteBuffer serialized) throws EINVALException {
		this.client = client;
		deserialize(serialized);
	}
	
	public boolean isBad() {
		return missedMessages > 1;
	}
	
	public synchronized void missedMessage() {
		missedMessages++;
	}
	
	public synchronized void acknowledgedMessage() {
		missedMessages = 0;
	}
	
	public void ping() {
		client.pingMessage(this, null).send();
	}
	
	public void findNode(DHTID nodeId, DHTFindNodeCallback callback) {
		client.findNodeMessage(this, nodeId, (resp)->{
			ArrayList<DHTPeer> receivedPeers = new ArrayList<>();
			this.remoteAuthTag = resp.authTag;
			ByteBuffer buf = ByteBuffer.wrap(resp.payload);
			while(buf.hasRemaining()) {
				if(buf.remaining() < 2) throw new ProtocolViolationException();
				
				int nextLen = Util.unsignShort(buf.getShort());
				if(nextLen <= 0 || nextLen > buf.remaining()) throw new ProtocolViolationException();
				int expectedPos = buf.position() + nextLen;
				DHTPeer peer;
				
				try {
					peer = new DHTPeer(client, buf);
				} catch(EINVALException exc) {
					throw new ProtocolViolationException();
				}
				
				if(expectedPos != buf.position()) throw new ProtocolViolationException();
				
				receivedPeers.add(peer);
				client.routingTable.suggestPeer(peer);
			}
			
			callback.response(receivedPeers, resp.isFinal);
		}).send();
	}
	
	public void getRecords(DHTID recordId, DHTGetRecordsCallback callback) {
		client.getRecordsMessage(this, recordId, (resp)->{
			ArrayList<DHTRecord> receivedRecords = new ArrayList<>();
			this.remoteAuthTag = resp.authTag;
			ByteBuffer buf = ByteBuffer.wrap(resp.payload);
			while(buf.hasRemaining()) {
				int nextLen = buf.getShort();
				if(nextLen < 0 || nextLen > buf.remaining()) throw new ProtocolViolationException();
				int expectedPos = buf.position() + nextLen;
				
				try {
					receivedRecords.add(client.deserializeRecord(buf));
					if(expectedPos != buf.position()) throw new ProtocolViolationException();
				} catch (UnsupportedProtocolException exc) {
					exc.printStackTrace();
					buf.position(expectedPos);
				}
			}
			
			callback.response(receivedRecords, resp.isFinal);
		}).send();
	}
	
	public void addRecord(DHTID recordId, DHTRecord record) {
		client.addRecordMessage(this, recordId, record, null).send();
	}
	
	public boolean equals(Object o) {
		return id.equals(o);
	}
	
	public byte[] localAuthTag() {
		byte[] tag = new byte[DHTClient.AUTH_TAG_SIZE];
		String authStr = address + ":" + port + ":" + Util.bytesToHex(key.getBytes());
		System.arraycopy(client.tagKey.authenticate(authStr.getBytes()), 0, tag, 0, tag.length);
		return tag;
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(2 + address.length() + 2 + 2 + key.getBytes().length);
		buf.putShort((short) address.length());
		buf.put(address.getBytes());
		buf.putShort((short) port);
		buf.putShort((short) key.getBytes().length);
		buf.put(key.getBytes());
		return buf.array();
	}
	
	public boolean matches(String address, int port, PublicDHKey key) {
		return this.port == port && this.address.equals(address) && this.key.equals(key);
	}
	
	protected void deserialize(ByteBuffer serialized) throws EINVALException {
		try {
			int addrLen = Util.unsignShort(serialized.getShort());
			this.address = new String(serialized.array(), serialized.position(), addrLen);
			serialized.position(serialized.position() + addrLen);
			this.port = Util.unsignShort(serialized.getShort());
			int keyLen = Util.unsignShort(serialized.getShort());
			byte[] keyBytes = new byte[keyLen];
			serialized.get(keyBytes);
			this.key = client.crypto.makePublicDHKey(keyBytes);
			this.id = new DHTID(this.key);
		} catch(BufferUnderflowException exc) {
			throw new EINVALException("(dht peer)");
		}
	}
}
