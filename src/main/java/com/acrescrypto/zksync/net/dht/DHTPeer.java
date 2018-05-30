package com.acrescrypto.zksync.net.dht;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
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
	protected PublicDHKey key;
	protected byte[] remoteAuthTag;
	
	public DHTPeer(DHTClient client, String address, int port, byte[] pubKey) {
		this.client = client;
		this.address = address;
		this.port = port;

		this.key = client.crypto.makePublicDHKey(pubKey);
		this.id = new DHTID(client.crypto.hash(pubKey));
	}
	
	public DHTPeer(DHTClient client, byte[] serialized) {
		this.client = client;
		deserialize(serialized);
	}
	
	public boolean isBad() {
		// TODO DHT: (implement) return true if peer connection is bad
		return false;
	}
	
	public void ping() {
		DHTMessage.pingMessage(this, null);
	}
	
	public void findNode(DHTID nodeId, DHTFindNodeCallback callback) {
		DHTMessage.findNodeMessage(this, nodeId, (resp)->{
			ArrayList<DHTPeer> receivedPeers = new ArrayList<>();
			ByteBuffer buf = ByteBuffer.wrap(resp.payload);
			while(buf.hasRemaining()) {
				int nextLen = buf.getShort();
				if(nextLen < 0 || nextLen > buf.remaining()) throw new ProtocolViolationException();
				
				byte[] serialized = new byte[nextLen];
				buf.get(serialized);
				
				DHTPeer peer = new DHTPeer(client, serialized);
				receivedPeers.add(peer);
				client.routingTable.suggestPeer(peer);
			}
			
			callback.response(receivedPeers, resp.isFinal);
		});
	}
	
	public void getRecords(DHTID recordId, DHTGetRecordsCallback callback) {
		DHTMessage.getRecordsMessage(this, recordId, (resp)->{
			ArrayList<DHTRecord> receivedRecords = new ArrayList<>();
			ByteBuffer buf = ByteBuffer.wrap(resp.payload);
			while(buf.hasRemaining()) {
				int nextLen = buf.getShort();
				if(nextLen < 0 || nextLen > buf.remaining()) throw new ProtocolViolationException();
				
				byte[] serialized = new byte[nextLen];
				buf.get(serialized);
				receivedRecords.add(DHTRecord.recordForSerialization(serialized));
			}
			
			callback.response(receivedRecords, resp.isFinal);
		});
	}
	
	public void addRecord(DHTID recordId, DHTRecord record) {
		DHTMessage.addRecordMessage(this, recordId, record, null).send();
	}
	
	public boolean equals(Object o) {
		return id.equals(o);
	}
	
	public byte[] localAuthTag() {
		String authStr = address + ":" + port + ":" + Util.bytesToHex(key.getBytes());
		return client.tagKey.authenticate(authStr.getBytes());
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public byte[] serialize() {
		// TODO DHT: (implement) serialize peer
		return null;
	}
	
	protected void deserialize(byte[] serialized) {
		// TODO DHT: (implement) deserialize peer
	}
}
