package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageStub;

public class DHTClient {
	public final static int MAX_DATAGRAM_SIZE = 508; // 576 byte (guaranteed by RFC 791) - 60 byte IP header - 8 byte UDP header
	
	interface PeerForReferenceCallback {
		void receivedPeerForReference(DHTPeer peer);
	}
	
	interface LookupCallback {
		void receivedRecord(DHTRecord ad);
	}
	
	private Logger logger = LoggerFactory.getLogger(DHTClient.class);

	DatagramSocket socket;
	Blacklist blacklist;
	DHTRecordStore store;
	CryptoSupport crypto;
	DHTID id;
	DHTRoutingTable routingTable;
	PrivateDHKey key;
	Key tagKey;
	
	ArrayList<DHTMessageStub> pendingRequests;
	
	// TODO DHT: (implement) constructor
	// TODO DHT: (implement) socket listener (be sure to add every peer we meet to routing table)
	
	public void findPeers() {
		new DHTSearchOperation(this, id, (peers)->{
			// nodes automatically added to table as we go
		});
	}
	
	public void lookup(DHTID searchId, LookupCallback callback) {
		new DHTSearchOperation(this, searchId, (peers)->{
			MutableInt pending = new MutableInt();
			pending.setValue(peers.size());
			for(DHTPeer peer : peers) {
				peer.getRecords(searchId, (records, isFinal)->{
					if(records != null) {
						for(DHTRecord record : records) {
							callback.receivedRecord(record);
						}
					}
					
					if(isFinal) {
						// TODO DHT: (implement) Needs to be a timeout on this so we still signal "done" if packets are lost
						boolean last;
						synchronized(pending) { last = pending.decrementAndGet() == 0; }
						if(last) {
							callback.receivedRecord(null);
						}
					}
				});
			}
		});
	}
	
	public int authTagLength() {
		return crypto.hashLength();
	}
	
	public int idLength() {
		return crypto.hashLength();
	}
	
	protected void processMessage(String senderAddress, int senderPort, byte[] data) throws ProtocolViolationException {
		try {
			DHTMessage message = new DHTMessage(this, senderAddress, senderPort, data);
			
			if(blacklist.contains(senderAddress)) {
				logger.info("Ignoring message from blacklisted peer " + senderAddress);
				return;
			}
			
			if(message.isResponse()) {
				processResponse(message);
			} else {	
				processRequest(message);
			}
		} catch(SecurityException exc) {
			logger.warn("Received indecipherable message from " + senderAddress + "; ignoring.");
		}
	}
	
	protected void processResponse(DHTMessage message) throws ProtocolViolationException {
		for(DHTMessageStub request : pendingRequests) {
			if(request.dispatchResponseIfMatches(message)) {
				routingTable.freshen(message.peer);
				break;
			}
		}
	}

	protected void processRequest(DHTMessage message) {
		try {
			switch(message.cmd) {
			case DHTMessage.CMD_PING:
				processRequestPing(message);
				break;
			case DHTMessage.CMD_FIND_NODE:
				processRequestFindNode(message);
				break;
			case DHTMessage.CMD_GET_RECORDS:
				processRequestGetRecords(message);
				break;
			case DHTMessage.CMD_ADD_RECORD:
				processRequestAddRecord(message);
				break;
			default:
				throw new UnsupportedProtocolException();
			}
		} catch(ProtocolViolationException exc) {
			// TODO DHT: (refactor) move to socket listener
			logger.warn("Received illegal message from " + message.peer.address + "; blacklisting.");
			try {
				blacklist.add(message.peer.address, Blacklist.DEFAULT_BLACKLIST_DURATION_MS);
			} catch(IOException exc2) {
				logger.error("Encountered exception blacklisting peer {}", message.peer.address, exc2);
			}
		} catch (UnsupportedProtocolException e) {
			logger.warn("Received unsupported message from " + message.peer.address + "; ignoring.");
		}
	}
	
	protected void processRequestPing(DHTMessage message) {
		message.makeResponse(null).send();
	}
	
	protected void processRequestFindNode(DHTMessage message) throws ProtocolViolationException {
		assertRequiredState(message.payload.length == idLength());
		DHTID id = new DHTID(message.payload);
		DHTID greatestDistance = null;
		DHTPeer mostDistantPeer = null;
		ArrayList<DHTPeer> closest = new ArrayList<>(DHTBucket.MAX_BUCKET_CAPACITY);
		
		for(DHTPeer peer : routingTable.allPeers()) {
			DHTID distance = peer.id.xor(id);
			if(greatestDistance == null || distance.compareTo(greatestDistance) < 0) {
				if(mostDistantPeer != null) {
					closest.remove(mostDistantPeer);
				}
				
				closest.add(peer);
				greatestDistance = distance;
				for(DHTPeer listed : closest) {
					DHTID listedDistance = listed.id.xor(id);
					if(listedDistance.compareTo(greatestDistance) > 0) {
						greatestDistance = listedDistance;
						mostDistantPeer = listed;
					}
				}
			}
		}
		
		message.makeResponse(closest);
	}
	
	protected void processRequestGetRecords(DHTMessage message) throws ProtocolViolationException {
		assertRequiredState(message.payload.length == idLength());
		DHTID id = new DHTID(message.payload);
		message.makeResponse(store.recordsForId(id));
	}
	
	protected void processRequestAddRecord(DHTMessage message) throws ProtocolViolationException, UnsupportedProtocolException {
		ByteBuffer buf = ByteBuffer.wrap(message.payload);
		
		byte[] authTag = new byte[authTagLength()];
		assertRequiredState(buf.remaining() > authTag.length);
		buf.get(authTag);
		assertRequiredState(validAuthTag(message.peer, authTag));
		
		byte[] idRaw = new byte[idLength()];
		assertRequiredState(buf.remaining() > idRaw.length);
		buf.get(idRaw);
		DHTID id = new DHTID(idRaw);
		
		assertRequiredState(buf.remaining() > 0);
		byte[] recordRaw = new byte[buf.remaining()];
		buf.get(recordRaw);
		DHTRecord record = DHTRecord.recordForSerialization(recordRaw);
		assertSupportedState(record != null);
		assertRequiredState(record.validate());
		assertRequiredState(record.comesFrom(message.peer));
		
		store.addRecordForId(id, record);
	}
	
	protected boolean validAuthTag(DHTPeer peer, byte[] tag) {
		return Arrays.equals(peer.localAuthTag(), tag);
	}
	
	protected void assertSupportedState(boolean state) throws UnsupportedProtocolException {
		if(!state) throw new UnsupportedProtocolException();
	}
	
	protected void assertRequiredState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}
}
