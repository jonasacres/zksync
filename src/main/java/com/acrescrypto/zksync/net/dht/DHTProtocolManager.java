package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.net.DatagramPacket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.net.dht.DHTClient.LookupCallback;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageCallback;
import com.acrescrypto.zksync.utility.Util;

public class DHTProtocolManager {
	protected DHTClient                      client;
	protected ArrayList<DHTMessageStub>      pendingRequests   = new ArrayList<>();
	protected LinkedList<DHTSearchOperation> pendingOperations = new LinkedList<>();
	protected boolean                        autofind          = true,
			                                 initialized       = false;

	private Logger logger = LoggerFactory.getLogger(DHTProtocolManager.class);
	
	public DHTProtocolManager(DHTClient client) {
		this.client = client;
		this.pendingRequests = new ArrayList<>();
	}
	
	protected DHTProtocolManager() {}
	
	public void clearPendingRequests() {
		pendingRequests.clear();
	}
	
	public DHTPeer getLocalPeer() {
		return new DHTPeer(client,
				client.getSocketManager().getBindAddress(),
				client.getSocketManager().getPort(),
				client.getPublicKey().getBytes()
			);
	}
	
	protected DHTMessage pingMessage(DHTPeer recipient, DHTMessageCallback callback) {
		return new DHTMessage(recipient, DHTMessage.CMD_PING, new byte[0], callback);
	}
	
	public void findPeers() {
		if(!client.getSocketManager().isListening()) return;
		Key key = new Key(client.getMaster().getCrypto());
		
		logger.debug("DHT -: Finding peers...");
		addOperation(new DHTSearchOperation(client, client.getId(), key, (op, peers)->{
			logger.debug("DHT -: Found {} peers", peers.size());
			// no need to do anything; just doing the search populates the routing table
			if(peers == null || peers.isEmpty()) {
				client.updateStatus(DHTClient.STATUS_QUESTIONABLE);
			}
			
			initialized = true;
			finishedOperation(op);
		}, (record)->{
			// just ignore records here
		}));
	}
	
	public void autoFindPeers() {
		new Thread(client.getThreadGroup(), ()->{
			Util.setThreadName("DHTClient autoFindPeers");
			logger.debug("DHT -: Starting autoFindPeers thread; querying each {}ms",
					DHTClient.autoFindPeersIntervalMs);
			while(!client.isClosed()) {
				Util.blockOn(
						() ->   client.getRoutingTable().allPeers().isEmpty() 
							|| !client.getSocketManager().isListening()
							|| !autofind
					);
				findPeers();
				Util.sleep(DHTClient.autoFindPeersIntervalMs);
			}
			logger.debug("DHT -: Stopping autoFindPeers thread");
		}).start();
	}

	public void lookup(DHTID searchId, Key lookupKey, LookupCallback callback) {
		addOperation(new DHTSearchOperation(this.client, searchId, lookupKey, (op, peers)->{
			if(peers == null || peers.isEmpty()) {
				client.updateStatus(DHTClient.STATUS_QUESTIONABLE);
			}
			
			callback.receivedRecord(null); // we won't be getting any more records, so signal end of results
			finishedOperation(op);
		}, (record)->{
			try {
				logger.info("DHT {}:{}: Received TCP ad record for ID {}: {}, key {}",
						record.sender.address,
						record.sender.port,
						client.getId().toShortString(),
						record.routingInfo(),
						Util.bytesToHex(record.asAd().asTcp().getPubKey().getBytes()));
			} catch(Exception exc) {
				logger.info("DHT {}:{}: Received record for ID {}: {}",
						record.sender.address,
						record.sender.port,
						client.getId().toShortString(),
						record.routingInfo());
			}
			callback.receivedRecord(record);
		}));
	}
	
	public void addRecord(DHTID searchId, Key lookupKey, DHTRecord record) {
		addOperation(new DHTSearchOperation(this.client, searchId, lookupKey, (op, peers)->{
			if(peers == null || peers.isEmpty()) {
				client.updateStatus(DHTClient.STATUS_QUESTIONABLE);
			}
			
			for(DHTPeer peer : peers) {
				boolean keysMatch = Arrays.equals(
						client.getPublicKey().getBytes(),
						peer.key.getBytes()
					);
				if(keysMatch) {
					try {
						byte[] token = lookupKey.authenticate(Util.concat(searchId.rawId, peer.key.getBytes()));
						client.getRecordStore().addRecordForId(searchId, token, record);
					} catch (IOException exc) {
						logger.error("DHT {}:{}: Encountered exception adding record from peer, record search id {}",
								peer.address,
								peer.port,
								Util.bytesToHex(searchId.rawId),
								exc);
					}
				} else {
					peer.addRecord(searchId, lookupKey, record);
				}
			}
			
			finishedOperation(op);
		}, (existingRecord)->{
			// don't need to do anything with existing records here
		}));
	}
	
	public void markUninitialized() {
		initialized = false;
	}
	
	public boolean isInitialized() {
		return initialized;
	}
	
	protected DHTMessage findNodeMessage(DHTPeer recipient, DHTID id, Key lookupKey, DHTMessageCallback callback) {
		ByteBuffer buf = ByteBuffer.allocate(id.rawId.length + lookupKey.getCrypto().hashLength());
		buf.put(id.rawId);
		buf.put(lookupKey.authenticate(Util.concat(id.rawId, recipient.key.getBytes())));
		return new DHTMessage(recipient, DHTMessage.CMD_FIND_NODE, buf.array(), callback);
	}
	
	protected DHTMessage addRecordMessage(DHTPeer recipient, DHTID id, Key lookupKey, DHTRecord record, DHTMessageCallback callback) {
		byte[] serializedRecord = record.serialize();
		ByteBuffer buf = ByteBuffer.allocate(
				   id.rawId.length
				 + client.getMaster().getCrypto().hashLength()
				 + serializedRecord.length);
		buf.put(id.rawId);
		buf.put(lookupKey.authenticate(Util.concat(id.rawId, recipient.key.getBytes())));
		buf.put(record.serialize());
		return new DHTMessage(recipient, DHTMessage.CMD_ADD_RECORD, buf.array(), callback);
	}

	
	protected synchronized void addOperation(DHTSearchOperation op) {
		pendingOperations.add(op);
		op.run();
	}
	
	protected synchronized void finishedOperation(DHTSearchOperation op) {
		pendingOperations.remove(op);
	}
	
	protected synchronized void cancelOperations() {
		for(DHTSearchOperation op : pendingOperations) {
			op.cancel();
		}
		
		pendingOperations.clear();
	}
	
	protected synchronized void watchForResponse(DHTMessage message, DatagramPacket packet) {
		pendingRequests.add(new DHTMessageStub(message, packet));
	}
	
	protected void missedResponse(DHTMessageStub stub) {
		stub.peer.missedMessage();
		stopWatchingForResponse(stub);
	}
	
	protected synchronized void stopWatchingForResponse(DHTMessageStub stub) {
		pendingRequests.remove(stub);
	}
	
	protected void processMessage(String senderAddress, int senderPort, ByteBuffer data) {
		if(client.getBlacklist().contains(senderAddress)) {
			logger.info("DHT {}:{}: Ignoring message from blacklisted peer",
					senderAddress,
					senderPort);
			return;
		}

		try {
			DHTMessage message = new DHTMessage(client, senderAddress, senderPort, data);
						
			if(message.isResponse()) {
				processResponse(message);
			} else {
				processRequest(message);
			}
		} catch(BenignProtocolViolationException exc) {
			logger.info("DHT {}:{}: Received suspicious message; ignoring.",
					senderAddress,
					senderPort,
					exc);
		} catch(ProtocolViolationException exc) {
			logger.warn("DHT {}:{}: Received illegal message; blacklisting.",
					senderAddress,
					senderPort,
					exc);
			try {
				client.getBlacklist().add(senderAddress, Blacklist.DEFAULT_BLACKLIST_DURATION_MS);
			} catch(IOException exc2) {
				logger.error("DHT {}:{}: Encountered exception while blacklisting peer",
						senderAddress,
						senderPort,
						exc2);
			}
		} catch(SecurityException exc) {
			logger.warn("DHT {}:{}: Received indecipherable message; ignoring.",
					senderAddress,
					senderPort);
		}
	}
	
	protected void processResponse(DHTMessage message) throws ProtocolViolationException {
		if(client.getStatus() < DHTClient.STATUS_CAN_REQUEST) {
			client.updateStatus(DHTClient.STATUS_CAN_REQUEST);
		}
		
		logger.debug("DHT {}:{}: Received response to message {}",
				message.peer.address,
				message.peer.port,
				message.msgId);
		
		message.peer.acknowledgedMessage();
		message.peer.remoteAuthTag = message.authTag;
		
		DHTMessageStub stub = null;
		synchronized(this) {
			for(DHTMessageStub request : pendingRequests) {
				if(request.matchesMessage(message)) {
					stub = request;
				}
			}
		}
		
		if(stub == null) return; // ignore responses for stubs we don't have anymore
		stub.dispatchResponse(message);
		client.getRoutingTable().markFresh(message.peer);
	}

	protected void processRequest(DHTMessage message) throws ProtocolViolationException {
		client.updateStatus(DHTClient.STATUS_GOOD);
		
		try {
			switch(message.cmd) {
			case DHTMessage.CMD_PING:
				processRequestPing(message);
				break;
				
			case DHTMessage.CMD_FIND_NODE:
				processRequestFindNode(message);
				break;
				
			case DHTMessage.CMD_ADD_RECORD:
				processRequestAddRecord(message);
				break;
				
			default:
				throw new UnsupportedProtocolException();
			}
		} catch (UnsupportedProtocolException e) {
			logger.warn("DHT {}:{}: Received unsupported message from peer; ignoring.",
					message.peer.address,
					message.peer.port);
		}
	}
	
	protected void processRequestPing(DHTMessage message) {
		logger.debug("DHT {}:{}: received ping, msgId={}",
				message.peer.address,
				message.peer.port,
				message.msgId);

		message.makeResponse(null).send();
	}
	
	protected void processRequestFindNode(DHTMessage message) throws ProtocolViolationException, UnsupportedProtocolException {
		int expectedLen = client.idLength() + client.getMaster().getCrypto().hashLength();
		assertSupportedState(message.payload.length == expectedLen);

		ByteBuffer buf  = ByteBuffer.wrap(message.payload);
		byte[] idBytes  = new byte[client.idLength()];
		byte[] token    = new byte[client.getMaster().getCrypto().hashLength()];
		
		buf.get(idBytes);
		buf.get(token);
		
		logger.debug("DHT {}:{}: recv findNode {}, msgId={}",
				message.peer.address,
				message.peer.port,
				Util.bytesToHex(idBytes),
				message.msgId);
		
		DHTID id                         = new DHTID(idBytes);
		
		Collection<DHTPeer> closestPeers = client.getRoutingTable().closestPeers(
				id,
				DHTSearchOperation.maxResults
			); 
		DHTMessage response = message.makeResponse(closestPeers);
		response.addItemList(client.getRecordStore().recordsForId(id, token));
		response.send();
	}
	
	protected void processRequestAddRecord(DHTMessage message) throws ProtocolViolationException, UnsupportedProtocolException {
		ByteBuffer buf = ByteBuffer.wrap(message.payload);
		
		assertSupportedState(validAuthTag(message.peer, message.authTag));
		
		byte[] idRaw = new byte[client.idLength()];
		byte[] token = new byte[client.getMaster().getCrypto().hashLength()];
		assertSupportedState(buf.remaining() > idRaw.length + token.length);		
		buf.get(idRaw);
		buf.get(token);
		DHTID id = new DHTID(idRaw);

		logger.debug("DHT {}:{}: recv addRecord {}, msgId={}",
				message.peer.address,
				message.peer.port,
				Util.bytesToHex(idRaw),
				message.msgId);

		assertSupportedState(buf.remaining() > 0);
		DHTRecord record = deserializeRecord(message.peer, buf);
		assertSupportedState(record != null);
		assertSupportedState(record.isValid());
		
		try {
			client.getRecordStore().addRecordForId(id, token, record);
		} catch(IOException exc) {
			logger.error("DHT {}:{}: Encountered exception adding record",
					message.peer.address,
					message.peer.port,
					exc);
		}
		
		message.makeResponse(new ArrayList<>(0)).send();
	}
	
	// having the call happen here instead of directly accessing DHTRecord is convenient for stubbing out test classes
	protected DHTRecord deserializeRecord(DHTPeer sender, ByteBuffer serialized) throws UnsupportedProtocolException {
		if(sender == null) {
			return DHTRecord.deserializeRecord(
					client.getMaster().getCrypto(),
					serialized
				);
		} else {
			DHTRecord record = DHTRecord.deserializeRecordWithPeer(
					sender,
					serialized
				);
			record.setSender(sender);
			return record;
		}
	}

	public ArrayList<DHTMessageStub> getPendingRequests() {
		return pendingRequests;
	}
	
	protected boolean validAuthTag(DHTPeer peer, byte[] tag) {
		return Arrays.equals(peer.localAuthTag(), tag);
	}
	
	protected void assertSupportedState(boolean state) throws UnsupportedProtocolException {
		if(!state) {
			throw new UnsupportedProtocolException();
		}
	}

	public void setAutofind(boolean autofind) {
		this.autofind = autofind;		
	}
	
	public boolean getAutofind() {
		return autofind;
	}
}
