package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageCallback;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;

public class DHTClient {
	public final static int MAX_DATAGRAM_SIZE = 508; // 576 byte (guaranteed by RFC 791) - 60 byte IP header - 8 byte UDP header
	
	public final static int DEFAULT_LOOKUP_RESULT_MAX_WAIT_TIME_MS = 500;
	public static int LOOKUP_RESULT_MAX_WAIT_TIME_MS = DEFAULT_LOOKUP_RESULT_MAX_WAIT_TIME_MS; // consider a lookup finished if we've received nothing in this many milliseconds
	
	
	public final static int DEFAULT_MESSAGE_EXPIRATION_TIME_MS = 5000;
	public static int MESSAGE_EXPIRATION_TIME_MS = DEFAULT_MESSAGE_EXPIRATION_TIME_MS;
	
	public final static int KEY_INDEX_CLIENT_INFO = 0;
	public final static int KEY_INDEX_ROUTING_TABLE = 1;
	public final static int KEY_INDEX_RECORD_STORE = 2;
	
	public final static int STATUS_OFFLINE = 0; // we are clearly unable to reach the DHT network
	public final static int STATUS_ESTABLISHING = 1; // attempting to establish connection to DHT network
	public final static int STATUS_QUESTIONABLE = 2; // strong possibility we are not connected to network
	public final static int STATUS_CAN_SEND = 3; // able to send DHT traffic and receive replies
	public final static int STATUS_GOOD = 4; // able to send and receive DHT traffic
	
	interface PeerForReferenceCallback {
		void receivedPeerForReference(DHTPeer peer);
	}
	
	interface LookupCallback {
		void receivedRecord(DHTRecord ad);
	}
	
	interface DHTStatusCallback {
		void dhtStatusUpdate(int status);
	}
	
	private Logger logger = LoggerFactory.getLogger(DHTClient.class);

	DatagramSocket socket;
	Blacklist blacklist;
	DHTRecordStore store;
	CryptoSupport crypto;
	DHTID id;
	DHTRoutingTable routingTable;
	PrivateDHKey key;
	Key storageKey, tagKey;
	String bindAddress;
	Thread socketListenerThread;
	boolean closed;
	DHTStatusCallback statusCallback;
	int bindPort;
	int lastStatus = STATUS_OFFLINE;
	FS storage;
	
	ArrayList<DHTMessageStub> pendingRequests;
	
	public DHTClient(Key storageKey, Blacklist blacklist) {
		this.blacklist = blacklist;
		this.storageKey = storageKey;
		this.storage = blacklist.getFS();
		this.crypto = storageKey.getCrypto();
		this.pendingRequests = new ArrayList<>();
		
		read();

		this.store = new DHTRecordStore(this);
		this.routingTable = new DHTRoutingTable(this);
		// TODO DHT: (design) What about the bootstrap peer? Test vs. reality
	}
	
	protected DHTClient() {}
	
	public DHTClient listen(String address, int port) throws SocketException {
		this.bindAddress = address == null ? "0.0.0.0" : address;
		this.bindPort = port;
		openSocket();

		if(socketListenerThread == null || !socketListenerThread.isAlive()) {
			socketListenerThread = new Thread(()->socketListener());
			socketListenerThread.start();
		}
		
		return this;
	}
	
	public DHTClient setStatusCallback(DHTStatusCallback statusCallback) {
		this.statusCallback = statusCallback;
		return this;
	}
	
	public void findPeers() { // TODO DHT: (design) When does this get called?
		new DHTSearchOperation(this, id, (peers)->{
			// no need to do anything; just doing the search populates the routing table
			if(peers == null || peers.isEmpty()) {
				updateStatus(STATUS_QUESTIONABLE);
			}
		}).run();
	}
	
	public void lookup(DHTID searchId, LookupCallback callback) {
		new DHTSearchOperation(this, searchId, (peers)->{
			if(peers == null || peers.isEmpty()) {
				updateStatus(STATUS_QUESTIONABLE);
			}
			
			SnoozeThread progressMonitor = new SnoozeThread(LOOKUP_RESULT_MAX_WAIT_TIME_MS, true, ()->{
				callback.receivedRecord(null);
			});
			
			MutableInt pending = new MutableInt();
			pending.setValue(peers.size());
			for(DHTPeer peer : peers) {
				peer.getRecords(searchId, (records, isFinal)->{
					if(!progressMonitor.snooze()) return;
					
					if(records != null) {
						for(DHTRecord record : records) {
							callback.receivedRecord(record);
						}
					}
					
					if(isFinal) {
						boolean last;
						synchronized(pending) { last = pending.decrementAndGet() == 0; }
						if(last) {
							progressMonitor.cancel();
						}
					}
				});
			}
		}).run();
	}
	
	public void addPeer(DHTPeer peer) {
		routingTable.suggestPeer(peer);
	}
	
	public void addRecord(DHTID searchId, DHTRecord record) {
		new DHTSearchOperation(this, searchId, (peers)->{
			if(peers == null || peers.isEmpty()) {
				updateStatus(STATUS_QUESTIONABLE);
			}
			
			for(DHTPeer peer : peers) {
				peer.addRecord(searchId, record);
			}
		}).run();
	}
	
	public int authTagLength() {
		return crypto.hashLength();
	}
	
	public int idLength() {
		return crypto.hashLength();
	}
	
	public void close() {
		closed = true;
		if(socket != null) {
			socket.close();
		}
	}
	
	protected void openSocket() throws SocketException {
		if(closed) return;
		InetAddress addr;
		try {
			addr = InetAddress.getByName(bindAddress);
		} catch(UnknownHostException exc) {
			throw new RuntimeException("Unable to bind to address " + bindAddress + ":" + bindPort + ": unable to resolve address");
		}
		
		updateStatus(STATUS_ESTABLISHING);
		if(socket != null && !socket.isClosed()) {
			socket.close();
		}
		
		try {
			socket = new DatagramSocket(bindPort, addr);
			socket.setReuseAddress(true);
			updateStatus(STATUS_QUESTIONABLE);
		} catch(SocketException exc) {
			updateStatus(STATUS_OFFLINE);
			throw exc;
		}
	}
	
	protected void socketListener() {
		while(!closed) {
			try {
				if(socket == null) {
					Util.sleep(10);
					continue;
				}
				
				byte[] receiveData = new byte[MAX_DATAGRAM_SIZE];
				DatagramPacket packet = new DatagramPacket(receiveData, receiveData.length);
				socket.receive(packet);
				ByteBuffer buf = ByteBuffer.wrap(packet.getData(), 0, packet.getLength());
				processMessage(packet.getAddress().getHostAddress(), packet.getPort(), buf);
			} catch(IOException exc) {
				if(closed) return;
				logger.error("DHT socket listener thread encountered IOException", exc);
				Util.sleep(1000); // add in a delay to prevent a fail loop from gobbling CPU / spamming log
				try {
					openSocket();
				} catch (SocketException e) {
					logger.error("DHT socket listener thread encountered IOException rebinding socket", exc);
					Util.sleep(9000); // wait another 9 seconds if we know the socket is dead and the OS isn't giving it back
				}
			} catch(Exception exc) {
				logger.error("DHT socket listener thread encountered exception", exc);
			}
		}
	}
	
	protected void watchForResponse(DHTMessage message) {
		pendingRequests.add(new DHTMessageStub(message));
	}
	
	protected void missedResponse(DHTMessageStub stub) {
		stub.peer.missedMessage();
		stopWatchingForResponse(stub);
	}
	
	protected void stopWatchingForResponse(DHTMessageStub stub) {
		pendingRequests.remove(stub);
	}
	
	protected void sendDatagram(DatagramPacket packet) {
		for(int i = 0; i < 2; i++) {
			try {
				socket.send(packet);
				break;
			} catch (IOException exc) {
				if(i == 0) {
					logger.warn("Encountered exception sending on DHT socket; retrying", exc);
					try {
						openSocket();
					} catch (SocketException exc2) {
						logger.error("Encountered exception rebinding DHT socket; giving up on sending message", exc2);
						return;
					}
				} else {
					logger.error("Encountered exception sending on DHT socket; giving up", exc);
				}
			}
		}
	}
	
	protected void processMessage(String senderAddress, int senderPort, ByteBuffer data) {
		try {
			DHTMessage message = new DHTMessage(this, senderAddress, senderPort, data);
			
			if(blacklist.contains(senderAddress)) {
				logger.info("Ignoring message from blacklisted peer " + senderAddress);
				return;
			}
			
			routingTable.suggestPeer(message.peer);
			
			if(message.isResponse()) {
				processResponse(message);
			} else {
				processRequest(message);
			}
		} catch(ProtocolViolationException exc) {
			logger.warn("Received illegal message from " + senderAddress + "; blacklisting.");
			try {
				blacklist.add(senderAddress, Blacklist.DEFAULT_BLACKLIST_DURATION_MS);
			} catch(IOException exc2) {
				logger.error("Encountered exception while blacklisting peer {}", senderAddress, exc2);
			}
		} catch(SecurityException exc) {
			logger.warn("Received indecipherable message from " + senderAddress + "; ignoring.");
		}
	}
	
	protected void processResponse(DHTMessage message) throws ProtocolViolationException {
		if(lastStatus < STATUS_CAN_SEND) updateStatus(STATUS_CAN_SEND);
		
		message.peer.acknowledgedMessage(); // TODO DHT: (review) How can we be sure this is the same instance of peer as in the routing table?
		for(DHTMessageStub request : pendingRequests) {
			if(request.dispatchResponseIfMatches(message)) {
				routingTable.markFresh(message.peer);
				break;
			}
		}
	}

	protected void processRequest(DHTMessage message) throws ProtocolViolationException {
		updateStatus(STATUS_GOOD);
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
		
		message.makeResponse(routingTable.closestPeers(id, DHTSearchOperation.MAX_RESULTS)).send();
	}
	
	protected void processRequestGetRecords(DHTMessage message) throws ProtocolViolationException {
		assertRequiredState(message.payload.length == idLength());
		DHTID id = new DHTID(message.payload);
		message.makeResponse(store.recordsForId(id)).send();
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
		DHTRecord record = DHTRecord.deserializeRecord(crypto, buf);
		assertSupportedState(record != null);
		assertRequiredState(record.isValid());
		
		try {
			store.addRecordForId(id, record);
		} catch(IOException exc) {
			logger.error("Encountered exception adding record from {}", message.peer.address, exc);
		}
	}
	
	protected boolean validAuthTag(DHTPeer peer, byte[] tag) {
		return Arrays.equals(peer.localAuthTag(), tag);
	}
	
	protected Key clientInfoKey() {
		return storageKey.derive(KEY_INDEX_CLIENT_INFO, new byte[0]);
	}
	
	protected Key recordStoreKey() {
		return storageKey.derive(KEY_INDEX_RECORD_STORE, new byte[0]);
	}
	
	protected Key routingTableKey() {
		return storageKey.derive(KEY_INDEX_ROUTING_TABLE, new byte[0]);
	}
	
	protected String path() {
		return "dht-client-info";
	}
	
	protected DHTMessage pingMessage(DHTPeer recipient, DHTMessageCallback callback) {
		return new DHTMessage(recipient, DHTMessage.CMD_PING, new byte[0], callback);
	}
	
	protected DHTMessage findNodeMessage(DHTPeer recipient, DHTID id, DHTMessageCallback callback) {
		return new DHTMessage(recipient, DHTMessage.CMD_FIND_NODE, id.rawId, callback);
	}
	
	protected DHTMessage getRecordsMessage(DHTPeer recipient, DHTID id, DHTMessageCallback callback) {
		return new DHTMessage(recipient, DHTMessage.CMD_GET_RECORDS, id.rawId, callback);
	}
	
	protected DHTMessage addRecordMessage(DHTPeer recipient, DHTID id, DHTRecord record, DHTMessageCallback callback) {
		byte[] serializedRecord = record.serialize();
		ByteBuffer buf = ByteBuffer.allocate(id.rawId.length + serializedRecord.length + recipient.remoteAuthTag.length);
		buf.put(recipient.remoteAuthTag);
		buf.put(id.rawId);
		buf.put(record.serialize());
		return new DHTMessage(recipient, DHTMessage.CMD_ADD_RECORD, buf.array(), callback);
	}
	
	protected DHTRecord deserializeRecord(ByteBuffer serialized) throws UnsupportedProtocolException {
		return DHTRecord.deserializeRecord(crypto, serialized);
	}
	
	protected void initNew() {
		key = crypto.makePrivateDHKey();
		tagKey = new Key(crypto, crypto.makeSymmetricKey());
		id = new DHTID(key.publicKey());
		assert(id.rawId.length == idLength());
		
		try {
			write();
		} catch (IOException exc) {
			logger.error("Encountered exception writing DHT client info", exc);
		}
	}
	
	protected void read() {
		MutableSecureFile file = MutableSecureFile.atPath(storage, path(), clientInfoKey());
		try {
			deserialize(ByteBuffer.wrap(file.read()));
		} catch(IOException exc) {
			initNew();
		}
	}
	
	protected void write() throws IOException {
		MutableSecureFile file = MutableSecureFile.atPath(storage, path(), clientInfoKey());
		file.write(serialize(), 0);
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(key.getBytes().length + key.publicKey().getBytes().length + tagKey.getRaw().length);
		buf.put(key.getBytes());
		buf.put(key.publicKey().getBytes());
		buf.put(tagKey.getRaw());
		assert(buf.remaining() == 0);
		return buf.array();
	}
	
	protected void deserialize(ByteBuffer serialized) throws EINVALException {
		try {
			byte[] privKeyRaw = new byte[crypto.asymPrivateDHKeySize()];
			byte[] pubKeyRaw = new byte[crypto.asymPublicDHKeySize()];
			byte[] tagKeyRaw = new byte[crypto.symKeyLength()];
			
			serialized.get(privKeyRaw);
			serialized.get(pubKeyRaw);
			serialized.get(tagKeyRaw);
			
			key = crypto.makePrivateDHKeyPair(privKeyRaw, pubKeyRaw);
			tagKey = new Key(crypto, tagKeyRaw);
			id = new DHTID(key.publicKey());
		} catch(BufferUnderflowException exc) {
			throw new EINVALException(path());
		}
	}
	
	protected synchronized void updateStatus(int newStatus) {
		if(lastStatus == newStatus) return;
		lastStatus = newStatus;
		if(statusCallback != null) {
			statusCallback.dhtStatusUpdate(newStatus);
		}
	}
	
	protected void assertSupportedState(boolean state) throws UnsupportedProtocolException {
		if(!state) throw new UnsupportedProtocolException();
	}
	
	protected void assertRequiredState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}
}
