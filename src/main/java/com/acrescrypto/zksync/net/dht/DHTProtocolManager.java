package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
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

/** Contains the logic for processing DHT messages. Owned by DHTClient. */
public class DHTProtocolManager {
	protected class RecentSaltSet {
		HashSet<Long> set;
		long          timeStarted;
		
		public RecentSaltSet() {
			this.timeStarted = Util.currentTimeMillis();
			this.set         = new HashSet<>();
		}
		
		public long serviceLifeMs() {
			return client.getMaster().getGlobalConfig().getLong("net.dht.maxTimestampDelta")
			     - client.getMaster().getGlobalConfig().getLong("net.dht.minTimestampDelta");
		}
		
		public boolean isExpired() {
			return Util.currentTimeMillis() - timeStarted >   serviceLifeMs();
		}
		
		public boolean isDeletable() {
			return Util.currentTimeMillis() - timeStarted > 2*serviceLifeMs();
		}
		
		public boolean addNewSalt(Long salt) {
			if(set.contains(salt)) return false;
			
			set.add(salt);
			return true;
		}
	}
	
	protected DHTClient                      client;
	protected ArrayList<DHTMessageStub>      pendingRequests   = new ArrayList<>();
	protected LinkedList<DHTSearchOperation> pendingOperations = new LinkedList<>();
	protected boolean                        autofind          = true,
			                                 initialized       = false;
	protected LinkedList<RecentSaltSet>      recentSaltSets    = new LinkedList<>();

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
		try {
			return new DHTPeer(client,
					client.getSocketManager().getSocketAddress(),
					client.getSocketManager().getPort(),
					client.getPublicKey().getBytes()
				);
		} catch(UnknownHostException exc) {
			// This shouldn't happen when getting the local address of our own socket
			logger.error("DHT -: Caught exception fetching local peer", exc);
			throw new RuntimeException(exc);
		}
	}
	
	protected DHTMessage pingMessage(DHTPeer recipient, DHTMessageCallback callback) {
		return new DHTMessage(recipient, DHTMessage.CMD_PING, new byte[0], callback);
	}
	
	/** Discover new peers on the network. This works by doing a lookup of the client's own ID as a value in the DHT. DHT peers
	 * encountered during this operation will be used to populate the routing table. */
	public void findPeers() {
		if(!client.getSocketManager().isListening()) return;
		Key lookupKey = new Key(client.getMaster().getCrypto()); // Because we're not actually trying to store or retrieve records, we can set the lookup key to any random value.
		
		logger.debug("DHT -: Finding peers...");
		addOperation(new DHTSearchOperation(client, client.getId(), lookupKey, (op, peers)->{
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
	
	/** Automatically execute findPeers() operation periodically. The time interval is configurable as net.dht.autoFindPeersIntervalMs. */
	public void autoFindPeers() {
		new Thread(client.getThreadGroup(), ()->{
			Util.setThreadName("DHTClient autoFindPeers");
			logger.debug("DHT -: Starting autoFindPeers thread");
			while(!client.isClosed()) {
				/* Let's break this blockOnPoll call down.
				 * 
				 * UNBLOCK IF THE DHTCLIENT IS CLOSED. We want to terminate this thread if that happens. 
				 * 
				 * BLOCK IF THE ROUTING TABLE IS EMPTY. findPeers only works if we have 1 or more peers already; if the table
				 * is empty, we have to wait for it to get bootstrapped (i.e. initialized with starting peers from some non-DHT data source).
				 * 
				 * BLOCK IF THE SOCKET IS NOT OPEN. If the DHTClient is paused, or if it hasn't finished initializing the socket, we can't do
				 * any network operations, so obviously we don't want to call findPeers.
				 * 
				 * BLOCK IF AUTOFIND IS DISABLED. Obviously, we don't want to call findPeers automatically if autofind is disabled. :)
				 * 
				 * TODO: 2023-04-26 6105c0a7 autofind is only ever altered in test cases. Add a config variable, or base it on net.dht.autoFindPeersIntervalMs > 0. Also, why even have this thread running when autofind is false? 
				*/
				Util.blockOnPoll(
						() ->
							!client.isClosed() && (
									 client.getRoutingTable().allPeers().isEmpty() 
								 || !client.getSocketManager().isListening()
							     || !autofind
							)
					);
				if(!client.isClosed()) {
					findPeers();
					// config: net.dht.autoFindPeersIntervalMs (int) -> Number of milliseconds between automatic invocations of findPeers().
					int autoFindPeersIntervalMs = client.getMaster().getGlobalConfig().getInt("net.dht.autoFindPeersIntervalMs");
					Util.sleep(autoFindPeersIntervalMs);
				}
			}
			logger.debug("DHT -: Stopping autoFindPeers thread");
		}).start();
	}
	
	/** Look up an identifier in the network, and invoke a supplied callback with resulting records as they are discovered.
	 * 
	 * @param searchId Identifier to locate in DHT.
	 * @param lookupKey Key associated with this identifier; peer responses will not contain records unless we authenticate our request using the matching lookupKey for those records.
	 * @param callback Invoked with resulting DHTRecords as they are received. This method is invoked with a single DHTRecord at a time, and the search operation may still be ongoing at the time of its invocation. When the search operation is complete, the callback is invoked with a null value, and no further invocations will be made. 
	 */
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
	
	/** Insert a record to the DHT by discovering the appropriate peers in the network and issuing addRecord requests to them.
	 * Adds record to local record store if the local DHTClient happens to be an appropriate peer.
	 * 
	 * @param searchId ID for the record we wish to store.
	 * @param lookupKey Lookup key for this record. Clients attempting to query for this record will not be given copies unless their request demonstrates knowledge of this key. 
	 * @param record The DHTRecord to store into the DHT.
	 */
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
					// We are one of the peers that needs to store this record
					try {
						byte[] token = lookupKey.authenticate(Util.concat(searchId.serialize(), peer.key.getBytes()));
						client.getRecordStore().addRecordForId(searchId, token, record);
					} catch (IOException exc) {
						logger.error("DHT {}:{}: Encountered exception adding record from peer, record search id {}",
								peer.address,
								peer.port,
								Util.bytesToHex(searchId.serialize()),
								exc);
					}
				} else {
					peer.addRecord(searchId, lookupKey, record);
				}
			}
			
			// TODO: 2023-04-26 would be nice to have a callback here that notifies our caller of the list of peers we actually stored with.
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
	
	/** Construct a FIND_NODE message for a specific recipient and (id, lookupKey) pair. */
	protected DHTMessage findNodeMessage(DHTPeer recipient, DHTID id, Key lookupKey, DHTMessageCallback callback) {
		ByteBuffer buf = ByteBuffer.allocate(id.getLength() + lookupKey.getCrypto().hashLength());
		buf.put(id.serialize());
		buf.put(lookupKey.authenticate(Util.concat(id.serialize(), recipient.key.getBytes())));
		
		/* lookupKey is used to authenticate a binary string containing both the ID we're trying to look up, as well as the recipient public
		 * key. This way, DHT peers holding records do not have sufficient knowledge to go perform their own search for this (id, lookupKey)
		 * pair. */
		
		return new DHTMessage(recipient, DHTMessage.CMD_FIND_NODE, buf.array(), callback);
	}
	
	/** Construct an ADD_RECORD message for a specific recipient, (id, lookupKey) pair and record. */
	protected DHTMessage addRecordMessage(DHTPeer recipient, DHTID id, Key lookupKey, DHTRecord record, DHTMessageCallback callback) {
		byte[] serializedRecord = record.serialize();
		ByteBuffer buf = ByteBuffer.allocate(
				   id.getLength()
				 + client.getMaster().getCrypto().hashLength()
				 + serializedRecord.length);
		buf.put(id.serialize());
		buf.put(lookupKey.authenticate(Util.concat(id.serialize(), recipient.key.getBytes())));
		buf.put(record.serialize());
		return new DHTMessage(recipient, DHTMessage.CMD_ADD_RECORD, buf.array(), callback);
	}

	/** Add an operation to the list of pending search operations, and execute. */
	protected synchronized void addOperation(DHTSearchOperation op) {
		pendingOperations.add(op);
		op.run();
	}
	
	/** Remove an operation from the list of pending search operations. */
	protected synchronized void finishedOperation(DHTSearchOperation op) {
		pendingOperations.remove(op);
	}
	
	/** Cancel all pending search operations. Any future responses received for these messages will not be processed, nor will
	 * further callback invocations will be made for the cancelled messages.  */
	protected synchronized void cancelOperations() {
		for(DHTSearchOperation op : pendingOperations) {
			op.cancel();
		}
		
		pendingOperations.clear();
	}
	
	/** Add a request to the list of pending requests. Requests must be registered here in order to have responses processed. */
	protected synchronized void watchForResponse(DHTMessage message) {
		pendingRequests.add(new DHTMessageStub(message));
	}
	
	/** Indicate that an expected response was not received. This will increment the missedMessage count for the associated peer,
	 * which may result in its removal from the routing table. If a response is later received for this message, it will be ignored. */
	protected void missedResponse(DHTMessageStub stub) {
		stub.msg.peer.missedMessage();
		stopWatchingForResponse(stub);
	}
	
	/** Remove a request from the list of pending requests. Further responses for this message will be ignored. */
	protected synchronized void stopWatchingForResponse(DHTMessageStub stub) {
		pendingRequests.remove(stub);
	}
	
	/** Process a raw incoming encrypted packet from the listen socket. */
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
			// these are problems that have a reasonable explanation, e.g. something weird is happening on our end, so just ignore it
			logger.info("DHT {}:{}: Received suspicious message; ignoring.",
					senderAddress,
					senderPort,
					exc);
		} catch(ProtocolViolationException exc) {
			// these are problems that should not happen with a properly-implemented peer, so ignore messages from this peer in case this is some sort of attack
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
			// could be a garbled message; just ignore it
			logger.warn("DHT {}:{}: Received indecipherable message; ignoring.",
					senderAddress,
					senderPort);
		}
	}
	
	protected void processResponse(DHTMessage message) throws ProtocolViolationException {
		if(client.getStatus() < DHTClient.STATUS_CAN_REQUEST) {
			// Now we know we can issue DHT requests and receive responses.
			client.updateStatus(DHTClient.STATUS_CAN_REQUEST);
		}
		
		logger.debug("DHT {}:{}: Received response to message {}",
				message.peer.address,
				message.peer.port,
				String.format("%08x", message.msgId));
		
		// If the message has a DHTPeer instance other than an equivalent one that is actually in our routing table or recent peers list, go use the existing one instead. 
		DHTPeer canonPeer = client.getRoutingTable().canonicalPeer(message.peer);
		message.peer = canonPeer;
		message.peer.acknowledgedMessage(); // Mark that we have successful communication with this peer, which will help them stay in our routing table.
		
		// Go get the message stub holding the request we previously sent out that this response is in reply to.
		DHTMessageStub stub = null;
		synchronized(this) {
			for(DHTMessageStub request : pendingRequests) {
				if(request.matchesMessage(message)) {
					stub = request;
					break;
				}
			}
		}
		
		// Oh no, we don't have a stub! Maybe we cancelled this message. Ignore it.
		if(stub == null) {
			logger.debug("DHT {}:{}: Ignoring response to message {} since we do not have a stub for it",
					message.peer.address,
					message.peer.port,
					String.format("%08x", message.msgId));
			return;
		}
		
		stub.msg.peer = canonPeer; // make sure the stub also references the same DHTPeer instance that's in the routing table/recent peers list
		stub.dispatchResponse(message); // go do whatever logic we have to do for this message type
		client.getRoutingTable().markFresh(message.peer);
	}

	protected void processRequest(DHTMessage message) throws ProtocolViolationException {
		/* TODO: 2023-04-26 6105c0a
		 * We mark STATUS_GOOD here on the theory that we just received an unsolicited request, so that means we can handle incoming
		 * traffic, right? NOT SO FAST! We might be behind a firewall with a NAT table entry that lets that specific peer send us messages
		 * for now, but not necessarily other remote peers. So check out this situation:
		 * 
		 * Peer A is a bootstrap peer and freely receives DHT traffic on 1.2.3.4:20000.
		 * 
		 * Peer B joins the DHT network and begins by issuing a FIND_NODE to peer A.
		 * Peer B's local IP address is 192.168.1.10, and its local UDP port is 30000.
		 * Peer B is behind a NAT firewall F, which assigns random external UDP port 40000 to B's outgoing FIND_NODE packet, so the packet comes from 4.3.2.1:40000.
		 * Firewall F also adds a NAT table entry forwarding packets coming to 4.3.2.1:40000 from 1.2.3.4:20000 to 192.168.1.10:30000.  
		 * 
		 * Peer A receives the FIND_NODE request, and sends an appropriate response to 4.3.2.1:40000 from 1.2.3.4:20000. Peer A also adds B to its routing table.
		 * Firewall F receives the response, finds the NAT table maps it to 192.168.1.10:30000, and forwards the packet to Peer A appropriately.
		 * Peer A receives the packet and marks itself as STATUS_CAN_REQUEST.
		 * 
		 * Peer A sends a ping request to Peer B. This request also goes to 4.3.2.1:40000 from 1.2.3.4:20000, per A's routing table.
		 * Firewall F receives the request, finds the NAT table rule for 1.2.3.4:20000 -> 4.3.2.1:40000 ==> 192.168.1.10:30000, and forward the request.
		 * Peer A receives the unsolicited request, and marks itself STATUS_GOOD as per the current logic of this method.
		 * 
		 * Peer C joins the network. It has IP 100.1.1.1, port 50000.
		 * Peer C discovers Peer B via Peer A. Peer C sends a FIND_NODE to Peer A at 4.3.2.1:40000.
		 * Firewall F receives C's packet, and notes there is no rule for 100.1.1.1:50000 -> 4.3.2.1.:40000. The packet is dropped.
		 * 
		 * In other words, Peer A marked itself as STATUS_GOOD, when in actuality it should have STATUS_CAN_RECEIVE.
		 * 
		 * Off the top of my head, it seems like STATUS_GOOD should only be marked when this is a peer we have never attempted to
		 * communicate with before, which would demonstrate that there is no such NAT tomfoolery afoot. However, it would also mean
		 * that there is no request we can initiate that would cause us to move to STATUS_GOOD -- we would just have to wait until
		 * a previously-unknown peer chooses to initiate contact with us.
		 */
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
			// Unknown message IDs are quietly dropped. Blacklisting here would make forwards compatibility very difficult.
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
		/* Supported message format is the ID the peer wants to look up, and a binary token that is presumed to be the
		 * hash of that ID with our public key keyed with the lookup key. (We don't actually have the lookup key, so we have
		 * no way of knowing if that's true.) If there's more or less bytes, this might be a different protocol version, and
		 * we'll just ignore it. */
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
		
		DHTID id                         = DHTID.withBytes(idBytes);
		
		// config: net.dht.maxResults (int) -> Maximum number of result records to be included in DHT FIND_NODE responses.
		/* Our response consists of 2 lists:
		**   - The closest peers in our routing table to the requested ID (to a maximum count of net.dht.maxResults)
		**   - Any records that have been previously stored with us in an ADD_RECORD command with the same ID and token 
		*/
		Collection<DHTPeer> closestPeers = client.getRoutingTable().closestPeers(
				id,
				client.getMaster().getGlobalConfig().getInt("net.dht.maxResults")
			); 
		DHTMessage response = message.makeResponse(closestPeers);
		response.addItemList(client.getRecordStore().recordsForId(id, token));
		response.send();
	}
	
	protected void processRequestAddRecord(DHTMessage message) throws ProtocolViolationException, UnsupportedProtocolException {
		/* Supported message format is:
		 *   - the ID the peer wants to store for
		 *   - a binary token that is presumed to be the hash of that ID with our public key keyed with the lookup key. (We don't actually have the lookup key, so we have no way of knowing if that's true.)\
		 *   - an actual valid record to be stored   
		 **/
		
		ByteBuffer buf = ByteBuffer.wrap(message.payload);
		
		assertSupportedState(validAuthTag(message.peer, message.authTag));
		
		byte[] idRaw = new byte[client.idLength()];
		byte[] token = new byte[client.getMaster().getCrypto().hashLength()];
		assertSupportedState(buf.remaining() > idRaw.length + token.length);		
		buf.get(idRaw);
		buf.get(token);
		DHTID id = DHTID.withBytes(idRaw);

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
	
	protected boolean needsNewSaltSet() {
		return recentSaltSets.isEmpty()
			|| recentSaltSets.getLast().isExpired();
	}
	
	protected void refreshSaltSets() {
		if(!needsNewSaltSet()) return;
		
		synchronized(this) {
			if(!needsNewSaltSet()) return;
			recentSaltSets.removeIf((set)->set.isDeletable());
			recentSaltSets.add(new RecentSaltSet());
		}
	}
	
	public synchronized boolean recordMessageRnd(byte[] salt) {
		long salt64 = ByteBuffer.wrap(salt).getLong();
		refreshSaltSets();
		return recentSaltSets.getLast().addNewSalt(salt64);
	}
}
