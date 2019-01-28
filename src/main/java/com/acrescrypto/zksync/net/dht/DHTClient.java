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
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionToken;
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageCallback;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.Util;
import com.dosse.upnp.UPnP;

public class DHTClient {
	public final static int AUTH_TAG_SIZE = 4;
	public final static int MAX_DATAGRAM_SIZE = 508; // 576 byte (guaranteed by RFC 791) - 60 byte IP header - 8 byte UDP header
	
	public final static int DEFAULT_LOOKUP_RESULT_MAX_WAIT_TIME_MS = 500;
	public static int lookupResultMaxWaitTimeMs = DEFAULT_LOOKUP_RESULT_MAX_WAIT_TIME_MS; // consider a lookup finished if we've received nothing in this many milliseconds
		
	public final static int DEFAULT_MESSAGE_EXPIRATION_TIME_MS = 5000; // how long after first send attempt before we count a message as expired if no response received
	public static int messageExpirationTimeMs = DEFAULT_MESSAGE_EXPIRATION_TIME_MS;
	
	public final static int DEFAULT_MESSAGE_RETRY_TIME_MS = 2000;
	public static int messageRetryTimeMs = DEFAULT_MESSAGE_RETRY_TIME_MS;
	
	public final static int DEFAULT_SOCKET_OPEN_FAIL_CYCLE_DELAY_MS = 9000;
	public static int socketOpenFailCycleDelayMs = DEFAULT_SOCKET_OPEN_FAIL_CYCLE_DELAY_MS;
	
	public final static int DEFAULT_SOCKET_CYCLE_DELAY_MS = 1000;
	public static int socketCycleDelayMs = DEFAULT_SOCKET_CYCLE_DELAY_MS;
	
	public final static int DEFAULT_AUTO_FIND_PEERS_INTERVAL_MS = 1000*60*15;
	public static int autoFindPeersIntervalMs = DEFAULT_AUTO_FIND_PEERS_INTERVAL_MS;
	
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

	protected BandwidthMonitor monitorTx, monitorRx;
	protected DatagramSocket socket;
	protected Blacklist blacklist;
	protected ZKMaster master;
	protected DHTRecordStore store;
	protected CryptoSupport crypto;
	protected DHTID id;
	protected DHTRoutingTable routingTable;
	protected PrivateDHKey key;
	protected Key storageKey, tagKey;
	protected String bindAddress;
	protected Thread socketListenerThread;
	protected ThreadGroup threadGroup;
	protected GroupedThreadPool threadPool;
	protected boolean closed, paused, initialized;
	protected DHTStatusCallback statusCallback;
	protected int bindPort;
	protected int lastStatus = STATUS_OFFLINE;
	protected byte[] networkId;
	protected FS storage;
	protected LinkedList<SubscriptionToken<?>> subscriptions = new LinkedList<>();
	
	protected ArrayList<DHTMessageStub> pendingRequests;
	
	public DHTClient(Key storageKey, ZKMaster master) {
		this(storageKey, master, new byte[storageKey.getCrypto().hashLength()]);
	}
	
	public DHTClient(Key storageKey, ZKMaster master, byte[] networkId) {
		this.master = master;
		this.threadGroup = new ThreadGroup(master.getThreadGroup(), "DHTClient");
		this.threadPool = GroupedThreadPool.newCachedThreadPool(threadGroup, "DHTClient Pool");
		this.blacklist = master.getBlacklist();
		this.storageKey = storageKey;
		this.storage = master.getStorage();
		this.crypto = storageKey.getCrypto();
		this.pendingRequests = new ArrayList<>();
		this.networkId = networkId;
		
		this.monitorTx = new BandwidthMonitor(master.getBandwidthMonitorTx());
		this.monitorRx = new BandwidthMonitor(master.getBandwidthMonitorRx());
		
		read();

		this.store = new DHTRecordStore(this);
		this.routingTable = new DHTRoutingTable(this);
		
		setupSubscriptions();
		
		if(master.getGlobalConfig().getBool("net.dht.enabled", false)) {
			String addr = master.getGlobalConfig().getString("net.dht.bindaddress", "0.0.0.0");
			int port = master.getGlobalConfig().getInt("net.dht.port", 0);
			try {
				listen(addr, port);
			} catch (SocketException exc) {
				logger.error("DHT: Unable to set up DHT socket on " + addr + ":" + port, exc);
			}
		}
		
		if(master.getGlobalConfig().getBool("net.dht.bootstrap.enabled", false)) {
			addDefaults();
		}
	}
	
	protected DHTClient() {}
	
	protected void setupSubscriptions() {
		subscriptions.add(master.getGlobalConfig().subscribe("net.dht.enabled").asBoolean(false, (enabled)->{
			if(closed || enabled == isListening()) return;
			if(enabled) {
				try {
					listen(bindAddress, master.getGlobalConfig().getInt("net.dht.port", 0));
				} catch (SocketException exc) {
					logger.error("DHT: Unable to open DHT socket", exc);
				}
			} else {
				pause();
			}
		}));
		
		subscriptions.add(master.getGlobalConfig().subscribe("net.dht.port").asInt(0, (port)->{
			synchronized(this) {
				if(isListening() && getPort() != port) {
					bindPort = port;
					try {
						openSocket();
					} catch (SocketException exc) {
						logger.error("DHT: Unable to open DHT socket when rebinding to port " + port, exc);
					}
				}
			}
		}));
		
		subscriptions.add(master.getGlobalConfig().subscribe("net.dht.upnp").asBoolean(false, (enabled)->{
			if(enabled) {
				checkUPnP();
			} else if(getPort() > 0 && UPnP.isMappedUDP(getPort())) {
				UPnP.closePortTCP(getPort());
			}
		}));
		
		subscriptions.add(master.getGlobalConfig().subscribe("net.dht.bootstrap.enabled").asBoolean(false, (useBootstrap)->{
			if(useBootstrap) {
				addDefaults();
			} else {
				logger.warn("Wiping routing table since net.dht.bootstrap.enabled set false");
				routingTable.reset();
			}
		}));
	}
	
	protected void addDefaults() {
		String defaultHost = master.getGlobalConfig().getString("net.dht.bootstrap.host",
				"dht1.easysafe.io");
		int defaultPort = master.getGlobalConfig().getInt("net.dht.bootstrap.port",
				49921);
		String defaultKey = master.getGlobalConfig().getString("net.dht.bootstrap.key",
				"M/o1rvmhAsQO8+Z5evXJQ+21/sk2fxei4JKl+h1SPU5rKLNWRfnUyrxVAGxBK1Ydl2RQGoW+CZLKQRbZS+WLrw==");
		
		PublicDHKey pubkey = crypto.makePublicDHKey(Util.decode64(defaultKey));
		
		try {
			addPeer(new DHTPeer(this,
					InetAddress.getByName(defaultHost).getHostAddress(),
					defaultPort,
					pubkey));
			logger.info("DHT: Added bootstrap peer: " + defaultHost + ":" + defaultPort + " " + defaultKey);
		} catch (UnknownHostException exc) {
			logger.error("DHT: Unable to resolve " + defaultHost, exc);
		}
	}
	
	protected boolean isListening() {
		return !paused && socket != null;
	}
	
	public DHTClient listen(String address, int port) throws SocketException {
		closed = paused = false;
		
		this.bindAddress = address == null ? master.getGlobalConfig().getString("net.dht.bindaddress", "0.0.0.0") : address;
		this.bindPort = port;
		openSocket();

		if(socketListenerThread == null || !socketListenerThread.isAlive()) {
			socketListenerThread = new Thread(threadGroup, ()->socketListener());
			socketListenerThread.start();
		}
		
		return this;
	}
	
	public int getPort() {
		if(socket == null) return -1;
		return socket.getLocalPort();
	}
	
	public DHTPeer getLocalPeer() {
		return new DHTPeer(this, bindAddress, getPort(), key.publicKey().getBytes());
	}
	
	public DHTClient setStatusCallback(DHTStatusCallback statusCallback) {
		this.statusCallback = statusCallback;
		return this;
	}
	
	public void findPeers() {
		new DHTSearchOperation(this, id, new Key(crypto), (peers)->{
			// no need to do anything; just doing the search populates the routing table
			if(peers == null || peers.isEmpty()) {
				updateStatus(STATUS_QUESTIONABLE);
			}
			
			initialized = true;
		}, (record)->{
			// just ignore records here
		}).run();
	}
	
	public void autoFindPeers() {
		new Thread(threadGroup, ()->{
			Util.setThreadName("DHTClient autoFindPeers");
			while(!paused) {
				Util.blockOn(()->routingTable.allPeers().isEmpty());
				findPeers();
				Util.sleep(autoFindPeersIntervalMs);
			}
		}).start();
	}
	
	public void lookup(DHTID searchId, Key lookupKey, LookupCallback callback) {
		new DHTSearchOperation(this, searchId, lookupKey, (peers)->{
			if(peers == null || peers.isEmpty()) {
				updateStatus(STATUS_QUESTIONABLE);
			}
			
			callback.receivedRecord(null); // we won't be getting any more records, so signal end of results
		}, (record)->{
			callback.receivedRecord(record);
		}).run();
	}
	
	public void addPeer(DHTPeer peer) {
		routingTable.suggestPeer(peer);
	}
	
	public void addRecord(DHTID searchId, Key lookupKey, DHTRecord record) {
		new DHTSearchOperation(this, searchId, lookupKey, (peers)->{
			if(peers == null || peers.isEmpty()) {
				updateStatus(STATUS_QUESTIONABLE);
			}
			
			for(DHTPeer peer : peers) {
				if(Arrays.equals(key.publicKey().getBytes(), peer.key.getBytes())) {
					try {
						byte[] token = lookupKey.authenticate(Util.concat(searchId.rawId, peer.key.getBytes()));
						store.addRecordForId(searchId, token, record);
					} catch (IOException exc) {
						logger.error("DHT: Encountered exception adding record", exc);
					}
				} else {
					peer.addRecord(searchId, lookupKey, record);
				}
			}
		}, (existingRecord)->{
			// don't need to do anything with existing records here
		}).run();
	}
	
	public int idLength() {
		return crypto.hashLength();
	}
	
	public void pause() {
		paused = true;
		int port = getPort();
		
		if(socket != null) {
			socket.close();
		}

		if(port > 0 && master.getGlobalConfig().getBool("net.dht.upnp", false)) {
			UPnP.closePortUDP(port);
		}
	}
	
	public void close() {
		pause();
		closed = true;
		threadPool.shutdownNow();
		routingTable.close();
		
		for(SubscriptionToken<?> token : subscriptions) {
			token.close();
		}
		
		subscriptions.clear();
	}
	
	public boolean isInitialized() {
		return initialized;
	}
	
	public void dump() {
		System.out.println("DHTClient " + this + " initialized=" + initialized);
		routingTable.dump();
		store.dump();
	}
	
	protected void openSocket() throws SocketException {
		if(paused) return;
		InetAddress addr;
		try {
			addr = InetAddress.getByName(bindAddress);
		} catch(UnknownHostException exc) {
			throw new RuntimeException("Unable to bind to address " + bindAddress + ":" + bindPort + ": unable to resolve address");
		}
		
		updateStatus(STATUS_ESTABLISHING);
		if(socket != null && !socket.isClosed()) {
			int oldPort = socket.getLocalPort();
			socket.close();
			if(oldPort != bindPort && master.getGlobalConfig().getBool("net.dht.upnp", false)) {
				logger.info("DHT: Closing UPnP for DHT on UDP port " + getPort());
				UPnP.closePortUDP(oldPort);
			}
		}
		
		try {
			socket = new DatagramSocket(bindPort, addr);
			socket.setReuseAddress(true);
			if(socket.getLocalPort() != master.getGlobalConfig().getInt("net.dht.port", 0)) {
				master.getGlobalConfig().set("net.dht.port", socket.getLocalPort());
			}
			logger.info("DHT: listening on UDP port " + getPort());
			checkUPnP();
			updateStatus(STATUS_QUESTIONABLE);
		} catch(SocketException exc) {
			updateStatus(STATUS_OFFLINE);
			throw exc;
		}
	}
	
	protected void checkUPnP() {
		if(master.getGlobalConfig().getBool("net.dht.upnp", false) && socket != null && !paused) {
			logger.info("DHT: Requesting UPnP for DHT on UDP port " + getPort());
			UPnP.openPortUDP(getPort());
		}
	}
	
	protected void socketListener() {
		Util.setThreadName("DHTClient socketListener " + Util.bytesToHex(key.publicKey().getBytes(), 4) + " " + getPort());
		int lastPort = -1;
		
		while(!paused) {
			try {
				if(socket == null) {
					Util.sleep(10);
					continue;
				}
				
				lastPort = socket.getLocalPort();
				
				byte[] receiveData = new byte[MAX_DATAGRAM_SIZE];
				DatagramPacket packet = new DatagramPacket(receiveData, receiveData.length);
				monitorRx.observeTraffic(packet.getLength());
				socket.receive(packet);
				logger.debug("DHT: received {} bytes from {}:{}",
						packet.getLength(),
						packet.getAddress().getHostAddress(),
						packet.getPort());
				ByteBuffer buf = ByteBuffer.wrap(packet.getData(), 0, packet.getLength());
				processMessage(packet.getAddress().getHostAddress(), packet.getPort(), buf);
			} catch(IOException exc) {
				if(paused) return;
				if(socket.getLocalPort() == lastPort && !socket.isClosed()) {
					logger.error("DHT: socket listener thread encountered IOException", exc);
				} else {
					logger.warn("DHT: socket listener thread encountered IOException", exc);
				}
				
				Util.sleep(socketCycleDelayMs); // add in a delay to prevent a fail loop from gobbling CPU / spamming log
				try {
					openSocket();
				} catch (SocketException e) {
					logger.error("DHT: socket listener thread encountered IOException rebinding socket", exc);
					Util.sleep(socketOpenFailCycleDelayMs); // wait even longer if we know the socket is dead and the OS isn't giving it back
				}
			} catch(Exception exc) {
				logger.error("DHT: socket listener thread encountered exception", exc);
			}
		}
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
	
	protected synchronized void sendDatagram(DatagramPacket packet) {
		if(paused) return;
		for(int i = 0; i < 2; i++) {
			try {
				logger.debug("DHT: sending {} bytes to {}:{}",
						packet.getData().length,
						packet.getAddress().getHostAddress(),
						packet.getPort());
				socket.send(packet);
				monitorTx.observeTraffic(packet.getLength());
				break;
			} catch (IOException exc) {
				// TODO API: (coverage) exception
				if(paused) return;
				if(i == 0) {
					logger.warn("DHT: Encountered exception sending on DHT socket; retrying", exc);
					try {
						openSocket();
					} catch (SocketException exc2) {
						logger.error("DHT: Encountered exception rebinding DHT socket; giving up on sending message", exc2);
						return;
					}
				} else {
					logger.error("DHT: Encountered exception sending on DHT socket; giving up", exc);
				}
			}
		}
	}
	
	protected void processMessage(String senderAddress, int senderPort, ByteBuffer data) {
		try {
			DHTMessage message = new DHTMessage(this, senderAddress, senderPort, data);
			
			if(blacklist.contains(senderAddress)) {
				logger.info("DHT: Ignoring message from blacklisted peer " + senderAddress);
				return;
			}
			
			if(message.isResponse()) {
				processResponse(message);
			} else {
				processRequest(message);
			}
		} catch(ProtocolViolationException exc) {
			logger.warn("DHT: Received illegal message from " + senderAddress + "; blacklisting.");
			try {
				blacklist.add(senderAddress, Blacklist.DEFAULT_BLACKLIST_DURATION_MS);
			} catch(IOException exc2) {
				logger.error("DHT: Encountered exception while blacklisting peer {}", senderAddress, exc2);
			}
		} catch(SecurityException exc) {
			logger.warn("DHT: Received indecipherable message from " + senderAddress + "; ignoring.");
		}
	}
	
	protected void processResponse(DHTMessage message) throws ProtocolViolationException {
		if(lastStatus < STATUS_CAN_SEND) updateStatus(STATUS_CAN_SEND);
		
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
		routingTable.markFresh(message.peer);
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
			case DHTMessage.CMD_ADD_RECORD:
				processRequestAddRecord(message);
				break;
			default:
				throw new UnsupportedProtocolException();
			}
		} catch (UnsupportedProtocolException e) {
			logger.warn("DHT: Received unsupported message from " + message.peer.address + "; ignoring.");
		}
	}
	
	protected void processRequestPing(DHTMessage message) {
		logger.debug("DHT: received ping from {}",
				message.peer.address);

		message.makeResponse(null).send();
	}
	
	protected void processRequestFindNode(DHTMessage message) throws ProtocolViolationException, UnsupportedProtocolException {
		assertSupportedState(message.payload.length == idLength() + crypto.hashLength());
		
		ByteBuffer buf = ByteBuffer.wrap(message.payload);
		byte[] idBytes = new byte[idLength()];
		byte[] token = new byte[crypto.hashLength()];
		buf.get(idBytes);
		buf.get(token);
		
		logger.debug("DHT: received find node request from {} for id {}, token {}",
				message.peer.address,
				Util.bytesToHex(idBytes),
				Util.bytesToHex(token));
		
		DHTID id = new DHTID(idBytes);
		
		DHTMessage response = message.makeResponse(routingTable.closestPeers(id, DHTSearchOperation.maxResults));
		response.addItemList(store.recordsForId(id, token));
		response.send();
	}
	
	protected void processRequestAddRecord(DHTMessage message) throws ProtocolViolationException, UnsupportedProtocolException {
		ByteBuffer buf = ByteBuffer.wrap(message.payload);
		
		assertSupportedState(validAuthTag(message.peer, message.authTag));
		
		byte[] idRaw = new byte[idLength()];
		byte[] token = new byte[crypto.hashLength()];
		assertSupportedState(buf.remaining() > idRaw.length + crypto.hashLength());
		buf.get(idRaw);
		buf.get(token);
		DHTID id = new DHTID(idRaw);

		logger.debug("DHT: received add record request from {} for id {}, token {}",
				message.peer.address,
				Util.bytesToHex(idRaw),
				Util.bytesToHex(token));

		assertSupportedState(buf.remaining() > 0);
		DHTRecord record = DHTRecord.deserializeRecord(crypto, buf);
		assertSupportedState(record != null);
		assertSupportedState(record.isValid());
		
		try {
			store.addRecordForId(id, token, record);
		} catch(IOException exc) {
			logger.error("DHT: Encountered exception adding record from {}", message.peer.address, exc);
		}
		
		message.makeResponse(new ArrayList<>(0)).send();
	}
	
	protected boolean validAuthTag(DHTPeer peer, byte[] tag) {
		return Arrays.equals(peer.localAuthTag(), tag);
	}
	
	protected Key clientInfoKey() {
		return storageKey.derive("easysafe-dht-client-info-key");
	}
	
	protected Key recordStoreKey() {
		return storageKey.derive("easysafe-dht-record-store-key");
	}
	
	protected Key routingTableKey() {
		return storageKey.derive("easysafe-dht-routing-table-key");
	}
	
	protected String path() {
		ByteBuffer truncated = ByteBuffer.allocate(8);
		truncated.get(crypto.hash(networkId), 0, truncated.remaining());
		
		return "dht-client-info-"+Util.bytesToHex(truncated.array());
	}
	
	protected DHTMessage findNodeMessage(DHTPeer recipient, DHTID id, Key lookupKey, DHTMessageCallback callback) {
		ByteBuffer buf = ByteBuffer.allocate(id.rawId.length + lookupKey.getCrypto().hashLength());
		buf.put(id.rawId);
		buf.put(lookupKey.authenticate(Util.concat(id.rawId, recipient.key.getBytes())));
		return new DHTMessage(recipient, DHTMessage.CMD_FIND_NODE, buf.array(), callback);
	}
	
	protected DHTMessage addRecordMessage(DHTPeer recipient, DHTID id, Key lookupKey, DHTRecord record, DHTMessageCallback callback) {
		byte[] serializedRecord = record.serialize();
		ByteBuffer buf = ByteBuffer.allocate(id.rawId.length + crypto.hashLength() + serializedRecord.length);
		buf.put(id.rawId);
		buf.put(lookupKey.authenticate(Util.concat(id.rawId, recipient.key.getBytes())));
		buf.put(record.serialize());
		return new DHTMessage(recipient, DHTMessage.CMD_ADD_RECORD, buf.array(), callback);
	}
	
	// having the call happen here instead of directly accessing DHTRecord is convenient for stubbing out test classes
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
			logger.error("DHT: Encountered exception writing DHT client info", exc);
		}
	}
	
	// having these trivial message builders here might seem a little odd, but it makes it easy to stub these for test classes
	protected DHTMessage pingMessage(DHTPeer recipient, DHTMessageCallback callback) {
		return new DHTMessage(recipient, DHTMessage.CMD_PING, new byte[0], callback);
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
	
	public void purge() throws IOException {
		// TODO API: (test) test DHTClient purge
		close();
		unlinkIfExists(path());
		unlinkIfExists(store.path());
		unlinkIfExists(routingTable.path());
	}
	
	protected void unlinkIfExists(String path) throws IOException {
		try {
			storage.unlink(path);
		} catch(ENOENTException exc) {}
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
		logger.debug("DHT: status now {}, was {}", newStatus, lastStatus);
		lastStatus = newStatus;
		if(statusCallback != null) {
			statusCallback.dhtStatusUpdate(newStatus);
		}
	}
	
	public int getStatus() {
		return lastStatus;
	}
	
	public DHTRecordStore getRecordStore() {
		return store;
	}
	
	// TODO API: (test) Test DHTClient bandwidth monitors
	public BandwidthMonitor getMonitorRx() {
		return monitorRx;
	}
	
	public BandwidthMonitor getMonitorTx() {
		return monitorTx;
	}
	
	public DHTID getId() {
		return id;
	}
	
	public PublicDHKey getPublicKey() {
		return key.publicKey();
	}
	
	public byte[] getNetworkId() {
		return networkId;
	}
	
	public String getBindAddress() {
		return bindAddress;
	}
	
	public boolean isClosed() {
		return closed;
	}
	
	public boolean isPaused() {
		return paused;
	}
	
	public DHTRoutingTable getRoutingTable() {
		return routingTable;
	}
	
	public int numPendingRequests() {
		return pendingRequests.size();
	}
	
	protected void assertSupportedState(boolean state) throws UnsupportedProtocolException {
		if(!state) {
			throw new UnsupportedProtocolException();
		}
	}
	
	public String toString() {
		if(key == null) return "uninitialized DHTClient";
		return Util.bytesToHex(key.publicKey().getBytes(), 4) + " " + bindAddress + ":" + getPort();
	}
}
