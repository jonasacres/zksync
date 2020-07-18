package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.net.SocketException;
import java.util.Arrays;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionToken;
import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.Util;
import com.fasterxml.jackson.core.JsonProcessingException;

public class DHTClient {
	public final static int AUTH_TAG_SIZE = 4;
	
	public final static int KEY_INDEX_CLIENT_INFO         = 0;
	public final static int KEY_INDEX_ROUTING_TABLE       = 1;
	public final static int KEY_INDEX_RECORD_STORE        = 2;
	
	public final static int STATUS_OFFLINE                = 0; // we are clearly unable to reach the DHT network
	public final static int STATUS_ESTABLISHING           = 1; // attempting to establish connection to DHT network
	public final static int STATUS_QUESTIONABLE           = 2; // strong possibility we are not connected to network
	public final static int STATUS_CAN_REQUEST            = 3; // can send DHT requests and receive replies (but may not be able to receive requests)
	public final static int STATUS_GOOD                   = 4; // can send and receive, including unsolicited requests from peers
	
	interface PeerForReferenceCallback {
		void receivedPeerForReference(DHTPeer peer);
	}
	
	public interface LookupCallback {
		void receivedRecord(DHTRecord ad);
	}
	
	interface DHTStatusCallback {
		void dhtStatusUpdate(int status);
	}
	
	private Logger logger = LoggerFactory.getLogger(DHTClient.class);

	protected CryptoSupport                    crypto;
	protected ZKMaster                         master;
	
	protected DHTProtocolManager               protocolManager;
	protected DHTRoutingTable                  routingTable;
	protected DHTSocketManager                 socketManager;
	protected DHTRecordStore                   store;
	
	protected ThreadGroup                      threadGroup;
	protected GroupedThreadPool                threadPool;

	protected boolean                          closed;
	protected int                              lastStatus        = STATUS_OFFLINE;
	protected byte[]                           networkId;
	
	protected DHTID                            id;
	protected Key                              storageKey,
	                                           tagKey;
	protected PrivateDHKey                     privateKey;
	protected DHTStatusCallback                statusCallback;
	protected LinkedList<SubscriptionToken<?>> subscriptions     = new LinkedList<>();
	
	public DHTClient(Key storageKey, ZKMaster master) {
		this.master            = master;
		this.storageKey        = storageKey;
		this.crypto            = storageKey.getCrypto();
		this.networkId         = crypto.hash(master.getGlobalConfig().getString("net.dht.network").getBytes());
		
		this.socketManager     = new DHTSocketManager(this);
		this.protocolManager   = new DHTProtocolManager(this);
		
		this.threadGroup       = new ThreadGroup(master.getThreadGroup(), "DHTClient");
		this.threadPool        = GroupedThreadPool.newCachedThreadPool(threadGroup, "DHTClient Pool");
		
		init();
		
		this.store             = new DHTRecordStore(this);
		this.routingTable      = new DHTRoutingTable(this);
		
		setupSubscriptions();
		
		if(master.getGlobalConfig().getBool("net.dht.enabled")) {
			start();
		}
		
		if(master.getGlobalConfig().getBool("net.dht.bootstrap.enabled")) {
			bootstrap();
		}
	}
	
	protected DHTClient() {}
	
	protected void init() {
		DHTClientSerializer serializer = new DHTClientSerializer(this);
		try {
			serializer.read();
		} catch (IOException exc) {
			// unable to read stored DHT client info; set up from scratch
			privateKey = crypto.makePrivateDHKey();
			tagKey     = new Key(crypto, crypto.makeSymmetricKey());
			id         = new DHTID(getPublicKey());
			
			assert(id.rawId.length == idLength());
			
			try {
				serializer.write();
			} catch (IOException exc2) {
				logger.error("DHT -: Encountered exception writing DHT client info", exc);
			}
		}
	}
		
	protected void setupSubscriptions() {
		ConfigFile config = master.getGlobalConfig();
		
		subscriptions.add(config.subscribe("net.dht.enabled").asBoolean((enabled)->{
			if(isClosed())                             return;
			if(socketManager.isListening() == enabled) return;
			
			if(enabled) {
				try {
					listen(
							socketManager.getBindAddress(),
							master.getGlobalConfig().getInt("net.dht.port")
						);
				} catch (SocketException exc) {
					logger.error("DHT -: Unable to open DHT socket", exc);
				}
			} else {
				pause();
			}
		}));
		
		subscriptions.add(config.subscribe("net.dht.port").asInt((port)->{
			synchronized(this) {
				if(socketManager.isListening()  == false) return;
				if(socketManager.getPort()      == port)  return;
				
				socketManager.setBindPort(port);
				try {
					socketManager.openSocket();
				} catch (SocketException exc) {
					logger.error("DHT -: Unable to open DHT socket when rebinding to port {}",
							port,
							exc);
				}
			}
		}));
		
		subscriptions.add(config.subscribe("net.dht.upnp").asBoolean((enabled)->{
			socketManager.setUPnPEnabled(enabled);
		}));
		
		subscriptions.add(config.subscribe("net.dht.bootstrap.enabled").asBoolean((useBootstrap)->{
			if(useBootstrap) bootstrap();
		}));
		
		subscriptions.add(config.subscribe("net.dht.network").asString((network)->{
			byte[] newNetworkId = crypto.hash(network.getBytes());
			setNetworkId(newNetworkId);
		}));
		
		subscriptions.add(config.subscribe("net.dht.bootstrap.peerfile").asString( (host) -> bootstrap() ));
	}
	
	protected void start() {
		String addr = master.getGlobalConfig().getString("net.dht.bindaddress");
		int    port = master.getGlobalConfig().getInt   ("net.dht.port");
		
		try {
			listen(addr, port);
		} catch (SocketException exc) {
			logger.error("DHT {}:{}: Unable to set up DHT socket",
					addr,
					port,
					exc);
		}
	}
	
	protected synchronized void bootstrap() {
		if(!master.getGlobalConfig().getBool("net.dht.bootstrap.enabled")) return;
		
		routingTable.reset();
		protocolManager.pendingRequests.clear();
		protocolManager.cancelOperations();
		
		try {
			new DHTBootstrapper(this).bootstrap();
			
			/* TODO: It just isn't enough to log these errors. Clients need a way to find
			 * out in a very obvious fashion that their DHT bootstraps aren't working because
			 * they're not accessible, or aren't valid. This needs to make it all the way up
			 * to the API level.
			 * 
			 * One idea would be to make DHTBootstrapper persist as a member variable, then
			 * have it store these exceptions, and let the API return that information in
			 * GET /dht or something.
			 */
		} catch (JsonProcessingException exc) {
			logger.error("DHT -: Unable to bootstrap client, due to JSON processing exception", exc);
		} catch (IOException exc) {
			logger.error("DHT -: Unable to bootstrap client, due to IOException", exc);
		}
	}
	
	public boolean isEnabled() {
		return master.getGlobalConfig().getBool("net.dht.enabled");
	}
	
	public DHTClient listen(String address, int port) throws SocketException {
		socketManager.listen(address, port);
		protocolManager.autoFindPeers();
		return this;
	}
	
	public DHTClient setStatusCallback(DHTStatusCallback statusCallback) {
		this.statusCallback = statusCallback;
		return this;
	}
	
	public int idLength() {
		return crypto.hashLength();
	}
	
	public void pause() {
		socketManager.pause();
	}
	
	public void close() {
		pause();
		closed = true;
		threadPool.shutdownNow();
		routingTable.close();
		closeSubscriptions();
	}
	
	public void purge() throws IOException {
		close();
		new DHTClientSerializer(this).purge();
	}
	
	public boolean isInitialized() {
		return protocolManager.isInitialized();
	}
	
	public void dump() {
		System.out.println("DHTClient " + this + " initialized=" + isInitialized());
		routingTable.dump();
		store.dump();
	}
	
	public int getStatus() {
		return lastStatus;
	}
	
	public boolean isClosed() {
		return closed;
	}
	
	public boolean isPaused() {
		return socketManager.isPaused();
	}
	
	public DHTRecordStore getRecordStore() {
		return store;
	}
	
	public DHTRoutingTable getRoutingTable() {
		return routingTable;
	}
	
	public DHTSocketManager getSocketManager() {
		return socketManager;
	}
	
	public DHTProtocolManager getProtocolManager() {
		return protocolManager;
	}
	
	public DHTID getId() {
		return id;
	}
	
	public void setId(DHTID id) {
		this.id = id;
	}
	
	public PublicDHKey getPublicKey() {
		return privateKey.publicKey();
	}
	
	public byte[] getNetworkId() {
		return networkId;
	}
	
	public void setNetworkId(byte[] newNetworkId) {
		if(Arrays.equals(newNetworkId, this.networkId)) return;
		
		this.networkId = newNetworkId;
		logger.info("DHT -: Updated network id to {}; clearing routing table", Util.bytesToHex(networkId));
		
		try {
			write();
		} catch (IOException exc) {
			logger.error("DHT -: Error writing DHT configuration after updating network ID", exc);
		}
	}
	
	public String getBindAddress() {
		return socketManager.getBindAddress();
	}
	
	public int getPort() {
		return socketManager.getPort();
	}
	
	public BandwidthMonitor getMonitorRx() {
		return socketManager.getMonitorRx();
	}
	
	public BandwidthMonitor getMonitorTx() {
		return socketManager.getMonitorTx();
	}
	
	public Blacklist getBlacklist() {
		return master.getBlacklist();
	}
	
	public int numPendingRequests() {
		return protocolManager.getPendingRequests().size();
	}
	
	public ZKMaster getMaster() {
		return master;
	}
	
	public void addPeer(DHTPeer peer) {
		routingTable.suggestPeer(peer);
	}
	
	public void pingAll() {
		routingTable.allPeers().forEach((peer)->{
			peer.ping();
		});
	}
	
	public ThreadGroup getThreadGroup() {
		return threadGroup;
	}
	
	public String toString() {
		if(privateKey == null) return "uninitialized DHTClient";
		return Util.bytesToHex(privateKey.publicKey().getBytes(), 4)
				 + " "
				 + socketManager.getBindAddress()
				 + ":"
				 + socketManager.getPort();
	}
	
	

	protected void closeSubscriptions() {
		for(SubscriptionToken<?> token : subscriptions) {
			token.close();
		}
		
		subscriptions.clear();
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
	
	protected Key tagKey() {
		return tagKey;
	}
	
	protected void setTagKey(Key tagKey) {
		this.tagKey = tagKey;
	}
	
	protected PrivateDHKey getPrivateKey() {
		return privateKey;
	}
	
	protected void setPrivateKey(PrivateDHKey privateKey) {
		this.privateKey = privateKey;
	}
	
	protected FS getStorage() {
		return master.getStorage();
	}
	
	protected synchronized void updateStatus(int newStatus) {
		if(lastStatus == newStatus) return;
		logger.debug("DHT -: status now {}, was {}, table size {}",
				newStatus,
				lastStatus,
				routingTable.allPeers.size());
		lastStatus = newStatus;
		if(statusCallback != null) {
			statusCallback.dhtStatusUpdate(newStatus);
		}
	}
	
	public void write() throws IOException {
		new DHTClientSerializer(this).write();
	}
}
