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

public class DHTClient {
    public final static int AUTH_TAG_SIZE                 = 4;

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
    protected DHTBootstrapper                  bootstrapper;

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
        this.bootstrapper      = new DHTBootstrapper(this);

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
    
    /** Basic initialization. Read client state from disk and load from storage, or create new client state,
     * including private key, from which identity is derived. */
    protected void init() {
        DHTClientSerializer serializer = new DHTClientSerializer(this);
        try {
            serializer.read();
        } catch (IOException exc) {
            // unable to read stored DHT client info; set up from scratch
            privateKey = crypto.makePrivateDHKey();
            tagKey     = new Key(crypto, crypto.makeSymmetricKey());
            id         = DHTID.withKey(getPublicKey());

            assert(id.getLength() == idLength());

            try {
                serializer.write();
            } catch (IOException exc2) {
                logger.error("DHT -: Encountered exception writing DHT client info", exc);
            }
        }
    }
    
    /** Initialize ConfigFile subscriptions to receive notifications of configuration changes. */
    protected void setupSubscriptions() {
        ConfigFile config = master.getGlobalConfig();
        
        // config: net.dht.enabled (boolean) -> Start or stop DHT client.
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
        
        // config: net.dht.port (int) -> UDP bind port for DHT client. Set 0 for OS autoassignment.
        subscriptions.add(config.subscribe("net.dht.port").asInt((port)->{
            synchronized(this) {
                if(socketManager.isListening()  == false) return;
                if(socketManager.getPort()      == port)  return;

                socketManager.setBindPort(port);
                try {
                    socketManager.openSocket();
                } catch (SocketException exc) {
                    logger.warn("DHT -: Unable to open DHT socket when rebinding to port {}",
                            port,
                            exc);
                }
            }
        }));
        
        // config: net.dht.upnp (boolean) -> Enable UPnP support for DHT UDP traffic.
        subscriptions.add(config.subscribe("net.dht.upnp").asBoolean((enabled)->{
            socketManager.setUPnPEnabled(enabled);
        }));
        
        // config: net.dht.bootstrap.enabled (boolean) -> Bootstrap DHT from configured peerfile.
        subscriptions.add(config.subscribe("net.dht.bootstrap.enabled").asBoolean((useBootstrap)->{
            if(useBootstrap) bootstrap();
        }));
        
        // config: net.dht.network (string) -> DHT network ID. Failure to match peers will result in mismatching encryption keys and failure to connect with DHT peers.
        subscriptions.add(config.subscribe("net.dht.network").asString((network)->{
            byte[] newNetworkId = crypto.hash(network.getBytes());
            setNetworkId(newNetworkId);
        }));
        
        // config: net.dht.bootstrap.peerfile (string) -> DHT peerfile for bootstrap use. May be supplied as http(s) URL, file URL, or raw JSON object.
        subscriptions.add(config.subscribe("net.dht.bootstrap.peerfile").asString( (host) -> bootstrap() ));
    }
    
    /** Bind configured UDP port (net.dht.port) and listen for DHT traffic. */ 
    protected void start() {
    	// config: net.dht.bindaddress (string) -> Interface address to bind for DHT traffic. 0.0.0.0 for all IPv4 interfaces, 0:0:0:0:0:0:0:0 for all IPv6.
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
    
    public DHTBootstrapper bootstrapper() {
        return bootstrapper;
    }
    
    /** Use net.dht.bootstrap.peerfile to obtain initial peer list. Purges existing routing table and pending operations, regardless
     * of whether peerfile is valid or accessible. If net.dht.bootstrap.enabled is false, this method has no effect. */ 
    protected synchronized void bootstrap() {
        if(!master.getGlobalConfig().getBool("net.dht.bootstrap.enabled")) return;

        routingTable.reset();
        protocolManager.pendingRequests.clear();
        protocolManager.cancelOperations();
        bootstrapper().bootstrap();
    }
    
    /** Is this DHT client allowed to run right now? Checks net.dht.enabled. */
    public boolean isEnabled() {
        return master.getGlobalConfig().getBool("net.dht.enabled");
    }
    
    /** Bind UDP port on interface and port indicated by net.dht.address and net.dht.port. */
    public DHTClient listen(String address, int port) throws SocketException {
        socketManager.listen(address, port);
        protocolManager.autoFindPeers();
        return this;
    }
    
    /** Set callback to receive updates on DHT connection status. Only one callback can be bound. */
    public DHTClient setStatusCallback(DHTStatusCallback statusCallback) {
        this.statusCallback = statusCallback;
        return this;
    }
    
    /** Length of a DHT identifier. */
    public int idLength() {
        return crypto.hashLength();
    }
    
    /** Temporarily halt DHT participation by closing socket, without suspending background tasks like DHT routing table
     * freshening. Outgoing requests for these background tasks will not be sent, nor will responses to existing requests
     * be accepted. */
    public void pause() {
        socketManager.pause();
    }
    
    /** Tear down this DHTClient instance. Closes the socket, and stops all background tasks. */
    public void close() {
        pause();
        closed = true;
        threadPool.shutdownNow();
        routingTable.close();
        closeSubscriptions();
    }

    /** Tear down this DHTClient instance, and purge all locally stored DHT state, including routing table,
     * previously bound port, and private key. Future DHTClient instances will be constructed from scratch. If
     * net.dht.port is set to 0, then a new random port assignment will be requested from the OS.
     */
    public void purge() throws IOException {
        close();
        master.getGlobalConfig().set("net.dht.lastport", 0);
        new DHTClientSerializer(this).purge();
    }
    
    /** Returns true if we have completed our initial findPeers operation (regardless of whether or not we succeeded in
     * finding responsive peers). */
    public boolean isInitialized() {
        return protocolManager.isInitialized();
    }
    
    /** Print DHTClient state to stdout. */
    public void dump() {
        System.out.println("DHTClient " + this + " initialized=" + isInitialized());
        routingTable.dump();
        store.dump();
    }
    
    /** Return current client status (see DHTClient.STATUS_* constants) */
    public int getStatus() {
        return lastStatus;
    }
    
    /** Return true if this client has been torn down. */
    public boolean isClosed() {
        return closed;
    }
    
    /** Return true if this client has been paused. */
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
    
    /** Update network ID and write changes to local storage. Invoking this method directly should be avoided in favor of
     * updating the net.dht.network setting instead. */
    protected void setNetworkId(byte[] newNetworkId) {
        if(Arrays.equals(newNetworkId, this.networkId)) return;

        this.networkId = newNetworkId;
        logger.info("DHT -: Updated network id to {}; clearing routing table", Util.bytesToHex(networkId));
        // TODO: (review) This log entry says that the routing table is cleared. It does not actually seem to be cleared here. Why?

        try {
            write();
        } catch (IOException exc) {
            logger.error("DHT -: Error writing DHT configuration after updating network ID", exc);
        }
    }
    
    /** Return currently-bound UDP socket interface. */
    public String getBindAddress() {
        return socketManager.getBindAddress();
    }
    
    /** Return currently-bound UDP socket port. If the socket was bound to port 0 for automatic OS assignment, the actual
     * assigned port will be returned. Returns -1 if the socket is not bound. */
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
    
    /** Add a DHTPeer to the routing table, if capacity is available. If no capacity is available, the peer will not be added. */
    public void addPeer(DHTPeer peer) {
        routingTable.suggestPeer(peer);
    }
    
    /** Ping all peers in current routing table. */
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
    
    /** Updates private key for this DHTClient. Also causes update of client ID, which is derived from public key. */
    protected void setPrivateKey(PrivateDHKey privateKey) {
        this.privateKey = privateKey;
        this.id         = DHTID.withKey(privateKey.publicKey());
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
