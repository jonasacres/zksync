package com.acrescrypto.zksync.fs.zkfs.remote;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionToken;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksyncweb.State;

/** Listen for network connections. This is the frontend for ZKFS Remote support, which is intended
 * for FUSE driver support. ZKFS Remote exposes a TCP port for unencrypted, low-overhead access to
 * ZKFS instances, which can be opened via a revtag.
 */
public class ZKFSRemoteListener implements AutoCloseable {
    public final static int DEFAULT_PORT = 15692;
    protected ServerSocketChannel               listenSocket;
    protected LinkedList<ZKFSRemoteConnection>  connections = new LinkedList<>();
    protected GroupedThreadPool                 threadPool;
    protected BandwidthMonitor                  txMonitor,
                                                rxMonitor;
    protected State                             state;
    protected boolean                           listening;
    protected LinkedList<SubscriptionToken<?>>  tokens = new LinkedList<>();
    
    private   Logger logger = LoggerFactory.getLogger(ZKFSRemoteListener.class);
    
    public ZKFSRemoteListener(State state) throws IOException {
        this.state          = state;
        this.txMonitor      = new BandwidthMonitor(state.getMaster().getBandwidthMonitorTx());
        this.rxMonitor      = new BandwidthMonitor(state.getMaster().getBandwidthMonitorRx());
        this.threadPool     = GroupedThreadPool.newCachedThreadPool("ZKFSRemoteListener");
        
        monitorConfig();
        checkListen();
    }
    
    protected void monitorConfig() {
        ConfigFile config   = state.getMaster().getGlobalConfig();
        
        tokens.add(config.subscribe("net.remotefs.enabled").asBoolean(enabled->checkListen()));
        tokens.add(config.subscribe("net.remotefs.address").asString (address->checkListen()));
        tokens.add(config.subscribe("net.remotefs.port")   .asInt    (port   ->checkListen()));
    }
    
    protected synchronized void checkListen() {
        try {
            checkListenLower();
        } catch(IOException exc) {
            logger.error("ZKFSRemoteListener {}: Caught exception checking listen status; closing", exc);
            try {
                closeSocket();
            } catch (IOException exc2) {
                logger.error("ZKFSRemoteListener {}: Caught exception closing after encountering exception", exc2);
            }
        }
    }
    
    protected void checkListenLower() throws IOException {
        ConfigFile config   = state.getMaster().getGlobalConfig();
        boolean    enabled  = config.getBool  ("net.remotefs.enabled");
        String     address  = config.getString("net.remotefs.address");
        int        port     = config.getInt   ("net.remotefs.port");
        
        if(enabled == listening) {
            if(!enabled) return;
            
            boolean addressMatch = listenAddress().equals(address),
                    portMatch    = listenPort   () == port,
                    needsRebind  = !addressMatch || !portMatch;
            
            if(needsRebind) {
                closeSocket();
                openSocket(address, port);
            }
        } else {
            if(enabled) {
                openSocket(address, port);
            } else {
                closeSocket();
            }
        }
    }
    
    public void openSocket(String bindAddress, int bindPort) throws IOException {
        logger.debug("ZKFSRemoteListener {}: Attempting to bind on {}:{}",
                logName(),
                bindAddress,
                bindPort);
        listenSocket = ServerSocketChannel.open();
        listenSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        listenSocket.bind(new InetSocketAddress(bindAddress, bindPort));
        listen();
        
        logger.info("ZKFSRemoteListener {}: Listening on {}:{}",
                logName(),
                bindAddress,
                bindPort);
    }
    
    public void closeSocket() throws IOException {
        listening = false;
        if(listenSocket == null) return;

        logger.info("ZKFSRemoteListener {}: Closing socket",
                logName());
        
        listenSocket.close();
        listenSocket = null;
    }

    public void closedConnection(ZKFSRemoteConnection connection) {
        connections.remove(connection);
    }
    
    public boolean isListening() {
        return listening;
    }
    
    public void listen() {
        listening = true;
        threadPool.submit(()->listenBody(listenSocket));
    }
    
    public void listenBody(ServerSocketChannel lSocket) {
        while(listening && lSocket.isOpen()) {
            SocketChannel socket = null;
            
            try {
                socket = lSocket.accept();
                if(socket == null) continue;
                
                ZKFSRemoteConnection connection = new ZKFSRemoteConnection(this, socket);
                connections.add(connection);
            } catch(AsynchronousCloseException exc) {
                logger.debug("ZKFSRemoveListener {}: exiting listen loop due to closed socket",
                        logName());
            } catch(Exception exc) {
                logger.error("ZKFSRemoteListener {}: caught exception in listen thread, accepting peer {}",
                        logName(),
                        socket != null
                          ? socket.socket().getRemoteSocketAddress().toString()
                          : "null",
                        exc);
                if(socket != null) {
                    try {
                        socket.close();
                    } catch(IOException exc2) {
                        logger.error("ZKFSRemoteListener {}: caught subsequent exception closing socket {}",
                                socket.socket().getRemoteSocketAddress().toString(),
                                exc2);
                        exc.printStackTrace();
                    }
                }
            }
        }
    }
    
    public void close() throws IOException {
        for(SubscriptionToken<?> token : tokens) {
            token.close();
        }
        
        closeSocket();

        for(ZKFSRemoteConnection connection : connections) {
            connection.close();
        }
    }
    
    public Collection<ZKFSRemoteConnection> connections() {
        return connections;
    }
    
    public String listenAddress() {
        return listenSocket.socket().getInetAddress().getHostAddress();
    }
    
    public int listenPort() {
        return listenSocket.socket().getLocalPort();
    }

    public BandwidthMonitor txMonitor() {
        return txMonitor;
    }
    
    public BandwidthMonitor rxMonitor() {
        return rxMonitor;
    }
    
    public State state() {
        return state;
    }
    
    public String logName() {
        if(listenSocket == null || !listenSocket.isOpen()) return "-";
        
        return String.format("%s:%d",
                listenAddress(),
                listenPort());
    }

    public GroupedThreadPool threadPool() {
        return threadPool;
    }
}
