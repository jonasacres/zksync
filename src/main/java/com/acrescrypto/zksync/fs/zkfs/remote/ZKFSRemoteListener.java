package com.acrescrypto.zksync.fs.zkfs.remote;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionToken;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksyncweb.State;

public class ZKFSRemoteListener {
    public final static int DEFAULT_PORT = 15692;
    protected ServerSocketChannel               listenSocket;
    protected LinkedList<ZKFSRemoteConnection>  connections;
    protected GroupedThreadPool                 threadPool;
    protected BandwidthMonitor                  txMonitor,
                                                rxMonitor;
    protected State                             state;
    protected boolean                           listening;
    protected LinkedList<SubscriptionToken<?>>  tokens = new LinkedList<>();
    
    private   Logger logger = LoggerFactory.getLogger(ZKFSRemoteListener.class);
    
    public ZKFSRemoteListener(State state) throws IOException {
        this.state        = state;
        this.threadPool   = GroupedThreadPool.newCachedThreadPool("ZKFSRemoteListener");
        
        monitorConfig();
        checkListen();
    }
    
    public void close() throws IOException {
        for(SubscriptionToken<?> token : tokens) {
            token.close();
            for(ZKFSRemoteConnection connection : connections) {
                connection.close();
            }
        }

        closeSocket();
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
            
            boolean addressMatch = listenSocket
                                     .socket()
                                     .getLocalSocketAddress()
                                     .toString()
                                     .equals(address),
                    portMatch    = listenSocket
                                     .socket()
                                     .getLocalPort() == port,
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
        listenSocket.socket().bind(new InetSocketAddress(bindAddress, bindPort));
        listen();
        
        logger.info("ZKFSRemoteListener {}: Listening on {}:{}",
                logName(),
                bindAddress,
                bindPort);
    }
    
    public void closeSocket() throws IOException {
        if(listenSocket == null) return;

        logger.info("ZKFSRemoteListener {}: Closing socket",
                logName());
        
        listenSocket.close();
        listenSocket = null;
    }

    public void closedConnection(ZKFSRemoteConnection connection) {
        connections.remove(connection);
    }
    
    public void listen() {
        threadPool.submit(()->listenBody());
    }
    
    public void listenBody() {
        try {
            while(true) {
                SocketChannel socket = listenSocket.accept();
                ZKFSRemoteConnection connection = new ZKFSRemoteConnection(this, socket);
                connections.add(connection);
                
            }
        } catch(IOException exc) {
            logger.error("ZKFSRemoteListener {}:{}: caught fatal exception in listen thread",
                    listenAddress(),
                    listenPort(),
                    exc);
        }
    }
    
    public String listenAddress() {
        return listenSocket.socket().getLocalSocketAddress().toString();
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
}
