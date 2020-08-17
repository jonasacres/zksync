package com.acrescrypto.zksync.fs.zkfs.remote;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.GroupedThreadPool;

public class ZKFSRemoteListener {
    protected ServerSocketChannel               listenSocket;
    protected LinkedList<ZKFSRemoteConnection>  connections;
    protected GroupedThreadPool                 threadPool;
    protected BandwidthMonitor                  txMonitor,
                                                rxMonitor;
    protected ZKMaster                          master;
    
    private   Logger logger = LoggerFactory.getLogger(ZKFSRemoteListener.class);
    
    public ZKFSRemoteListener(ZKMaster master, String address, int port) throws IOException {
        this.master       = master;
        this.listenSocket = ServerSocketChannel.open();
        this.threadPool   = GroupedThreadPool.newCachedThreadPool("ZKFSRemoteListener");
        
        listenSocket.socket().bind(new InetSocketAddress(address, port));
        listen();
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
    
    public ZKMaster master() {
        return master;
    }
}
