package com.acrescrypto.zksync.fs.zkfs.remote;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.State;

/** Handles a single connection instance for ZKFS Remote. This is an unencrypted, low-overhead
 * means of creating and interacting with ZKFS instances, intended for use in FUSE driver support.
 * Incoming connections are authenticated via a fixed passphrase defined in the net.remotefs.secret
 * config variable; traffic is unencrypted, since it is intended that this protocol be invoked over
 * the loopback interface rather than the open network. Traffic over insecure networks should be routed
 * over an encrypted tunnel, such as a VPN.
 * 
 * Connections are multiplexed into individual channels, each handling a specific form of traffic, eg.
 * control operations or operations against a specific ZKFS instance. This is intended to allow numerous
 * parallel operations on a single connection, while minimizing the likelihood of a time intensive task
 * on one filesystem interfering with control messages or operations on other filesystems.
 * 
 * Channel 0 is reserved as the control channel, which is opened after authentication.
 */
public class ZKFSRemoteConnection {
    public final static long CHANNEL_ID_CONTROL     = 0;
    public final static long CHANNEL_ID_USER_START  = 65536;
    
    protected SocketChannel                           socket;
    protected ZKFSRemoteListener                      listener;
    protected HashMap<Long,ZKFSRemoteMessageIncoming> messages      = new HashMap<>();
    protected ConcurrentHashMap<Long,ChannelListener> channels      = new ConcurrentHashMap<>();
    protected ByteBuffer                              readBuf       = ByteBuffer.allocate(64*1024),
                                                      takeBuf       = ByteBuffer.wrap(readBuf.array(), 0, 0);;
    protected ZKFSRemoteMessageIncoming               activeMessage;
    protected long                                    remainingOnActive;
    protected AtomicLong                              nextChannelId = new AtomicLong(CHANNEL_ID_USER_START);
    protected BandwidthMonitor                        txMonitor,
                                                      rxMonitor;
    protected SnoozeThread                            authenticationTimer;
    
    public interface ChannelListener {
        public void processMessage(ZKFSRemoteMessageIncoming msg);
        public void setChannelId  (long channelId);
        public long channelId     ();
        public void close         ()                                throws IOException;
    }

    private   Logger logger = LoggerFactory.getLogger(ZKFSRemoteConnection.class);
    
    public ZKFSRemoteConnection(ZKFSRemoteListener listener, SocketChannel socket) {
        this.socket              = socket;
        this.listener            = listener;
        this.txMonitor           = new BandwidthMonitor(listener.txMonitor());
        this.rxMonitor           = new BandwidthMonitor(listener.rxMonitor());
        this.authenticationTimer = new SnoozeThread(authTimeout(), false, ()->{
            logger.warn("ZKFSRemoteConnection {}: Timed out waiting for authentication",
                    remoteAddress());
            try {
                close();
            } catch (IOException exc) {
                logger.error("ZKFSRemoteConnection {}: Encountered exception closing during auth timeout",
                        remoteAddress(),
                        exc);
            }
        });
    }
    
    public int authTimeout() {
        return listener.state().getMaster().getGlobalConfig().getInt("net.remotefs.authTimeoutMs");
    }
    
    public long registerChannel(ChannelListener channelListener) {
        long channelId = nextChannelId.getAndIncrement();
        channels.put(channelId, channelListener);
        channelListener.setChannelId(channelId);
        
        return channelId;
    }
    
    public Collection<Long> channels() {
        return channels.keySet();
    }
    
    public boolean hasChannel(long channelId) {
        return channels.containsKey(channelId);
    }
    
    public ChannelListener listenerForChannel(long channelId) {
        return channels.getOrDefault(channelId, null);
    }
    
    public void monitor(GroupedThreadPool pool) {
        pool.submit(()->monitorBody());
    }
    
    protected void monitorBody() {
        try {
            logger.debug("ZKFSRemoteConnection {}: Thread started", remoteAddress());
            authenticate();
            
            while(true) {
                check();
            }
        } catch(IOException|ProtocolViolationException exc) {
            try {
                logger.warn("ZKFSRemoteConnection {}: Caught exception",
                        remoteAddress(),
                        exc);
                close();
            } catch (IOException exc2) {
                logger.error("ZKFSRemoteConnection -: Caught subsequent exception while closing connectio in processing exception.", exc2);
            }
        }
    }
    
    protected void authenticate() throws IOException, ProtocolViolationException {
        CryptoSupport crypto        = listener.state().getMaster().getCrypto();
        byte[]        salt          = crypto.rng(crypto.hashLength()),
                      response      = new byte[2*crypto.hashLength()],
                      peerSalt      = new byte[  crypto.hashLength()],
                      peerHash      = new byte[  crypto.hashLength()];
        ByteBuffer    responseBuf   = ByteBuffer.wrap(response);
        
        socket.write(ByteBuffer.wrap(Util.serializeInt(authHashIterations())));
        socket.write(ByteBuffer.wrap(salt));
        txMonitor.observeTraffic(4 + salt.length);
        
        IOUtils.readFully(socket, responseBuf);
        
        responseBuf.flip();
        rxMonitor.observeTraffic(responseBuf.remaining());
        responseBuf.get(peerSalt);
        responseBuf.get(peerHash);
        
        byte[]        secret        = State
                                      .sharedState()
                                      .getMaster()
                                      .getGlobalConfig()
                                      .getString("net.remotefs.secret")
                                      .getBytes();
        byte[]        combinedSalt  = Util.concat(salt, peerSalt),
                      counterSalt   = Util.concat(peerSalt, salt),
                      expectedHash  = iteratedHash(authHashIterations(), combinedSalt, secret),
                      counterHash   = iteratedHash(authHashIterations(), counterSalt,  secret);
        boolean       match         = Arrays.equals(expectedHash, peerHash);
        
        if(!match) {
            logger.warn("ZKFSRemoteConnection {}: Peer failed secret challenge", remoteAddress());
            throw new ProtocolViolationException();
        }
        
        authenticationTimer.cancel();
        authenticationTimer = null;
        
        logger.debug("ZKFSRemoteConnection {}: Authenticated", remoteAddress());
        socket.write(ByteBuffer.wrap(counterHash));
        txMonitor.observeTraffic(counterHash.length);
        
        channels.put(CHANNEL_ID_CONTROL,
                     new ZKFSRemoteControlChannel(this));
        monitor(listener.threadPool());
    }
    
    protected int authHashIterations() {
        return listener.state().getMaster().getGlobalConfig().getInt("net.remotefs.authHashIterations");
    }
    
    protected byte[] iteratedHash(int iterations, byte[] salt, byte[] secret) {
        CryptoSupport crypto  = listener.state().getMaster().getCrypto();
        byte[]        current = secret;
        
        for(int i = 0; i < iterations; i++) {
            current           = crypto.authenticate(salt, current);
        }
        
        return current;
    }
    
    public void check() throws IOException, ProtocolViolationException {
        int     headerSize  = 3*8 + 1*4;
        boolean needsData   = activeMessage != null,
                needsHeader = activeMessage == null
                           && takeBuf.remaining() < headerSize,
                needsRead   = needsHeader || needsData;
        
        System.out.printf("check(): needsRead=%s, needsData=%s, needsHeader=%s\n", needsRead, needsData, needsHeader);
        if(needsRead) {
            int bytesRead   = socket.read(readBuf);
            if(bytesRead < 0) {
                close();
            }
            
            System.out.printf("check(): bytesRead=%d\n", bytesRead);
            takeBuf.limit   (takeBuf.limit()    + bytesRead);
        }
        
        if(activeMessage == null) {
            if(takeBuf.remaining() < headerSize) return;
            
            long              msgLen = takeBuf.getLong();
            long               msgId = takeBuf.getLong();
            long           channelId = takeBuf.getLong();
            int                  cmd = takeBuf.getInt ();
            
            remainingOnActive        = msgLen
                                     - headerSize;
            require(remainingOnActive >= 0);
            
            ChannelListener listener = channels.getOrDefault(channelId, null);
            activeMessage            = messages.getOrDefault(msgId,     null);
            if(activeMessage == null) {
                activeMessage        = new ZKFSRemoteMessageIncoming(this,
                                                                     msgId,
                                                                     channelId,
                                                                     cmd);
                if(listener != null) {
                    listener.processMessage(activeMessage);
                }
            }
        }
        
        int         readLen = (int) Math.min(remainingOnActive,
                                     takeBuf.remaining());
        System.out.printf("check(): readLen=%d, remainingOnActive=%d\n", readLen, remainingOnActive);
        
        if(readLen > 0) {
            ZKFSRemoteMessageIncoming msg    = activeMessage;
            remainingOnActive               -= readLen;
            ByteBuffer                msgBuf = ByteBuffer.allocate(readLen);
            msgBuf.put(takeBuf.array(), takeBuf.position(), readLen);
            msgBuf.flip();
    
            listener.threadPool.submit(()->{
                System.out.printf("check(): adding bytes, msgBuf.remaining()=%d, msg=%s\n", msgBuf.remaining(), msg);
                msg.addBytes(msgBuf);
            });
            
            takeBuf.position(takeBuf.position() + readLen);
        }

        if(remainingOnActive == 0) {
            removeMessage(activeMessage);
            activeMessage = null;
        }
        
        if(!takeBuf.hasRemaining() || readBuf.remaining() < 1024) {
            try {
                recycleBuffer();
            } catch(Exception exc) {
                exc.printStackTrace();
            }
        }
    }
    
    protected void recycleBuffer() {
        if(takeBuf.hasRemaining()) {
            // make a new buffer, copy the unread bytes to the start
            ByteBuffer newBuffer = ByteBuffer.allocate(readBuf.capacity());
            newBuffer.put(takeBuf);
            
            readBuf = newBuffer;
            takeBuf = ByteBuffer.wrap(newBuffer.array(),
                                      0,
                                      newBuffer.position());
            return;
        }
        
        // no unread data, so just reset the buffer indices
        readBuf.clear();
        takeBuf.clear();
        takeBuf.limit(0);
    }
    
    public void removeMessage(ZKFSRemoteMessageIncoming msg) {
        messages.remove(msg.msgId()); 
    }
    
    public void sendResponse(ZKFSRemoteMessageIncoming msg, Collection<ByteBuffer> payload, boolean finished) {
        int  headerSize   = 2*8;
        long payloadSize  = 0;
        
        for(ByteBuffer buf : payload) {
            payloadSize  += buf.remaining();
        }
        
        long mask         = finished ? (1 << 63) : 0,
             maskedId     = msg.msgId() | mask,
             msgLen       = headerSize + payloadSize;
        
        ByteBuffer header = ByteBuffer.allocate(headerSize);
        header.putLong(msgLen);
        header.putLong(maskedId);
        header.flip();
        
        try {
            socket.write(header);
            for(ByteBuffer buf : payload) {
                socket.write(buf);
            }
        } catch (IOException exc) {
            logger.error("ZKFSRemoteConnection {}: Exception sending bytes for msgId {}, cmd {}, finished {}",
                    remoteAddress(),
                    msg.msgId(),
                    msg.cmd(),
                    finished,
                    exc);
            try {
                close();
            } catch (IOException exc2) {
                logger.error("ZKFSRemoteConnection {}: Exception closing socket following previous exception for msgId {}",
                        remoteAddress(),
                        exc);
            }
        }
    }
    
    public String remoteAddress() {
        return socket.socket().getInetAddress().getHostAddress();
    }
    
    public void close() throws IOException {
        socket.close();
        
        for(ChannelListener channel : channels.values()) {
            try {
                channel.close();
            } catch(IOException exc) {
                logger.error("ZKFSRemoteConnection {}: Caught exception closing channel {}",
                        remoteAddress(),
                        channel);
                channels.remove(channel.channelId());
            }
        }
    }
    
    public void require(boolean test) throws ProtocolViolationException {
        if(!test) throw new ProtocolViolationException();
    }

    public ZKFSRemoteListener listener() {
        return listener;
    }

    public void closedChannel(ChannelListener channel) {
        channels.remove(channel.channelId());
    }

    public BandwidthMonitor txMonitor() {
        return txMonitor;
    }
    
    public BandwidthMonitor rxMonitor() {
        return rxMonitor;
    }
}
