package com.acrescrypto.zksync.fs.zkfs.remote;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.GroupedThreadPool;

public class ZKFSRemoteConnection {
    protected SocketChannel                           socket;
    protected ZKFSRemoteListener                      listener;
    protected HashMap<Long,ZKFSRemoteMessageIncoming> messages;
    protected ByteBuffer                              readBuf = ByteBuffer.allocate(64*1024),
                                                      takeBuf = ByteBuffer.wrap(readBuf.array(), 0, 0);;
    protected ZKFSRemoteMessageIncoming               activeMessage;
    protected long                                    remainingOnActive;
    protected BandwidthMonitor                        txMonitor,
                                                      rxMonitor;

    private   Logger logger = LoggerFactory.getLogger(ZKFSRemoteConnection.class);
    
    public ZKFSRemoteConnection(ZKFSRemoteListener listener, SocketChannel socket) {
        this.socket    = socket;
        this.listener  = listener;
        this.txMonitor = new BandwidthMonitor(listener.txMonitor());
        this.rxMonitor = new BandwidthMonitor(listener.rxMonitor());
    }
    
    public void monitor(GroupedThreadPool pool) {
        pool.submit(()->monitorBody());
    }
    
    protected void monitorBody() {
        try {
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
    
    public void check() throws IOException, ProtocolViolationException {
        int     headerSize  = 3*8 + 1*4;
        boolean needsData   = activeMessage != null,
                needsHeader = activeMessage == null
                           && takeBuf.remaining() < headerSize,
                needsRead   = needsHeader || needsData;
        
        if(needsRead) {
            int bytesRead   = socket.read(readBuf);
            if(bytesRead < 0) {
                close();
            }
            
            readBuf.position(readBuf.position() + bytesRead);
            takeBuf.limit(takeBuf.limit() + bytesRead);
        }
        
        if(activeMessage == null) {
            if(takeBuf.remaining() < headerSize) return;
            
            long              msgLen = readBuf.getLong();
            long               msgId = readBuf.getLong();
            long           channelId = readBuf.getLong();
            int                  cmd = readBuf.getInt ();
            
            remainingOnActive        = msgLen - headerSize - readBuf.remaining();
            require(remainingOnActive >= 0);
            
            activeMessage            = messages.getOrDefault(msgId, null);
            if(activeMessage == null) {
                activeMessage        = new ZKFSRemoteMessageIncoming(this,
                                                                     msgId,
                                                                     channelId,
                                                                     cmd,
                                                                     msgLen);
            }
        }
        
        int readLen = (int) Math.min(remainingOnActive,
                                     takeBuf.remaining());
        remainingOnActive -= readLen;
        ByteBuffer msgBuf = ByteBuffer.allocate(readLen);
        msgBuf.put(takeBuf.array(), takeBuf.position(), readLen);

        listener.threadPool.submit(()->{
            activeMessage.addBytes(msgBuf);
        });
        
        takeBuf.position(takeBuf.position() + readLen);

        if(remainingOnActive == 0) {
            removeMessage(activeMessage);
            activeMessage = null;
        }
        
        if(!takeBuf.hasRemaining() || readBuf.remaining() < 1024) {
            recycleBuffer();
        }
    }
    
    public void recycleBuffer() {
        if(takeBuf.hasRemaining()) {
            ByteBuffer newBuffer = ByteBuffer.allocate(readBuf.capacity());
            newBuffer.put(takeBuf);
            
            readBuf = newBuffer;
            takeBuf = ByteBuffer.wrap(newBuffer.array(),
                                      0,
                                      newBuffer.position());
            return;
        }
        
        readBuf.reset();
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
             maskedId     = msg.msgId() & mask,
             msgLen       = headerSize + payloadSize;
        
        ByteBuffer header = ByteBuffer.allocate(headerSize);
        header.putLong(msgLen);
        header.putLong(maskedId);
        
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
        return socket.socket().getRemoteSocketAddress().toString();
    }
    
    public void close() throws IOException {
        socket.close();
    }
    
    public void require(boolean test) throws ProtocolViolationException {
        if(!test) throw new ProtocolViolationException();
    }
}
