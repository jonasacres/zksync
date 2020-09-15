package com.acrescrypto.zksync.fs.zkfs.remote;

import com.acrescrypto.zksync.exceptions.ErrnoException;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

public class ZKFSRemoteMessageIncoming {
    public interface WaitCallback {
        public void ready(ByteBuffer data) throws IOException;
    }

    public final static int CMD_CLOSE_CHANNEL       =  0;
    public final static int CMD_OPEN_FS             =  1;
    
    public final static int CMD_ACCESS              =  2;
    public final static int CMD_BMAP                =  3;
    public final static int CMD_COPY_FILE_RANGE     =  4;
    public final static int CMD_CREATE              =  5;
    public final static int CMD_DESTROY             =  6;
    public final static int CMD_FALLOCATE           =  7;
    public final static int CMD_FLOCK               =  8;
    public final static int CMD_FLUSH               =  9;
    public final static int CMD_FORGET              = 10;
    public final static int CMD_FSYNC               = 11;
    public final static int CMD_FSYNCDIR            = 12;
    public final static int CMD_GETATTR             = 13;
    public final static int CMD_GETLK               = 14;
    public final static int CMD_GETXATTR            = 15;
    public final static int CMD_IOCTL               = 16;
    public final static int CMD_LINK                = 17;
    public final static int CMD_LISTXATTR           = 18;
    public final static int CMD_LOOKUP              = 19;
    public final static int CMD_LSEEK               = 20;
    public final static int CMD_MKDIR               = 21;
    public final static int CMD_MKNOD               = 22;
    public final static int CMD_OPEN                = 23;
    public final static int CMD_OPENDIR             = 24;
    public final static int CMD_POLL                = 25;
    public final static int CMD_READ                = 26;
    public final static int CMD_READDIR             = 27;
    public final static int CMD_READDIRPLUS         = 28;
    public final static int CMD_READLINK            = 29;
    public final static int CMD_RELEASE             = 30;
    public final static int CMD_RELEASEDIR          = 31;
    public final static int CMD_REMOVEXATTR         = 32;
    public final static int CMD_RENAME              = 33;
    public final static int CMD_RETRIEVE_REPLY      = 34;
    public final static int CMD_RMDIR               = 35;
    public final static int CMD_SETATTR             = 36;
    public final static int CMD_SETLK               = 37;
    public final static int CMD_SETXATTR            = 38;
    public final static int CMD_STATFS              = 39;
    public final static int CMD_SYMLINK             = 40;
    public final static int CMD_UNLINK              = 41;
    public final static int CMD_WRITE               = 42;
    
    public final static int RENAME_NOREPLACE        = (1 << 0);
    public final static int RENAME_EXCHANGE         = (1 << 1);
    
    // taken from https://github.com/libfuse/libfuse/blob/7b3e3899157566875280a8b860eb5ad5c73eadc1/include/fuse_lowlevel.h:131
    public final static int FUSE_SET_ATTR_MODE      = (1 <<  0);
    public final static int FUSE_SET_ATTR_UID       = (1 <<  1);
    public final static int FUSE_SET_ATTR_GID       = (1 <<  2);
    public final static int FUSE_SET_ATTR_SIZE      = (1 <<  3);
    public final static int FUSE_SET_ATTR_ATIME     = (1 <<  4);
    public final static int FUSE_SET_ATTR_MTIME     = (1 <<  5);
      // no constant for 1 << 6 in reference header
    public final static int FUSE_SET_ATTR_ATIME_NOW = (1 <<  7);
    public final static int FUSE_SET_ATTR_MTIME_NOW = (1 <<  8);
      // no constant for 1 << 9 in reference header
    public final static int FUSE_SET_ATTR_CTIME     = (1 << 10);
    public final static int FUSE_SET_ATTR_KILLPRIV  = (1 << 31); // not in reference header; hijacking this bit to indicate if we squash setuid/setgid on setattr

    public final static int ATTR_ITEM_MODE          =  1;
    public final static int ATTR_ITEM_UID           =  2;
    public final static int ATTR_ITEM_GID           =  3;
    public final static int ATTR_ITEM_USER          =  4;
    public final static int ATTR_ITEM_GROUP         =  5;
    public final static int ATTR_ITEM_SIZE          =  6;
    public final static int ATTR_ITEM_ATIME         =  7;
    public final static int ATTR_ITEM_MTIME         =  8;
    public final static int ATTR_ITEM_ATIME_NOW     =  9;
    public final static int ATTR_ITEM_MTIME_NOW     = 10;
    public final static int ATTR_ITEM_CTIME         = 11;
    
    public final static int ATTR_FLAG_KILLPRIV      = (1 << 0);
    
    public class ByteConsumer {
        int                    readLength,
                               remaining;
        LinkedList<ByteBuffer> buffers     = new LinkedList<>();
        WaitCallback           callback;
        
        public ByteConsumer(int readLength, WaitCallback callback) {
            this.readLength = readLength;
            this.remaining  = readLength;
            this.callback   = callback;
        }
        
        public boolean append(ByteBuffer buffer) {
            buffers.add(buffer);
            
            this.remaining -= buffer.remaining();
            if(this.remaining < 0) {
                dispatch();
                return readLength < 0;
            }
            
            return false;
        }
        
        public void dispatch() {
            ByteBuffer merged = ByteBuffer.allocate(readLength);
            for(ByteBuffer fragment : buffers) {
                int readLen = Math.min(merged.remaining(), fragment.remaining());
                if(readLen == 0) break;
                merged.put(fragment.array(), fragment.position(), readLen);
            }
            merged.flip();
            
            try {
                callback.ready(merged);
            } catch(IOException exc) {
                ZKFSRemoteMessageIncoming.this.error(exc);
            }
        }
    }
    
    protected Queue<ByteConsumer>    consumers              = new LinkedList<>();
    protected Queue<ByteBuffer>      pendingReadBuffers     = new LinkedList<>();
    protected Queue<ByteBuffer>      pendingResponseBuffers = new LinkedList<>();
    protected ZKFSRemoteConnection   connection;
    protected int                    cmd;
    protected boolean                responseStarted,
                                     finished,
                                     senderFinished;
    protected long                   msgId,
                                     channelId,
                                     lastDeferTime;
    protected SnoozeThread           snoozeSend;
    
    public ZKFSRemoteMessageIncoming(ZKFSRemoteConnection connection, long msgId, long channelId, int cmd) {
        this.connection = connection;
        this.msgId      = msgId;
        this.channelId  = channelId;
        this.cmd        = cmd;
    }
    
    public synchronized void addBytes(ByteBuffer buf) {
        System.out.printf("addBytes(): buf.remaining()=%d\n", buf.remaining());
        pendingReadBuffers.add(buf);
        drain();
    }

    public int cmd() {
        return cmd;
    }
    
    public void waitForFinish(WaitCallback callback) throws IOException {
        consumers.add(new ByteConsumer(-1, callback));
        drain();
    }
    
    public void readUntilFinished(int length, WaitCallback callback) throws IOException {
        consumers.add(new ByteConsumer(length, (buf)->{
            callback.ready(buf);
            if(buf != null) readUntilFinished(length, callback);
        }));
        drain();
    }
    
    public void read(int length, WaitCallback callback) throws IOException {
        consumers.add(new ByteConsumer(length, callback));
        drain();
    }
    
    public void respond(byte[] data) {
        respond(data, true);
    }
    
    public void respond(ByteBuffer buf) {
        byte[] data = new byte[buf.remaining()];
        buf.get(data);
        
        respond(data, true);
    }

    public void respond(byte[] data, boolean finished) {
        if(!responseStarted) {
            ByteBuffer noError     = ByteBuffer.wrap(Util.serializeByte((byte) 0));
            pendingResponseBuffers.add(noError);
        }
        
        responseStarted = true;
        pendingResponseBuffers.add(ByteBuffer.wrap(data));
        
        sendIfReady();
    }
    
    public void respond() {
        respond(new byte[0], true);
    }
    
    public void senderFinished() {
        try {
            senderFinished = true;
            
            if(!consumers.isEmpty()) {
                consumers.peek().callback.ready(null);
            }
        } catch(IOException exc) {
            error(exc);
        }
    }
    
    public boolean isSenderFinished() {
        return senderFinished;
    }
    
    public void error(IOException exc) {
        if(responseStarted) {
            respond(); // too late to send error, just let receiver know we're done
            return;
        }
        
        int errno;
        
        if(exc instanceof ErrnoException) {
            errno = ((ErrnoException) exc).errno();
        } else {
            errno = -1;
        }
        
        String msg = String.format("%x:%s:%s",
                errno,
                exc.getClass().getSimpleName(),
                exc.getMessage());
        
        if(msg.length() >= 256) {
            msg = msg.substring(0, 256);
        }
        
        ByteBuffer errLenBuf = ByteBuffer.wrap(Util.serializeByte((byte) msg.length()));
        ByteBuffer msgBuf    = ByteBuffer.wrap(msg.getBytes());
        
        pendingResponseBuffers.add(errLenBuf);
        pendingResponseBuffers.add(msgBuf);
        send();
    }
    
    protected void sendIfReady() {
        if(!finished) {
            long pendingSize = 0;
            for(ByteBuffer buf : pendingResponseBuffers) {
                pendingSize += buf.getLong();
            }
            
            if(pendingSize < sendSizeThresholdBytes()) {
                if(snoozeSend == null) {
                    snoozeSend    = new SnoozeThread(
                            sendTimeThresholdMs(),
                            sendTimeMaxMs(),
                            false,
                            ()->send());
                } else {                
                    snoozeSend.snooze();
                }
            }
        }
        
        send();
    }
    
    protected void drain() {
        System.out.printf("drain(): pendingBytes=%d\n", pendingBytes());
        while(!pendingReadBuffers.isEmpty()
           && !consumers         .isEmpty())
        {
            ByteBuffer buffer = pendingReadBuffers.peek();
            
            if(consumers.peek().append(buffer)) {
                consumers.poll();
            }
            
            if(!buffer.hasRemaining()) {
                pendingReadBuffers.poll();
            }
        }
    }
    
    protected void send() {
        connection.sendResponse(this, pendingResponseBuffers, finished);
        pendingResponseBuffers.clear();
        
        if(snoozeSend != null) {
            snoozeSend.cancel();
            snoozeSend = null;
        }
    }

    public long msgId() {
        return msgId;
    }

    public long channelId() {
        return channelId;
    }
    
    public long sendSizeThresholdBytes() {
        return 16*1024;
    }
    
    public long sendTimeThresholdMs() {
        return 10;
    }
    
    public long sendTimeMaxMs() {
        return 20;
    }
    
    public long pendingBytes() {
        long totalLen = 0;
        
        for(ByteBuffer buf : pendingReadBuffers) {
            totalLen += buf.remaining();
        }
        
        return totalLen;
    }

    public boolean hasBytes(int length) {
        return pendingBytes() >= length;
    }
}
