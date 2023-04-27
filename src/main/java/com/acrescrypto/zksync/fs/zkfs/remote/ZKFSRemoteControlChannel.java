package com.acrescrypto.zksync.fs.zkfs.remote;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.remote.ZKFSRemoteConnection.ChannelListener;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.State;

/** Handles traffic for the ZKFS Remote control channel. Every connection has exactly one control
 * channel, with channel ID 0. This channel is implicitly opened after authentication.
 */
public class ZKFSRemoteControlChannel implements ChannelListener {
    public final static int CMD_PING          = 1;
    public final static int CMD_OPEN_REVTAG   = 2;
    
    protected ZKFSRemoteConnection connection;
    protected long                 channelId;
    private   Logger logger = LoggerFactory.getLogger(ZKFSRemoteControlChannel.class);

    public ZKFSRemoteControlChannel(ZKFSRemoteConnection connection) {
        this.connection = connection;
    }
    
    public synchronized void processMessage(ZKFSRemoteMessageIncoming msg) {
        try {
            processMessageLower(msg);
        } catch (IOException exc) {
            logger.error("ZKFSRemoteControlChannel {}: Caught exception handling msgId {}, cmd {}",
                    connection.remoteAddress(),
                    msg.msgId(),
                    msg.cmd());
            msg.error(exc);
        }
    }
    
    public void processMessageLower(ZKFSRemoteMessageIncoming msg) throws IOException {
        switch(msg.cmd()) {
        case CMD_PING:
            processPing(msg);
            break;
        case CMD_OPEN_REVTAG:
            processOpenRevtag(msg);
            break;
        default:
           processUnsupported(msg);
        }
    }
    
    protected void processPing(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish(buf->{
            msg.respond(buf);
        }); 
    }
    
    protected void processOpenRevtag(ZKFSRemoteMessageIncoming msg) throws IOException {
        State state = connection.listener().state();
        int hashLen = state.getMaster().getCrypto().hashLength();
        
        msg.read(hashLen, archiveIdBuf->{
            byte[] archiveId = new byte[hashLen];
            archiveIdBuf.get(archiveId);
            
            ZKArchiveConfig config = state.configForArchiveId(archiveId);
            if(config == null) throw new ENOENTException(Util.formatArchiveId(archiveId));
            
            msg.waitForFinish(revTagBuf->{
                ZKFS fs;
                if(revTagBuf.hasRemaining()) {
                    byte[] revTagBytes = new byte[revTagBuf.remaining()];
                    revTagBuf.get(revTagBytes);
                    String revTag64    = Util.encode64(revTagBytes);
                
                    fs = state.fsForRevision(config, revTag64);
                } else {
                    fs = state.activeFs(config);
                }
                
                ZKFSRemoteFSChannel channel = new ZKFSRemoteFSChannel(connection,
                                                                      fs);
                long channelId = connection.registerChannel(channel);
                msg.respond(Util.serializeLong(channelId));
            });
        });
    }
    
    protected void processUnsupported(ZKFSRemoteMessageIncoming msg) throws EINVALException {
        throw new EINVALException("unsupported control command " + msg.cmd());
    }

    @Override
    public void setChannelId(long channelId) {
        this.channelId = channelId;
    }
    
    @Override
    public long channelId() {
        return channelId;
    }
    
    @Override
    public void close() {
        connection.closedChannel(this);
    }
}
