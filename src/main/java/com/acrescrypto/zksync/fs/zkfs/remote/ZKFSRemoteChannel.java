package com.acrescrypto.zksync.fs.zkfs.remote;

import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.EISNOTDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.ENOTEMPTYException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.ZKDirectory;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFile;
import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.utility.Util;

import static com.acrescrypto.zksync.fs.Stat.*;
import static com.acrescrypto.zksync.fs.zkfs.ZKFile.*;
import static com.acrescrypto.zksync.fs.zkfs.remote.ZKFSRemoteMessageIncoming.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: lookup counts!

public class ZKFSRemoteChannel implements AutoCloseable {
    protected ZKFS                         fs;
    protected long                         channelId;
    protected int                          nextFileDescriptor   = 1;
    
    protected HashMap<Integer,ZKFile>      fileDescriptors      = new HashMap<>();
    protected HashMap<Integer,ZKDirectory> directoryDescriptors = new HashMap<>();
    private   Logger                       logger               = LoggerFactory.getLogger(ZKFSRemoteChannel.class);
    
    public void processMessage(ZKFSRemoteMessageIncoming msg) throws IOException {
        switch(msg.cmd()) {
        case CMD_CLOSE_CHANNEL:
            processCloseChannel          (msg);
            break;
        case CMD_COPY_FILE_RANGE:
            processCopyFileRange         (msg);
            break;
        case CMD_CREATE:
            processCreate                (msg);
            break;
        case CMD_DESTROY:
            processCloseChannel          (msg);
            break;
        case CMD_FSYNC:
            processFsync                 (msg);
            break;
        case CMD_FSYNCDIR:
            processFsyncDir              (msg);
            break;
        case CMD_GETATTR:
            processGetAttr               (msg);
            break;
        case CMD_LINK:
            processLink                  (msg);
            break;
        case CMD_LOOKUP:
            processLookup                (msg);
            break;
        case CMD_MKDIR:
            processMkdir                 (msg);
            break;
        case CMD_MKNOD:
            processMknod                 (msg);
            break;
        case CMD_OPEN:
            processOpen                  (msg);
            break;
        case CMD_OPENDIR:
            processOpendir               (msg);
            break;
        case CMD_READ:
            processRead                  (msg);
            break;
        case CMD_READDIR:
            processReadDir               (msg);
            break;
        case CMD_READDIRPLUS:
            processReadDirPlus           (msg);
            break;
        case CMD_READLINK:
            processReadLink              (msg);
            break;
        case CMD_RELEASE:
            processRelease               (msg);
            break;
        case CMD_RELEASEDIR:
            processReleaseDir            (msg);
            break;
        case CMD_RENAME:
            processRename                (msg);
            break;
        case CMD_RMDIR:
            processRmdir                 (msg);
            break;
        case CMD_SETATTR:
            processSetAttr               (msg);
            break;
        case CMD_STATFS:
            processStatFS                (msg);
            break;
        case CMD_SYMLINK:
            processSymlink               (msg);
            break;
        case CMD_UNLINK:
            processUnlink                (msg);
            break;
        case CMD_WRITE:
            processWrite                 (msg);
            break;
        default:
            processUnsupported           (msg);
        }
    }
    
    protected void processCloseChannel(ZKFSRemoteMessageIncoming msg) throws IOException {
        close();
    }
    
    protected void processCopyFileRange(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long srcInodeId            = buf.getLong();
            long srcOffset             = buf.getLong();
             int srcFd                 = buf.getInt ();
             
            long destInodeId           = buf.getLong();
            long destOffset            = buf.getLong();
             int destFd                = buf.getInt ();
             
            long length                = buf.getLong();
            
            // 32-bit flags left in buffer but we don't use them
            
            ZKFile src                 = file(srcFd ,  srcInodeId);
            ZKFile dest                = file(destFd, destInodeId);
             
             int gobbleSize            = 1024*1024;
            long totalWritten          = 0;
             
            long srcPos                =  src.pos();
            long destPos               = dest.pos();
             
             src.seek( srcOffset, SEEK_SET);
            dest.seek(destOffset, SEEK_SET);
             
            try {
                byte[] data            = new byte[gobbleSize];
                while(totalWritten < length) {
                    int readLen        = src.read (data, 0, gobbleSize);
                    dest.write(data);
                     
                    totalWritten      += readLen;
                }
            } finally {
                 src.seek(srcPos,  SEEK_SET);
                dest.seek(destPos, SEEK_SET);
            }
        });
    }
    
    protected void processCreate(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        parentInodeId  = buf.getLong ();
            int         mode           = buf.getInt  ();
            int         type           = buf.getInt  ();
            int         flags          = buf.getInt  ();
            String      name           = new String(remainder(buf));
            
            if(name.length() > ZKDirectory.MAX_NAME_LEN) {
                throw new EINVALException("file name too long (max " + ZKDirectory.MAX_NAME_LEN + ")");
            }
            
            if(type < 0 || type > TYPE_MAX_SUPPORTED) {
                throw new EINVALException("invalid file type: " + type);
            }
            
            ZKDirectory parent         = directory(-1, parentInodeId);
            Inode       inode;
            try {
                long    inodeId        = parent.inodeForName(name);
                inode                  = fs.getInodeTable().inodeWithId(inodeId);
            } catch(ENOENTException exc) {
                inode                  = fs.getInodeTable().issueInode();
                inode.getStat().setType(type);
                inode.getStat().setMode(mode);
            }
            
            inode.getStat().setType(type);
            
            ZKFile      file           = fs.open(inode, flags);
            int         fileDescriptor = nextFileDescriptor++;
            fileDescriptors.put(fileDescriptor, file);
            
            msg.respond(inode.serialize());
            msg.respond(Util.serializeInt(fileDescriptor));
        });
    }
    
    protected void processDestroy(ZKFSRemoteMessageIncoming msg) throws IOException {
        close();
        msg.respond();
    }
    
    protected void processFsync(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long       inodeId         = buf.getLong ();
            @SuppressWarnings("unused")
            boolean    datasync        = buf.get     () != 0;
            int        fd              = buf.getInt  ();
            
            // datasync=false means we're allowed to defer updating other inode info, but that's not supported right now
            ZKFile     file            = file(fd, inodeId);
            file.flush();
            
            msg.respond();
        });
    }
    
    protected void processFsyncDir(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long       inodeId         = buf.getLong ();
            @SuppressWarnings("unused")
            boolean    datasync        = buf.get     () != 0;
            int        fd              = buf.getInt  ();
            
            ZKFile     file            = directory(fd, inodeId);
            file.flush();
            
            msg.respond();
        });
    }
    
    protected void processGetAttr(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long       inodeId         = buf.getLong ();
            int        fd              = buf.getInt  ();
            ZKFile     file            = file(fd, inodeId);
            
            msg.respond(file.getInode().serialize());
        });
    }
    
    protected void processLink(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        inodeId        = buf.getLong ();
            long        newParent      = buf.getLong ();
            String      newName        = new String(remainder(buf));
            
            ZKDirectory parent         = directory(-1, newParent);
            Inode       inode          = fs.getInodeTable().inodeWithId(inodeId);
            parent.link(inode, newName);
            
            msg.respond();
        });
    }

    protected void processLookup(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        parentInodeId  = buf.getLong ();
            String      name           = new String(remainder(buf));
            
            ZKDirectory parent         = directory(-1, parentInodeId);
            long        inodeId        = parent.inodeForName(name);
            Inode       inode          = fs.getInodeTable().inodeWithId(inodeId);
            
            msg.respond(inode.serialize());
        });
    }
    
    protected void processMkdir(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        parentInodeId  = buf.getLong ();
            int         mode           = buf.getInt  ();
            String      name           = new String(remainder(buf));
            
            ZKDirectory parent         = directory(-1, parentInodeId);
            ZKDirectory child          = parent.mkdir(name, mode);
            
            msg.respond(child.getInode().serialize());
        });
    }
    
    protected void processMknod(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        parentInodeId  = buf.getLong ();
            int         type           = buf.getInt  ();
            int         mode           = buf.getInt  ();
            String      name           = new String(varlen(buf));
            int         devMajor       = 0,
                        devMinor       = 0;
            
            if(type == TYPE_BLOCK_DEVICE || type == TYPE_CHARACTER_DEVICE) {
                devMajor               = buf.getInt ();
                devMinor               = buf.getInt ();
            }
            
            ZKDirectory parent         = directory(-1, parentInodeId);
            if(parent.contains(name)) throw new EEXISTSException(name);
            
            Inode       inode          = fs.getInodeTable().issueInode();
            inode.getStat().setMode(mode);
            inode.getStat().setType(type);
            
            if(inode.getStat().isDevice()) {
                inode.getStat().setDevMajor(devMajor);
                inode.getStat().setDevMajor(devMinor);
            }
            
            parent.link(inode, name);
            
            msg.respond(inode.serialize());
        });
    }
    
    protected void processOpen(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        inodeId        = buf.getLong ();
            int         mode           = buf.getInt  ();
            
            Inode       inode          = fs.getInodeTable().inodeWithId(inodeId);
            ZKFile      file           = new ZKFile(fs, inode, mode, true);
            int         fileDescriptor = nextFileDescriptor++;
            fileDescriptors.put(fileDescriptor, file);
            
            msg.respond(Util.serializeInt(fileDescriptor));
        });
    }
    
    protected void processOpendir(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        inodeId        = buf.getLong ();
            int         mode           = buf.getInt  ();
            
            Inode       inode          = fs.getInodeTable().inodeWithId(inodeId);
            ZKFile      file           = new ZKFile(fs, inode, mode, true);
            int         dirDescriptor  = nextFileDescriptor++; // file and directory descriptors come from same pool
            fileDescriptors.put(dirDescriptor, file);
            
            msg.respond(Util.serializeInt(dirDescriptor));
        });
    }
    
    protected void processRead(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        inodeId        = buf.getLong ();
            long        size           = buf.getLong ();
            long        offset         = buf.getLong ();
            int         fileDescriptor = buf.getInt  ();
            
            ZKFile      file           = file(fileDescriptor, inodeId);
            if(file.getSize() < offset) {
                throw new EINVALException("read offset exceeds file size");
            }
            
            file.seek(offset, File.SEEK_SET);
            int         gobbleSize     = 1024*64;
            long        bytesRead      = 0;
            byte[]      readBuf        = new byte[gobbleSize];
            boolean     finished       = false;
            
            while(!finished) {
                int maxReadLen         = (int) Math.min(gobbleSize,
                                                        size - bytesRead);
                int readLen            = file.read(readBuf, 0, maxReadLen);
                bytesRead             += readLen;
                finished              |= bytesRead >= size
                                      || readLen   <= 0;
                
                msg.respond(readBuf, finished);
            }
        });
    }
    
    protected void processReadDir(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        inodeId        = buf.getLong ();
            long        size           = buf.getLong ();
            long        offset         = buf.getLong ();
            int         dirDescriptor  = buf.getInt  ();
            
            ZKDirectory dir            = directory(dirDescriptor, inodeId);
            if(dir.getSize() < offset) {
                throw new EINVALException("read offset exceeds file size");
            }
            
            long        index          = -1;
            Map<String, Long> entries  = dir.getEntries();
            
            for(String name : entries.keySet()) {
                assert(name.length()   < 256);
                
                index++;
                if(index < offset)         continue;
                if(index - offset >= size) break;
                
                long    entryInodeId   = entries.get(name);
                Inode   entryInode     = fs.getInodeTable().inodeWithId(entryInodeId);
                Stat    entryStat      = entryInode.getStat();
                byte[]  nameLenBstr    = Util.serializeByte((byte) name.length());
                
                msg.respond(nameLenBstr,                              false);
                msg.respond(name.getBytes(),                          false);
                msg.respond(Util.serializeInt(entryStat.getMode()),   false);
                msg.respond(Util.serializeInt(entryStat.getType()),   false);
            }
            
            msg.respond();
        });
    }
    
    protected void processReadDirPlus(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        inodeId        = buf.getLong ();
            long        size           = buf.getLong ();
            long        offset         = buf.getLong ();
            int         dirDescriptor  = buf.getInt  ();
            
            ZKDirectory dir            = directory(dirDescriptor, inodeId);
            if(dir.getSize() < offset) {
                throw new EINVALException("read offset exceeds file size");
            }
            
            long        index          = -1;
            Map<String, Long> entries  = dir.getEntries();
            
            for(String name : entries.keySet()) {
                assert(name.length()   < 256);
                
                index++;
                if(index < offset)         continue;
                if(index - offset >= size) break;
                
                long    entryInodeId   = entries.get(name);
                Inode   entryInode     = fs.getInodeTable().inodeWithId(entryInodeId);
                byte[]  inodeBytes     = entryInode.serialize();
                byte[]  inodeLenBstr   = Util.serializeShort((short) inodeBytes.length);
                byte[]  nameLenBstr    = Util.serializeByte ((byte)  name      .length());
                
                assert(inodeBytes.length < 65536);
                
                msg.respond(nameLenBstr,     false);
                msg.respond(name.getBytes(), false);
                msg.respond(inodeLenBstr,    false);
                msg.respond(inodeBytes,      false);
            }
            
            msg.respond();
        });
    }
    
    protected void processReadLink(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        inodeId        = buf.getLong ();
            Inode       inode          = fs.getInodeTable().inodeWithId(inodeId);
            int         flags          = O_RDONLY
                                       | O_NOFOLLOW
                                       | O_LINK_LITERAL;
            
            if(!inode.getStat().isSymlink()) {
                throw new EINVALException("not a symlink");
            }
            
            try(ZKFile file = new ZKFile(fs, inode, flags, true)) {
                byte[]  target         = file.read();
                msg.respond(target);
            }
        });
    }
    
    protected void processRelease(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        inodeId        = buf.getLong ();
            int         fileDescriptor = buf.getInt  ();
            
            closeFile(fileDescriptor, inodeId);
            msg.respond();
        });
    }
    
    protected void processReleaseDir(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        inodeId        = buf.getLong ();
            int         dirDescriptor  = buf.getInt  ();
            
            closeFile(dirDescriptor, inodeId);
            msg.respond();
        });
    }
    
    protected void processRename(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        parentInodeId  = buf.getLong ();
            String      name           = new String(varlen(buf));
            long     newParentInodeId  = buf.getLong ();
            String   newName           = new String(varlen(buf));
            int         flags          = buf.getInt  ();

            boolean     flagNoReplace  = (flags & RENAME_NOREPLACE) != 0;
            boolean     flagExchange   = (flags & RENAME_EXCHANGE)  != 0;
            boolean     canExist       = !flagNoReplace || flagExchange;
            
            ZKDirectory    parent      = directory(-1,    parentInodeId);
            ZKDirectory newParent      = directory(-1, newParentInodeId);
            
            long        inodeId        = parent.inodeForName(name);
            boolean     newExists      = newParent.contains(newName);
            if(newExists && !canExist)   throw new EEXISTSException(newName);
            
            if(flagExchange) {
                /* Atomic exchange of files.
                 * "new" in this branch just means "the other existing file."
                 */
                
                if(!newExists)           throw new ENOENTException(newName);
                long    newInodeId     = newParent.inodeForName(newName);
                fs.lockedOperation(()->{
                    newParent.updateLink(   inodeId,    newName);
                    parent   .updateLink(newInodeId,       name);
                    return null;
                });
            } else {
                fs.lockedOperation(()->{
                   newParent.updateLink(inodeId, newName);
                   parent   .removeLink(name);
                   return null;
                });
            }
            
            msg.respond();
        });
    }
    
    protected void processRmdir(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        parentInodeId  = buf.getLong ();
            String      name           = new String(remainder(buf));
            
            ZKDirectory parent         = directory(-1, parentInodeId);
            
            fs.lockedOperation(()->{
                long        inodeId    = parent.inodeForName(name);
                Inode       inode      = fs.getInodeTable().inodeWithId(inodeId);
                
                if(!inode.getStat().isDirectory()) {
                    throw new EISNOTDIRException(name);
                }
                
                try(ZKDirectory directory = fs.opendirSemicache(inode)) {
                    if(directory.list().size() > 0) {
                        throw new ENOTEMPTYException(name);
                    }
                }
                
                parent.unlink(name);
                
                return null;
            });
            
            msg.respond();
        });
    }
    
    protected void processSetAttr(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        inodeId        = buf.getLong ();
            int         fileDescriptor = buf.getInt  ();
            int         flags          = buf.get     ();
            
            ZKFile      file           = file(fileDescriptor, inodeId);
            Stat        stat           = file.getStat();
            boolean     killPriv       = (flags & ATTR_FLAG_KILLPRIV) != 0;
            boolean     privAffected   = false;
            
            while(buf.hasRemaining()) {
                int         item       = buf.get     ();
                
                switch(item) {
                case ATTR_ITEM_MODE:
                    int mode           = buf.getInt  ();
                    int type           = buf.getInt  ();
                    stat.setMode(mode);
                    stat.setType(type);
                    break;
                case ATTR_ITEM_UID:
                    privAffected       = true;
                    int uid            = buf.getInt  ();
                    stat.setUid(uid);
                    break;
                case ATTR_ITEM_GID:
                    privAffected       = true;
                    int gid            = buf.getInt  ();
                    stat.setGid(gid);
                    break;
                case ATTR_ITEM_USER:
                    privAffected       = true;
                    String user        = new String(varlen(buf));
                    stat.setUser(user);
                    break;
                case ATTR_ITEM_GROUP:
                    privAffected       = true;
                    String group       = new String(varlen(buf));
                    stat.setGroup(group);
                    break;
                case ATTR_ITEM_SIZE:
                    privAffected       = true;
                    long size          = buf.getLong ();
                    file.truncate(size);
                    break;
                case ATTR_ITEM_ATIME:
                    long atime         = buf.getLong ();
                    stat.setAtime(atime);
                    break;
                case ATTR_ITEM_MTIME:
                    long mtime         = buf.getLong ();
                    stat.setMtime(mtime);
                    break;
                case ATTR_ITEM_ATIME_NOW:
                    stat.setAtime(Util.currentTimeNanos());
                    break;
                case ATTR_ITEM_MTIME_NOW:
                    stat.setMtime(Util.currentTimeNanos());
                    break;
                case ATTR_ITEM_CTIME:
                    long ctime         = buf.getLong ();
                    stat.setCtime(ctime);
                    break;
                default:
                    throw new EINVALException("invalid item type " + item);
                }
            }
            
            if(privAffected && killPriv) {
                int mask = ~(MODE_SUID | MODE_SGID);
                stat.setMode(stat.getMode() & mask);
            }
            
            fs.markDirty();
            fs.notifyChange(file.getPath(), stat); // file's path is nonsense, but we need to notify so autocommit sees this
            msg.respond(file.getInode().serialize());
            
            msg.respond();
        });
    }
    
    protected void processStatFS(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            msg.respond(fs.fsStat().serialize());
        });
    }
    
    protected void processSymlink(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        parentInodeId  = buf.getLong ();
            String      name           = new String(varlen(buf));
            String      target         = new String(remainder(buf));
            
            fs.lockedOperation(()->{
                Inode      parentInode = fs.getInodeTable().inodeWithId(parentInodeId);
                ZKDirectory parent     = fs.opendirSemicache(parentInode);
                Inode       inode      = fs.getInodeTable().issueInode();
                
                inode.getStat().makeSymlink();
                parent.link(inode, name);
                
                try(ZKFile  file       = fs.open(inode,
                                                 O_WRONLY
                                               | O_NOFOLLOW
                                               | O_CREAT
                                               | O_LINK_LITERAL))
                {
                    file.write(target.getBytes());
                }
                
                msg.respond(inode.serialize());
                
                return null; 
            });
        });
    }
    
    protected void processUnlink(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.waitForFinish((buf)->{
            long        parentInodeId  = buf.getLong ();
            String      name           = new String(remainder(buf));
            
            fs.lockedOperation(()->{
                Inode      parentInode = fs.getInodeTable().inodeWithId(parentInodeId);
                ZKDirectory parent     = fs.opendirSemicache(parentInode);
                parent.unlink(name);
                
                return null;
            });
            
        });
    }
    
    protected void processWrite(ZKFSRemoteMessageIncoming msg) throws IOException {
        msg.read(12, (hdr)->{
            long        inodeId        = hdr.getLong ();
            int         fileDescriptor = hdr.getInt  ();
            
            ZKFile      file           = file(fileDescriptor, inodeId);
            int         gobbleSize     = 1024*64;
            AtomicLong  bytesWritten   = new AtomicLong();
            
            msg.readUntilFinished(gobbleSize, (buf)->{
                if(buf == null) {
                    msg.respond(Util.serializeLong(bytesWritten.get()));
                    return;
                }
                
                file.write(buf.array(), buf.position(), buf.remaining());
                bytesWritten.addAndGet(buf.remaining());
                buf.position(buf.position() + buf.remaining());
            });
        });
    }
    
    protected void processUnsupported(ZKFSRemoteMessageIncoming msg) throws EINVALException {
        throw new EINVALException("unsupported command: " + msg.cmd());
    }
    
    
    public void close() throws IOException {
        for(ZKFile file : fileDescriptors.values()) {
            try {
                file.close();
            } catch (IOException exc) {
                logger.error("FSRemoteChannel {}: Encountered exception closing file {} during fs close",
                        fs.toString(),
                        file.getPath(),
                        exc);
            }
        }
        
        fs.close();
    }
    
    public ZKFile file(int fd, long inodeId) throws IOException {
        ZKFile          file           = fileDescriptors.getOrDefault(fd, null);
        if(file == null) {
            Inode       inode          = fs.getInodeTable().inodeWithId(inodeId);
            file                       = fs.open(inode, O_RDWR);
        }

        return file;
    }
    
    public ZKDirectory directory(int fd, long inodeId) throws IOException {
        ZKDirectory     dir            = directoryDescriptors.getOrDefault(fd, null);
        if(dir == null) {
            dir                        = (ZKDirectory) fs.lockedOperation(()->{
                Inode   inode          = fs.getInodeTable().inodeWithId(inodeId);
                return fs.opendirSemicache(inode);
            });
        }
        
        return dir;
    }
    
    public void closeFile(int fd, long inodeId) throws IOException {
        ZKFile          file           = fileDescriptors.remove(fd);
        if(file == null) return;
        
        file.close();
    }
    
    public void closeDirectory(int fd, long inodeId) throws IOException {
        ZKDirectory     dir            = directoryDescriptors.remove(fd);
        if(dir == null) return;
        
        dir.close();
    }
    
    public byte[] remainder(ByteBuffer buf) {
        byte[]         bytes           = new byte[buf.remaining()];
        buf.get(bytes);
        
        return bytes;
    }
    
    public byte[] fixedlen(int len, ByteBuffer buf) {
        byte[]          result         = new byte[len];
        buf.get(result);
        return result;
    }
    
    public byte[] varlen(ByteBuffer buf) {
        int             len            = buf.get();
        return fixedlen(len, buf);
    }
}
