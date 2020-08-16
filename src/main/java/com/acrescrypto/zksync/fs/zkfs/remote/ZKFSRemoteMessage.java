package com.acrescrypto.zksync.fs.zkfs.remote;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ZKFSRemoteMessage {
    public interface WaitCallback {
        public void ready(ByteBuffer data) throws IOException;
    }

    public final static int CMD_CLOSE_CHANNEL     =  0;
    public final static int CMD_OPEN_FS           =  1;
    
    public final static int CMD_ACCESS            =  2;
    public final static int CMD_BMAP              =  3;
    public final static int CMD_COPY_FILE_RANGE   =  4;
    public final static int CMD_CREATE            =  5;
    public final static int CMD_DESTROY           =  6;
    public final static int CMD_FALLOCATE         =  7;
    public final static int CMD_FLOCK             =  8;
    public final static int CMD_FLUSH             =  9;
    public final static int CMD_FORGET            = 10;
    public final static int CMD_FSYNC             = 11;
    public final static int CMD_FSYNCDIR          = 12;
    public final static int CMD_GETATTR           = 13;
    public final static int CMD_GETLK             = 14;
    public final static int CMD_GETXATTR          = 15;
    public final static int CMD_IOCTL             = 16;
    public final static int CMD_LINK              = 17;
    public final static int CMD_LISTXATTR         = 18;
    public final static int CMD_LOOKUP            = 19;
    public final static int CMD_LSEEK             = 20;
    public final static int CMD_MKDIR             = 21;
    public final static int CMD_MKNOD             = 22;
    public final static int CMD_OPEN              = 23;
    public final static int CMD_OPENDIR           = 24;
    public final static int CMD_POLL              = 25;
    public final static int CMD_READ              = 26;
    public final static int CMD_READDIR           = 27;
    public final static int CMD_READDIRPLUS       = 28;
    public final static int CMD_READLINK          = 29;
    public final static int CMD_RELEASE           = 30;
    public final static int CMD_RELEASEDIR        = 31;
    public final static int CMD_REMOVEXATTR       = 32;
    public final static int CMD_RENAME            = 33;
    public final static int CMD_RETRIEVE_REPLY    = 34;
    public final static int CMD_RMDIR             = 35;
    public final static int CMD_SETATTR           = 36;
    public final static int CMD_SETLK             = 37;
    public final static int CMD_SETXATTR          = 38;
    public final static int CMD_STATFS            = 39;
    public final static int CMD_SYMLINK           = 40;
    public final static int CMD_UNLINK            = 41;
    public final static int CMD_WRITE             = 42;
    
    public final static int RENAME_NOREPLACE      = (1 << 0);
    public final static int RENAME_EXCHANGE       = (1 << 1);
    
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

    public int cmd() {
        return 0;
    }
    
    public void waitForFinish(WaitCallback callback) throws IOException {
        
    }
    
    public void readUntilFinished(int length, WaitCallback callback) throws IOException {
    }
    
    public void read(int length, WaitCallback callback) throws IOException {
    }
    
    public void respond(byte[] data) {
    }

    public void respond() {
        // TODO Auto-generated method stub
        
    }

    public void respond(byte[] readBuf, boolean finished) {
        // TODO Auto-generated method stub
        
    }
    
    public void finished() {
    }
}
