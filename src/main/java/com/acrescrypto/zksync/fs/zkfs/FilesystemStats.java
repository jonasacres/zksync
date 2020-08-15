package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FilesystemStats {
    // fields based on `struct statvfs`, as defined in statvfs(3)
    protected long blockSize,          // Filesystem block size                        (f_bsize)
                   fragmentSize,       // Fragment size                                (f_frsize)
                   blocks,             // Size of fs in fragments                      (f_blocks)
                   freeBlocks,         // Number of free blocks                        (f_bfree)
                   availableBlocks,    // Number of free blocks for unprivileged users (f_bavail)
                                       
                   files,              // Number of inodes                             (f_files)
                   freeInodes,         // Number of free inodes                        (f_ffree)
                   availableInodes,    // Number of free inodes for unprivileged users (f_favail)
                   
                   filesystemId,       // Filesystem ID                                (f_fsid)
                   mountFlags,         // Mount flags                                  (f_flag)
                   nameMax;            // Max filename length                          (f_namemax)
    
    public FilesystemStats(ZKFS fs) throws IOException {
        this.blockSize                 = fs.getArchive().getConfig().getPageSize();
        this.fragmentSize              = fs.getArchive().getConfig().getPageSize();
        this.blocks                    = fs.storageSize() / this.blockSize;
        this.freeBlocks                = -1;
        this.availableBlocks           = -1;
        
        this.files                     = fs.getInodeTable().nextInodeId()
                                       - fs.getInodeTable().freelist.getSize();
        this.freeInodes                = -1;
        this.availableInodes           = -1;
        
        this.filesystemId              = ByteBuffer.wrap(fs.getArchive().getConfig().getArchiveId()).getLong();
        this.mountFlags                = 0;
        this.nameMax                   = ZKDirectory.MAX_NAME_LEN;
    }
    
    public byte[] serialize() {
        byte[]     array = new byte[8*11]; // 11 longs, each 8 bytes
        ByteBuffer buf   = ByteBuffer.wrap(array);
        
        buf.putLong(blockSize);
        buf.putLong(fragmentSize);
        buf.putLong(blocks);
        buf.putLong(freeBlocks);
        buf.putLong(availableBlocks);
        
        buf.putLong(files);
        buf.putLong(freeInodes);
        buf.putLong(availableInodes);
        
        buf.putLong(filesystemId);
        buf.putLong(mountFlags);
        buf.putLong(nameMax);
        
        return array;
    }
}
