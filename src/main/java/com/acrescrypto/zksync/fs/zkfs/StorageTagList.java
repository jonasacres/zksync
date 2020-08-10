package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.DirectoryTraverser;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;

public class StorageTagList {
    protected ConcurrentHashMap<Long,StorageTag> allPageTags = new ConcurrentHashMap<>();
    protected ZKArchive                          archive;
    protected File                               file;
    protected SnoozeThread                       flushThread;
    
    public StorageTagList(ZKArchive archive) throws IOException {
        this.archive = archive;
        this.file    = openFile();
        
        StorageTag configTag = archive.getConfig().tag();
        allPageTags.put(configTag.shortTag(), configTag);
        read();
    }
    
    public void close() {
        if(this.flushThread != null) this.flushThread.cancel();
        
        try {
            if(file != null) {
                file.close();
            }
        } catch(IOException exc) {
            archive.logger.error("ZKFS {}: Encountered exception closing storage tag list",
                    Util.formatArchiveId(archive.getConfig().getArchiveId()),
                    exc);
        }
    }
    
    public void resync() throws IOException {
        CryptoSupport crypto = archive.getCrypto();
        StorageTag configTag = archive.getConfig().tag();
        
        allPageTags.clear();
        allPageTags.put(configTag.shortTag(), configTag);
        
        try(Directory dir = archive.getStorage().opendir("/")) {
            DirectoryTraverser traverser = new DirectoryTraverser(archive.getStorage(), dir);
            while(traverser.hasNext()) {
                StorageTag tag = new StorageTag(crypto, traverser.next().getPath());
                allPageTags.put(tag.shortTag(), tag);
            }
        }
        
        write();
    }
    
    public long storedPageSize() {
        long size = archive.getConfig().getSerializedPageSize();
        return allPageTags.size() * size;
    }
    
    public boolean hasPageTag(StorageTag pageTag) throws IOException {
        return hasPageTag(pageTag, true);
    }
    
    public boolean hasPageTag(StorageTag pageTag, boolean checkStorage) throws IOException {
        StorageTag existing = allPageTags.get(pageTag.shortTagPreserialized());
        if(existing == null) {
            if(!checkStorage) return false;
            
            boolean actuallyWeDoHaveIt = archive.getConfig().getCacheStorage().exists(pageTag.path());
            if(actuallyWeDoHaveIt) {
                add(pageTag);
                return true;
            } else {
                return false;
            }
        };
        
        return existing.equals(pageTag);
    }
    
    public void add(StorageTag pageTag) throws IOException {
        allPageTags.put(pageTag.shortTagPreserialized(), pageTag);
        append(pageTag);
    }
    
    protected void flushAfterDelay() {
        if(this.flushThread == null) {
            int interval = archive.getMaster().getGlobalConfig().getInt("fs.settings.tagCacheFlushIntervalMs"),
                maxDelay = archive.getMaster().getGlobalConfig().getInt("fs.settings.tagCacheMaxFlushDelayMs");
            this.flushThread = new SnoozeThread(interval, maxDelay, true, ()->{
                this.flushThread = null;
               if(file != null) {
                   try {
                       file.flush();
                   } catch (IOException exc) {
                       archive.logger.error("ZKFS {}: Caught exception writing tagcache",
                               Util.formatArchiveId(archive.getConfig().getArchiveId()),
                               exc);
                   }
               }
            });
        } else {
            this.flushThread.snooze();
        }
    }
    
    protected File openFile() throws IOException {
        if(file != null) file.close();
        String path = ".zksync/archive/tagcache";
        
        archive.getConfig().getLocalStorage().mkdirp(archive.getConfig().getLocalStorage().dirname(path));
        return archive
                .getConfig()
                .getLocalStorage()
                .open(path, File.O_RDWR|File.O_CREAT|File.O_APPEND);
    }
    
    protected void append(StorageTag tag) throws IOException {
        file.write(tag.getTagBytesPreserialized());
        flushAfterDelay();
    }
    
    protected void read() throws IOException {
        CryptoSupport crypto = archive.getCrypto();

        file.rewind();
        byte[] data = new byte[crypto.hashLength()];
        
        while(file.hasData()) {
            file.read(data, 0, data.length);
            StorageTag tag = new StorageTag(crypto, data);
            
            allPageTags.put(tag.shortTagPreserialized(), tag);
        }
        
        long encodedTags = file.pos() / crypto.hashLength();
        if(allPageTags.size() < 0.9 * encodedTags) {
            write();
        }
    }

    protected void write() throws IOException {
        file.truncate(0);
        file.rewind();
        
        for(StorageTag tag : allPageTags.values()) {
            file.write(tag.getTagBytesPreserialized());
        }
        
        file.flush();
    }

    public Collection<StorageTag> allPageTags() {
        return allPageTags.values();
    }
}
