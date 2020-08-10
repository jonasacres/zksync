package com.acrescrypto.zksync.fs.localfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.CommandFailedException;
import com.acrescrypto.zksync.exceptions.EACCESException;
import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.ENOTEMPTYException;
import com.acrescrypto.zksync.fs.*;
import com.acrescrypto.zksync.utility.Util;

public class LocalFS extends FS {
    protected HashMap<Integer,CachedName> cachedUserNames = new HashMap<>();
    protected HashMap<Integer,CachedName> cachedGroupNames = new HashMap<>();
    private Logger logger = LoggerFactory.getLogger(LocalFS.class);

    protected class CachedName {
        String name;
        long id;
        long timestamp;
        public final static long EXPIRATION_INTERVAL_MS = 10000;

        public CachedName(String name, long id) {
            this.name = name;
            this.id = id;
            this.timestamp = Util.currentTimeMillis();
        }

        public boolean isExpired() {
            return Util.currentTimeMillis() > timestamp + EXPIRATION_INTERVAL_MS;
        }

        public long getId() {
            return id;
        }
    }

    public LocalFS(String root) {
        this(FSPath.with(root));
    }

    public LocalFS(FSPath root) {
        this.root = root;
    }

    @Override
    public FSPath root() {
        if(root == null) {
            if(Util.isWindows()) {
                root = FSPath.with("C:\\");
            } else {
                root = FSPath.with("/");
            }
        }

        return root;
    }

    @Override
    public long size(String path, boolean followSymlinks) throws IOException {
        LinkOption[] linkOpt = followSymlinks
                ? new LinkOption[0]
                : new LinkOption[] { LinkOption.NOFOLLOW_LINKS };
        BasicFileAttributes attr = Files.readAttributes(
                Paths.get(path),
                BasicFileAttributes.class,
                linkOpt);
        return attr.size();
    }

    private Stat statWithLinkOption(String pathStr, LinkOption... linkOpt) throws IOException {
        return isWindows()
                ? statWithLinkOptionWindows(pathStr, linkOpt)
                : statWithLinkOptionPosix  (pathStr, linkOpt);
    }

    private Stat statWithLinkOptionWindows(String pathStr, LinkOption... linkOpt) throws IOException {
        Stat stat = new Stat();
        Path path = qualifiedPathNative(pathStr);

        /* TODO: Profiler shows that in a test like indefiniteTestComplexManyPeerEquivalent, we
         * burn most of our time on checking user and group names (string, not ID). In practice, most
         * use cases will never care. Consider having the ability to ignore uid/gid/username/group name
         * and force them to always be "easysafe" or something. 
         */

        try {
            BasicFileAttributes attr = Files.readAttributes(path,
                    BasicFileAttributes.class,
                    linkOpt);
            stat.setMtime(attr.lastModifiedTime().to(TimeUnit.NANOSECONDS));
            stat.setAtime(attr.lastAccessTime  ().to(TimeUnit.NANOSECONDS));
            stat.setCtime(attr.creationTime    ().to(TimeUnit.NANOSECONDS));
            stat.setMode(getFilePermissions(attr, path.toString()));

            if(!isWindows()) {
                stat.setUid((Integer) Files.getAttribute(path, "unix:uid", linkOpt));
                stat.setGid((Integer) Files.getAttribute(path, "unix:gid", linkOpt));
                stat.setGroup(cachedGroupName(stat.getGid(), path, linkOpt));
            }

            stat.setUser(cachedUserName(stat.getUid(), path, linkOpt));
            int type = getStatType(attr);
            if(type >= 0) {
                stat.setType(type);
            } else {
                try {
                    scrapeLSForUnixSpecific(stat, path.toString());
                } catch(CommandFailedException exc) {
                    throw new ENOENTException(path.toString());
                }
            }

            if(stat.isSymlink()) {
                stat.setSize(0);
            } else {
                stat.setSize(attr.size());
            }
        } catch(NoSuchFileException exc) {
            throw new ENOENTException(path.toString());
        } catch(java.nio.file.AccessDeniedException exc) {
            throw new EACCESException(path.toString());
        }

        return stat;
    }


    private Stat statWithLinkOptionPosix(String pathStr, LinkOption... linkOpt) throws IOException {
        Stat stat = new Stat();
        Path path = qualifiedPathNative(pathStr);

        /* TODO: Profiler shows that in a test like indefiniteTestComplexManyPeerEquivalent, we
         * burn most of our time on checking user and group names (string, not ID). In practice, most
         * use cases will never care. Consider having the ability to ignore uid/gid/username/group name
         * and force them to always be "easysafe" or something. 
         */

        try {
            PosixFileAttributes attr = Files.readAttributes(path, PosixFileAttributes.class, linkOpt);
            
            stat.setMtime(attr.lastModifiedTime().to(TimeUnit.NANOSECONDS));
            stat.setAtime(attr.lastAccessTime()  .to(TimeUnit.NANOSECONDS));
            stat.setCtime(attr.creationTime()    .to(TimeUnit.NANOSECONDS));
            stat.setMode(getFilePermissions(attr));
            if(!isWindows()) {
                stat.setUid((Integer) Files.getAttribute(path, "unix:uid", linkOpt));
                stat.setGid((Integer) Files.getAttribute(path, "unix:gid", linkOpt));
                stat.setGroup(cachedGroupName(stat.getGid(), path, linkOpt));
            }
            stat.setUser(cachedUserName(stat.getUid(), path, linkOpt));
            int type = getStatType(attr);
            if(type >= 0) {
                stat.setType(type);
            } else {
                try {
                    scrapeLSForUnixSpecific(stat, path.toString());
                } catch(CommandFailedException exc) {
                    throw new ENOENTException(path.toString());
                }
            }

            if(stat.isSymlink()) {
                stat.setSize(0);
            } else {
                stat.setSize(attr.size());
            }

            stat.setInodeId((Long) Files.getAttribute(path, "unix:ino", linkOpt));
        } catch(NoSuchFileException exc) {
            throw new ENOENTException(path.toString());
        } catch(java.nio.file.AccessDeniedException exc) {
            throw new EACCESException(path.toString());
        }

        return stat;
    }

    private int getStatType(BasicFileAttributes attr) {
        if       (attr.isDirectory()   ) {
            return Stat.TYPE_DIRECTORY;
        } else if(attr.isRegularFile() ) {
            return Stat.TYPE_REGULAR_FILE;
        } else if(attr.isSymbolicLink()) {
            return Stat.TYPE_SYMLINK;
        } else {
            return -1;
        }
    }

    private void scrapeLSForUnixSpecific(Stat stat, String path) throws IOException {
        String[] out = runCommand(new String[] { "ls", "-l", path }).split("\\s+");
        switch(out[0].charAt(0)) {
        case 'p':
            stat.setType(Stat.TYPE_FIFO);
            break;
        case 'b':
        case 'c':
            stat.setType(out[0].charAt(0) == 'b' ? Stat.TYPE_BLOCK_DEVICE : Stat.TYPE_CHARACTER_DEVICE);
            stat.setDevMajor(Integer.parseInt(out[4].substring(0, out[4].length()-1)));
            stat.setDevMinor(Integer.parseInt(out[5]));
            break;
        default:
            throw new UnsupportedOperationException(path + ": unknown file type: " + out[0].charAt(0));
        }		
    }

    private int getFilePermissions(PosixFileAttributes attr) throws IOException {
        int mode = 0;
        Set<PosixFilePermission> perms = attr.permissions();

        if(perms.contains(PosixFilePermission.OTHERS_EXECUTE)) mode |= 0001;
        if(perms.contains(PosixFilePermission.OTHERS_WRITE)) mode |= 0002;
        if(perms.contains(PosixFilePermission.OTHERS_READ)) mode |= 0004;

        if(perms.contains(PosixFilePermission.GROUP_EXECUTE)) mode |= 0010;
        if(perms.contains(PosixFilePermission.GROUP_WRITE)) mode |= 0020;
        if(perms.contains(PosixFilePermission.GROUP_READ)) mode |= 0040;

        if(perms.contains(PosixFilePermission.OWNER_EXECUTE)) mode |= 0100;
        if(perms.contains(PosixFilePermission.OWNER_WRITE)) mode |= 0200;
        if(perms.contains(PosixFilePermission.OWNER_READ)) mode |= 0400;

        return mode;
    }

    private int getFilePermissions(BasicFileAttributes attr, String name) throws IOException {
        int mode = 0666; // in windowsland, we'll just say everything is a+rw, and a+x if .com, .exe, .bat
        String[] executableExtensions = {
                ".exe",
                ".bat",
                ".com",
                ".msi",
                ".cmd",
                ".vbs",
                ".jse",
                ".wsf",
                ".wsh",
                ".psc1"
        };

        for(String extension : executableExtensions) {
            if(!name.endsWith(extension)) continue;

            mode |= 0111;
            break;
        }

        return mode;
    }

    private boolean isWindows() {
        return Util.isWindows();
    }

    private String runCommand(String[] args) throws IOException {
        Process process = null;
        byte[] buf = new byte[1024];

        process = new ProcessBuilder(args)
                .start();

        try {
            process.waitFor();
            process.getInputStream().read(buf);
            if(process.exitValue() != 0) {
                throw new CommandFailedException(String.join(" ", args));
            }
            return new String(buf);
        } catch (InterruptedException e) {
            throw new IOException();
        }
    }

    @Override
    public Stat stat(String path) throws IOException {
        return statWithLinkOption(path);
    }

    @Override
    public Stat lstat(String path) throws IOException {
        return statWithLinkOption(path, LinkOption.NOFOLLOW_LINKS);
    }

    @Override
    public LocalDirectory opendir(String path) throws IOException {
        return opendir(path, stat(path));
    }

    @Override
    public LocalDirectory opendir(String path, Stat stat) throws IOException {
        return new LocalDirectory(this, path, stat);
    }

    @Override
    public void mv(String oldPath, String newPath) throws ENOENTException, IOException {
        CopyOption[] opts = new CopyOption[] { StandardCopyOption.REPLACE_EXISTING };
        String relocatedPath = Paths.get(newPath, basename(oldPath)).toString();
        String targetPath = newPath;

        Stat existingStat = stat(oldPath);

        try {
            Stat newPathStat = stat(newPath);
            if(newPathStat.isDirectory()) {
                try {
                    Stat reloStat = stat(relocatedPath);
                    if(existingStat.isDirectory()) {
                        if(reloStat.isDirectory()) {
                            try(LocalDirectory dir = opendir(relocatedPath)) {
                                if(!dir.list().isEmpty()) {
                                    throw new ENOTEMPTYException(relocatedPath);
                                }
                            }

                            rmdir(relocatedPath);
                            targetPath = relocatedPath;
                        } else {
                            throw new EEXISTSException(newPath);
                        }
                    } else {
                        throw new EEXISTSException(relocatedPath);
                    }
                } catch(ENOENTException exc) {
                    targetPath = relocatedPath;
                }
            } else if(existingStat.isDirectory()) {
                throw new EEXISTSException(newPath);
            }
        } catch(ENOENTException exc) {}

        try {
            Files.move(qualifiedPathNative(oldPath), qualifiedPathNative(targetPath), opts);
        } catch(DirectoryNotEmptyException exc) {
            try {
                Stat reloStat = stat(relocatedPath);
                if(reloStat.isDirectory()) {
                    throw new ENOTEMPTYException(relocatedPath);
                } else {
                    throw new EEXISTSException(relocatedPath);
                }
            } catch(ENOENTException exc2) {
                logger.error("Unexpected exception handling mv " + oldPath + " " + newPath, exc2);
                throw exc2;
            }
        }
    }

    @Override
    public void mkdir(String path) throws IOException {
        logger.debug("LocalFS {}: mkdir {}", root, path);
        Files.createDirectory(qualifiedPathNative(path));
    }

    @Override
    public void mkdirp(String path) throws IOException {
        logger.debug("LocalFS {}: mkdirp {}", root, path);
        Files.createDirectories(qualifiedPathNative(path));
    }

    @Override
    public void rmdir(String path) throws IOException {
        logger.debug("LocalFS {}: rmdir {}", root, path);
        Path p = qualifiedPathNative(path);
        if(!Files.exists(p)) throw new ENOENTException(path);
        if(!Files.isDirectory(p)) throw new IOException(path + ": not a directory");
        Files.delete(p);
    }

    @Override
    public void unlink(String path) throws IOException {
        logger.debug("LocalFS {}: unlink {}", root, path);
        try {
            Files.delete(qualifiedPathNative(path));
        } catch(NoSuchFileException exc) {
            throw new ENOENTException(path);
        }
    }

    @Override
    public void link(String source, String dest) throws IOException {
        logger.debug("LocalFS {}: link {} -> {}", root, source, dest);
        try {
            Files.createLink(qualifiedPathNative(dest), qualifiedPathNative(source));
        } catch(FileAlreadyExistsException exc) {
            throw new EEXISTSException(qualifiedPath(source).toString());
        } catch(NoSuchFileException exc) {
            throw new ENOENTException(source);
        }
    }

    @Override
    public void symlink(String source, String dest) throws IOException {
        if(Util.isWindows()) throw new UnsupportedOperationException();

        logger.debug("LocalFS {}: symlink {} -> {}", root, source, dest);
        Path trueSource = source.substring(0, 1).equals("/")
                          ? qualifiedPath(source)
                          : Paths.get(source);
        assertPathInScope(trueSource.toString());
        Files.createSymbolicLink(qualifiedPathNative(dest), trueSource);
    }

    @Override
    public void symlink_unsafe(String source, String dest) throws IOException {
        logger.debug("LocalFS {}: symlink_unsafe {} -> {}", root, source, dest);
        Path psource = Paths.get(source);
        Files.createSymbolicLink(qualifiedPathNative(dest), psource);
    }

    @Override
    public String readlink(String link) throws IOException {
        try {
            FSPath target = FSPath.with(Files.readSymbolicLink(qualifiedPathNative(link)).toString());
            if(target.descendsFrom(root)) {
                return target.isAbsolute()
                        ? root.relativize(target).makeAbsolute().toNative()
                        : root.relativize(target).toNative();
            }

            return target.toNative();
        } catch(NoSuchFileException exc) {
            throw new ENOENTException(link);
        }
    }

    @Override
    public String readlink_unsafe(String link) throws IOException {
        try {
            return Files.readSymbolicLink(qualifiedPathNative(link)).toString();
        } catch(NoSuchFileException exc) {
            throw new ENOENTException(link);
        }
    }

    @Override
    public void mknod(String path, int type, int major, int minor) throws IOException {
        logger.debug("LocalFS {}: mknod {} {} {} {}", root, path, type, major, minor);
        if(isWindows()) throw new UnsupportedOperationException(path + ": Windows does not support devices");
        String typeStr;
        switch(type) {
        case Stat.TYPE_CHARACTER_DEVICE:
            typeStr = "c";
            break;
        case Stat.TYPE_BLOCK_DEVICE:
            typeStr = "b";
            break;
        default:
            throw new IllegalArgumentException(String.format("Illegal node type: %d", type));
        }

        try {
            runCommand(new String[] { "mknod", expandPath(path), typeStr, String.format("%d", major), String.format("%d", minor) });
        } catch(CommandFailedException exc) {
            // ugly how we're doing this, but communicate that we can't do this on this system/user
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void mkfifo(String path) throws IOException {
        logger.debug("LocalFS {}: mkfifo {}", root, path);
        if(isWindows()) throw new UnsupportedOperationException(path + ": Windows does not support named pipes");
        try {
            runCommand(new String[] { "mkfifo", expandPath(path) });
        } catch(CommandFailedException exc) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void chmod(String path, int mode, boolean followSymlinks) throws IOException {
        if(!followSymlinks) throw new UnsupportedOperationException("can't chmod without following symlinks");
        logger.debug("LocalFS {}: chmod {} {}", root, path, mode);
        Set<PosixFilePermission> modeSet = new HashSet<PosixFilePermission>();

        if((mode & 0100) != 0) modeSet.add(PosixFilePermission.OWNER_EXECUTE);
        if((mode & 0200) != 0) modeSet.add(PosixFilePermission.OWNER_WRITE);
        if((mode & 0400) != 0) modeSet.add(PosixFilePermission.OWNER_READ);

        if((mode & 0010) != 0) modeSet.add(PosixFilePermission.GROUP_EXECUTE);
        if((mode & 0020) != 0) modeSet.add(PosixFilePermission.GROUP_WRITE);
        if((mode & 0040) != 0) modeSet.add(PosixFilePermission.GROUP_READ);

        if((mode & 0001) != 0) modeSet.add(PosixFilePermission.OTHERS_EXECUTE);
        if((mode & 0002) != 0) modeSet.add(PosixFilePermission.OTHERS_WRITE);
        if((mode & 0004) != 0) modeSet.add(PosixFilePermission.OTHERS_READ);

        // TODO Someday: (implement) Add setuid/setgid/sticky bit support

        try {
            Files.setPosixFilePermissions(qualifiedPathNative(path), modeSet);
        } catch(NoSuchFileException exc) {
            throw new ENOENTException(path);
        }
    }

    @Override
    public void chown(String path, int uid, boolean followSymlinks) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void chown(String path, String user, boolean followSymlinks) throws IOException {
        logger.debug("LocalFS {}: chown {} {}", root, path, user);
        try {
            UserPrincipal userPrincipal = FileSystems.getDefault().getUserPrincipalLookupService().lookupPrincipalByName(user);
            java.io.File targetFile = new java.io.File(expandPath(path));
            LinkOption[] options = followSymlinks ? new LinkOption[0] : new LinkOption[] { LinkOption.NOFOLLOW_LINKS };

            PosixFileAttributeView view = Files
                    .getFileAttributeView(targetFile.toPath(), PosixFileAttributeView.class, options);
            if(view == null) throw new UnsupportedOperationException();
            view.setOwner(userPrincipal);
        } catch(FileSystemException exc) {
            throw new UnsupportedOperationException();
        } catch(UserPrincipalNotFoundException exc) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void chgrp(String path, int gid, boolean followSymlinks) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void chgrp(String path, String group, boolean followSymlinks) throws IOException {
        logger.debug("LocalFS {}: chgrp {} {}", root, path, group);
        try {
            GroupPrincipal groupPrincipal = FileSystems.getDefault().getUserPrincipalLookupService().lookupPrincipalByGroupName(group);
            java.io.File targetFile = new java.io.File(expandPath(path));
            LinkOption[] options = followSymlinks ? new LinkOption[0] : new LinkOption[] { LinkOption.NOFOLLOW_LINKS };
            Files
            .getFileAttributeView(targetFile.toPath(), PosixFileAttributeView.class, options)
            .setGroup(groupPrincipal);
        } catch(FileSystemException exc) {
            throw new UnsupportedOperationException();
        } catch(UserPrincipalNotFoundException exc) {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void setMtime(String path, long mtime, boolean followSymlinks) throws IOException {
        logger.debug("LocalFS {}: set_mtime {} {}", root, path, mtime);
        // attempting to use java.nio to set timestamps causes an indefinite block on linux (btrfs/Ubuntu 18.04)
        if(!followSymlinks) throw new UnsupportedOperationException("can't setMtime without following symlinks");
        if(stat(path).isFifo()) throw new UnsupportedOperationException("can't set atime on fifo");
        FileTime fileTime = FileTime.from(mtime, TimeUnit.NANOSECONDS);
        try {
            Files.setAttribute(qualifiedPathNative(path), "lastModifiedTime", fileTime);
        } catch(NoSuchFileException exc) {
            throw new ENOENTException(path);
        }
    }

    @Override
    public void setCtime(String path, long ctime, boolean followSymlinks) throws IOException {
        throw new UnsupportedOperationException("can't set ctime on filesystem");
    }

    @Override
    public void setAtime(String path, long atime, boolean followSymlinks) throws IOException {
        logger.debug("LocalFS {}: set_atime {} {}", root, path, atime);
        if(!followSymlinks) throw new UnsupportedOperationException("can't setAtime without following symlinks");
        if(stat(path).isFifo()) throw new UnsupportedOperationException("can't set atime on fifo");
        FileTime fileTime = FileTime.from(atime, TimeUnit.NANOSECONDS);
        try {
            Files.setAttribute(qualifiedPathNative(path), "lastAccessTime", fileTime);
        } catch(NoSuchFileException exc) {
            throw new ENOENTException(path);
        }
    }

    @Override
    public void write(String path, byte[] contents, int offset, int length) throws IOException {
        if(!exists(dirname(path))) mkdirp(dirname(path));
        try(LocalFile file = open(path, File.O_WRONLY|File.O_CREAT|File.O_TRUNC)) {
            file.write(contents, offset, length);
        }
    }

    @Override
    public LocalFile open(String path, int mode) throws IOException {
        return new LocalFile(this, path, mode);
    }

    @Override
    public void truncate(String path, long size) throws IOException {
        logger.debug("LocalFS {}: truncate {} {}", root, path, size);
        try(
            FileOutputStream stream = new FileOutputStream(qualifiedPathNative(path).toString(), true);
            FileChannel chan = stream.getChannel();
        ) {
            long oldSize = chan.size();
            long delta   = size - oldSize;
            
            if(delta > 0) {
                chan.position(oldSize);
                chan.write(ByteBuffer.allocate((int) (delta)));
            } else {
                chan.truncate(size);
            }
        }
    }

    @Override
    public boolean exists(String path, boolean followLinks) {
        LinkOption[] linkOpt = new LinkOption[] {};
        if(followLinks) linkOpt = new LinkOption[] {};
        else linkOpt = new LinkOption[] { LinkOption.NOFOLLOW_LINKS };
        try {
            return Files.exists(qualifiedPathNative(path), linkOpt);
        } catch(ENOENTException exc) {
            return false;
        }
    }

    @Override
    public LocalFS scopedFS(String subpath) throws IOException {
        if(!exists(subpath)) mkdirp(subpath);
        return new LocalFS(root.join(subpath));
    }

    @Override
    public LocalFS unscopedFS() throws IOException {
        return new LocalFS("/");
    }

    public FSPath getRoot() {
        return root;
    }

    protected String cachedUserName(int uid, Path path, LinkOption[] linkOpt) throws IOException {
        CachedName cached = cachedUserNames.get(uid);
        if(cached == null || cached.isExpired()) {
            cached = new CachedName(Files.getOwner(path, linkOpt).getName(), uid);
            cachedUserNames.put(uid, cached);
        }

        return cached.name;
    }

    protected String cachedGroupName(int gid, Path path, LinkOption[] linkOpt) throws IOException {
        CachedName cached = cachedGroupNames.get(gid);
        if(cached == null || cached.isExpired()) {
            GroupPrincipal group = Files.readAttributes(path, PosixFileAttributes.class, linkOpt).group();
            cached = new CachedName(group.getName(), gid);
            cachedGroupNames.put(gid, cached);
        }

        return cached.name;
    }

    protected String expandPath(String path) throws ENOENTException {
        return new FSPath(qualifiedPath(path).toString()).toPosix();
    }

    protected Path qualifiedPath(String path) throws ENOENTException {
        FSPath normalized = root.join(path).normalize();
        if(!normalized.descendsFrom(root)) {
            throw new ENOENTException(path);
        }

        return Paths.get(normalized.toPosix());
    }

    protected Path qualifiedPathNative(String path) throws ENOENTException {
        if(!path.contains("\\") && !Util.isWindows()) {
            /* Profiling shows qualfiedPathNative is a performance hotspot on large filesystems.
             * Let's speed things along if we know that there's no converting to be done. */
            String posixRoot = root.toPosix();
            Path normalized = Paths.get(posixRoot, path).normalize();
            if(!normalized.startsWith(posixRoot)) throw new ENOENTException(path);
            return normalized;
        }
        
        FSPath normalized = root.join(path).normalize();
        if(!normalized.descendsFrom(root)) {
            throw new ENOENTException(path);
        }

        return Paths.get(normalized.toNative());
    }

    protected void assertPathInScope(String path) throws ENOENTException {
        if(!root.join(path).normalize().descendsFrom(root)) {
            throw new ENOENTException(path);
        }
    }

    public String toString() {
        return this.getClass().getSimpleName() + " " + this.getRoot();
    }
}
