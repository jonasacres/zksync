package com.acrescrypto.zksync.fs.localfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.acrescrypto.zksync.exceptions.CommandFailedException;
import com.acrescrypto.zksync.exceptions.EACCESException;
import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.FileTypeNotSupportedException;
import com.acrescrypto.zksync.fs.*;
import com.acrescrypto.zksync.utility.Util;

public class LocalFS extends FS {
	protected String root;
	
	public LocalFS(String root) {
		this.root = root;
	}

	private Stat statWithLinkOption(String pathStr, LinkOption... linkOpt) throws IOException {
		Stat stat = new Stat();
		Path path = qualifiedPath(pathStr);
		
		try {
			stat.setAtime(decodeTime(Files.getAttribute(path, "lastAccessTime", linkOpt)));
			stat.setMtime(decodeTime(Files.getAttribute(path, "lastModifiedTime", linkOpt)));
			stat.setCtime(decodeTime(Files.getAttribute(path, "creationTime", linkOpt)));
			stat.setMode(getFilePermissions(path, linkOpt));
			stat.setUser(Files.getOwner(path, linkOpt).getName());
			if(!isWindows()) {
				stat.setUid((Integer) Files.getAttribute(path, "unix:uid", linkOpt));
				stat.setGid((Integer) Files.getAttribute(path, "unix:gid", linkOpt));
				GroupPrincipal group = Files.readAttributes(path, PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS).group();
				stat.setGroup(group.getName());
			}
			int type = getStatType(path, linkOpt);
			if(type >= 0) {
				stat.setType(type);
			} else {
				scrapeLSForUnixSpecific(stat, path.toString());
			}
			
			if(stat.isSymlink()) {
				stat.setSize(0);
			} else {
				stat.setSize(Files.size(path));
			}
						
			if(!isWindows()) stat.setInodeId((Long) Files.getAttribute(path, "unix:ino", linkOpt));
		} catch(NoSuchFileException exc) {
			throw new ENOENTException(path.toString());
		} catch(java.nio.file.AccessDeniedException exc) {
			throw new EACCESException(path.toString());
		}
		
		return stat;
	}
	
	private long decodeTime(Object obj) {
		FileTime time = (FileTime) obj;
		return time.to(TimeUnit.NANOSECONDS);
	}
	
	private int getStatType(Path path, LinkOption... linkOpt) {
		if(Files.isDirectory(path, linkOpt)) {
			return Stat.TYPE_DIRECTORY;
		} else if(Files.isRegularFile(path, linkOpt)) {
			return Stat.TYPE_REGULAR_FILE;
		} else if(Files.isSymbolicLink(path) && linkOpt != null) {
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
	
	private int getFilePermissions(Path path, LinkOption... linkOpt) throws IOException {
		int mode = 0;
		Set<PosixFilePermission> perms = Files.getPosixFilePermissions(path, linkOpt);
		
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
		return new LocalDirectory(this, path);
	}

	@Override
	public void mkdir(String path) throws IOException {
		Files.createDirectory(qualifiedPath(path));
	}
	
	@Override
	public void mkdirp(String path) throws IOException {
		Files.createDirectories(qualifiedPath(path));
	}

	@Override
	public void rmdir(String path) throws IOException {
		Path p = qualifiedPath(path);
		if(!Files.exists(p)) throw new ENOENTException(path);
		if(!Files.isDirectory(p)) throw new IOException(path + ": not a directory");
		Files.delete(p);
	}

	@Override
	public void unlink(String path) throws IOException {
		try {
			Files.delete(qualifiedPath(path));
		} catch(NoSuchFileException exc) {
			throw new ENOENTException(path);
		}
	}

	@Override
	public void link(String source, String dest) throws IOException {
		try {
			Files.createLink(qualifiedPath(dest), qualifiedPath(source));
		} catch(FileAlreadyExistsException exc) {
			throw new EEXISTSException(qualifiedPath(source).toString());
		} catch(NoSuchFileException exc) {
			throw new ENOENTException(source);
		}
	}

	@Override
	public void symlink(String source, String dest) throws IOException {
		Path trueSource = source.substring(0, 1).equals("/")
				? qualifiedPath(source)
				: Paths.get(source);
		
		String aroot = root.endsWith("/") ? root : root + "/";
		Path explicit = Paths.get(root).resolve(trueSource).normalize().toAbsolutePath();
		if(!explicit.startsWith(aroot)) {
			throw new ENOENTException(source);
		}
		
		Files.createSymbolicLink(qualifiedPath(dest), trueSource);
	}
	
	@Override
	public void symlink_unsafe(String source, String dest) throws IOException {
		Path psource = Paths.get(source);
		Files.createSymbolicLink(qualifiedPath(dest), psource);
	}
	
	@Override
	public String readlink(String link) throws IOException {
		try {
			String target = Files.readSymbolicLink(qualifiedPath(link)).toString();
			if(target.startsWith(root)) {
				return target.substring(root.length(), target.length());
			}
			
			return target;
		} catch(NoSuchFileException exc) {
			throw new ENOENTException(link);
		}
	}
	
	@Override
	public String readlink_unsafe(String link) throws IOException {
		try {
			return Files.readSymbolicLink(qualifiedPath(link)).toString();
		} catch(NoSuchFileException exc) {
			throw new ENOENTException(link);
		}
	}

	@Override
	public void mknod(String path, int type, int major, int minor) throws IOException {
		if(isWindows()) throw new FileTypeNotSupportedException(path + ": Windows does not support devices");
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
		if(isWindows()) throw new FileTypeNotSupportedException(path + ": Windows does not support named pipes");
		try {
			runCommand(new String[] { "mkfifo", expandPath(path) });
		} catch(CommandFailedException exc) {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public void chmod(String path, int mode) throws IOException {
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
			Files.setPosixFilePermissions(qualifiedPath(path), modeSet);
		} catch(NoSuchFileException exc) {
			throw new ENOENTException(path);
		}
	}

	@Override
	public void chown(String path, int uid) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void chown(String path, String user) throws IOException {
		try {
			UserPrincipal userPrincipal = FileSystems.getDefault().getUserPrincipalLookupService().lookupPrincipalByName(user);
			java.io.File targetFile = new java.io.File(expandPath(path));
			Files.getFileAttributeView(targetFile.toPath(), PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).setOwner(userPrincipal);
		} catch(FileSystemException exc) {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public void chgrp(String path, int gid) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void chgrp(String path, String group) throws IOException {
		try {
			GroupPrincipal groupPrincipal = FileSystems.getDefault().getUserPrincipalLookupService().lookupPrincipalByGroupName(group);
			java.io.File targetFile = new java.io.File(expandPath(path));
			Files.getFileAttributeView(targetFile.toPath(), PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).setGroup(groupPrincipal);
		} catch(FileSystemException exc) {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public void setMtime(String path, long mtime) throws IOException {
		// attempting to use java.nio to set timestamps causes an indefinite block on linux (btrfs/Ubuntu 18.04)
		if(stat(path).isFifo()) throw new UnsupportedOperationException("can't set atime on fifo");
		FileTime fileTime = FileTime.from(mtime, TimeUnit.NANOSECONDS);
		try {
			Files.setAttribute(qualifiedPath(path), "lastModifiedTime", fileTime);
		} catch(NoSuchFileException exc) {
			throw new ENOENTException(path);
		}
	}

	@Override
	public void setCtime(String path, long ctime) throws IOException {
		throw new UnsupportedOperationException("can't set ctime on filesystem");
	}

	@Override
	public void setAtime(String path, long atime) throws IOException {
		if(stat(path).isFifo()) throw new UnsupportedOperationException("can't set atime on fifo");
		FileTime fileTime = FileTime.from(atime, TimeUnit.NANOSECONDS);
		try {
			Files.setAttribute(qualifiedPath(path), "lastAccessTime", fileTime);
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
		FileOutputStream stream = null;
		FileChannel chan = null;
		
		try {
			stream = new FileOutputStream(expandPath(path), true);
			chan = stream.getChannel();
			long oldSize = chan.size();
			if(size > oldSize) {
				chan.position(oldSize);
				chan.write(ByteBuffer.allocate((int) (size-oldSize)));
			} else {
				chan.truncate(size);
			}
		} finally {
			if(chan != null) chan.close();
			if(stream != null) stream.close();
		}
	}
	
	@Override
	public boolean exists(String path, boolean followLinks) {
		LinkOption[] linkOpt;
		if(followLinks) linkOpt = new LinkOption[] {};
		else linkOpt = new LinkOption[] { LinkOption.NOFOLLOW_LINKS };
		try {
			return Files.exists(qualifiedPath(path), linkOpt);
		} catch(ENOENTException exc) {
			return false;
		}
	}
	
	@Override
	public LocalFS scopedFS(String subpath) throws IOException {
		if(!exists(subpath)) mkdirp(subpath);
		return new LocalFS(expandPath(subpath));
	}
	
	@Override
	public LocalFS unscopedFS() throws IOException {
		return new LocalFS("/");
	}
	
	public String getRoot() {
		return root;
	}
	
	protected String expandPath(String path) throws ENOENTException {
		return qualifiedPath(path).toString();
	}
	
	protected Path qualifiedPath(String path) throws ENOENTException {
		Path p = Paths.get(root, path).normalize();
		if(!p.startsWith(root)) throw new ENOENTException(path);
		return p;
	}
	
	public String toString() {
		return this.getClass().getSimpleName() + " " + this.getRoot();
	}
}
