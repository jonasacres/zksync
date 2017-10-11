package com.acrescrypto.zksync.fs.localfs;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.acrescrypto.zksync.exceptions.FileTypeNotSupportedException;
import com.acrescrypto.zksync.fs.*;

public class LocalFS extends FS {
	protected String root;
	
	public LocalFS() {
		this("/");
	}
	
	public LocalFS(String root) {
		this.root = root;
	}

	private Stat statWithLinkOption(String pathStr, LinkOption... linkOpt) throws IOException {
		Stat stat = new Stat();
		Path path = Paths.get(root, pathStr);
		
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
		stat.setSize(Files.size(path));
		
		int type = getStatType(path, linkOpt);
		if(type >= 0) {
			stat.setType(type);
		} else {
			scrapeLSForUnixSpecific(stat, path.toString());
		}
		
		if(!isWindows()) stat.setInodeId((Long) Files.getAttribute(path, "unix:ino", linkOpt));
		
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
		return System.getProperty("os.name").toLowerCase().indexOf("win") >= 0;
	}
	
	private String runCommand(String[] args) throws IOException {
		Process process = null;
		byte[] buf = new byte[1024];
		
	    process = new ProcessBuilder(args)
	      .start();
	    
	    try {
			process.waitFor();
			process.getInputStream().read(buf);
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
		Files.createDirectory(Paths.get(root, path));
	}

	@Override
	public void rmdir(String path) throws IOException {
		Path p = Paths.get(root, path);
		if(!Files.isDirectory(p)) throw new IOException(path + ": not a directory");
		Files.delete(p);
	}

	@Override
	public void unlink(String path) throws IOException {
		Files.delete(Paths.get(root, path));
	}

	@Override
	public void link(String source, String dest) throws IOException {
		Files.createLink(Paths.get(root, dest), Paths.get(root, source));
	}

	@Override
	public void symlink(String source, String dest) throws IOException {
		Files.createSymbolicLink(Paths.get(root, dest), Paths.get(source));
	}
	
	@Override
	public String readlink(String link) throws IOException {
		return Files.readSymbolicLink(Paths.get(root, link)).toString();
	}

	@Override
	public void mknod(String path, int type, int major, int minor) throws IOException {
		if(isWindows()) throw new FileTypeNotSupportedException(path + ": Windows does not support devices");
		String typeChars[] = { "c", "b", "p" };
		if(type >= typeChars.length) throw new IllegalArgumentException();
		
		runCommand(new String[] { "mknod", typeChars[type], path, String.format("%d", major), String.format("%d", minor) });
	}

	@Override
	public void mkfifo(String path) throws IOException {
		if(isWindows()) throw new FileTypeNotSupportedException(path + ": Windows does not support named pipes");
		runCommand(new String[] { "mkfifo", Paths.get(root, path).toString() });
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
		
		// TODO: wtf to do about sticky, setuid, setgid?
		
		Files.setPosixFilePermissions(Paths.get(root, path), modeSet);
	}

	@Override
	public void chown(String path, int uid) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void chown(String path, String user) throws IOException {
		UserPrincipal userPrincipal = FileSystems.getDefault().getUserPrincipalLookupService().lookupPrincipalByName(user);
		java.io.File targetFile = new java.io.File(path);
		Files.getFileAttributeView(targetFile.toPath(), PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).setOwner(userPrincipal);
	}

	@Override
	public void chgrp(String path, int gid) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void chgrp(String path, String group) throws IOException {
		UserPrincipal groupPrincipal = FileSystems.getDefault().getUserPrincipalLookupService().lookupPrincipalByName(group);
		java.io.File targetFile = new java.io.File(path);
		Files.getFileAttributeView(targetFile.toPath(), PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS).setOwner(groupPrincipal);
	}

	@Override
	public void setMtime(String path, long mtime) throws IOException {
		FileTime fileTime = FileTime.from(mtime, TimeUnit.NANOSECONDS);
		Files.setAttribute(Paths.get(root, path), "lastModifiedTime", fileTime);
	}

	@Override
	public void setCtime(String path, long ctime) throws IOException {
		throw new UnsupportedOperationException("can't set ctime on filesystem");
	}

	@Override
	public void setAtime(String path, long atime) throws IOException {
		FileTime fileTime = FileTime.from(atime, TimeUnit.NANOSECONDS);
		Files.setAttribute(Paths.get(root, path), "lastAccessTime", fileTime);
	}

	@Override
	public void write(String path, byte[] contents) throws IOException {
		LocalFile file = open(path, File.O_WRONLY|File.O_CREAT);
		file.write(contents);
		file.close();
	}

	@Override
	public byte[] read(String path) throws IOException {
		LocalFile file = open(path, File.O_RDONLY);
		byte[] retval = file.read();
		file.close();
		return retval;
	}

	@Override
	public LocalFile open(String path, int mode) throws IOException {
		return new LocalFile(this, path, mode);
	}
	
	@Override
	public boolean exists(String path, boolean followLinks) {
		LinkOption[] linkOpt;
		if(followLinks) linkOpt = new LinkOption[] {};
		else linkOpt = new LinkOption[] { LinkOption.NOFOLLOW_LINKS };
		return Files.exists(Paths.get(root, path), linkOpt);
	}
	
	public String getRoot() {
		return root;
	}
}
