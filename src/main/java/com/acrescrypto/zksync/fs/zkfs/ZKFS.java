package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;

import com.acrescrypto.zksync.exceptions.*;
import com.acrescrypto.zksync.fs.*;
import com.acrescrypto.zksync.utility.HashCache;
import com.acrescrypto.zksync.utility.Util;

// A ZKSync archive.
public class ZKFS extends FS {
	public final static int MAX_SYMLINK_DEPTH = 32;
	public interface ZKFSDirtyMonitor {
		public void notifyDirty(ZKFS fs);
	}
	
	protected InodeTable inodeTable;
	protected HashCache<String,ZKDirectory> directoriesByPath;
	protected ZKArchive archive;
	protected RevisionTag baseRevision;
	protected String root;
	protected boolean dirty;
	protected LinkedList<ZKFSDirtyMonitor> dirtyMonitors = new LinkedList<>();
		
	public final static int MAX_PATH_LEN = 65535;
	
	public ZKFS(RevisionTag revision, String root) throws IOException {
		this.root = root;
		rebase(revision);
	}
	
	public ZKFS(RevisionTag revision) throws IOException {
		this(revision, "/");
	}
	
	public RevisionTag commit(RevisionTag[] additionalParents) throws IOException {
		return commitWithTimestamp(additionalParents, -1);
	}
	
	public RevisionTag commitWithTimestamp(RevisionTag[] additionalParents, long timestamp) throws IOException {
		for(ZKDirectory dir : directoriesByPath.values()) {
			dir.commit();
		}
		
		baseRevision = inodeTable.commitWithTimestamp(additionalParents, timestamp);
		dirty = false;
		return baseRevision;
	}
	
	public RevisionTag commit() throws IOException {
		return commit(new RevisionTag[0]);
	}
	
	public Inode inodeForPath(String path) throws IOException {
		return inodeForPath(path, true);
	}
	
	public Inode inodeForPath(String path, boolean followSymlinks) throws IOException {
		if(followSymlinks) return inodeForPathWithSymlinks(path, 0);
		String absPath = absolutePath(path);
		if(path.equals("")) throw new ENOENTException(path);
		if(absPath.equals("/")) {
			return inodeTable.inodeWithId(InodeTable.INODE_ID_ROOT_DIRECTORY);
		}
		
		ZKDirectory root;
		try {
			root = opendir("/");
		} catch (EISNOTDIRException e) {
			throw new ENOENTException(path);
		}
		
		long inodeId = root.inodeForPath(absPath);
		root.close();
		Inode inode = inodeTable.inodeWithId(inodeId);
		
		return inode;
	}
	
	protected Inode inodeForPathWithSymlinks(String path, int depth) throws IOException {
		if(depth > MAX_SYMLINK_DEPTH) throw new ELOOPException(path);
		String absPath = absolutePath(path);
		if(path.equals("")) throw new ENOENTException(path);
		if(absPath.equals("/")) {
			return inodeTable.inodeWithId(InodeTable.INODE_ID_ROOT_DIRECTORY);
		}
		
		ZKDirectory root;
		try {
			root = opendir("/");
		} catch (EISNOTDIRException e) {
			throw new ENOENTException(path);
		}
		
		long inodeId = root.inodeForPath(absPath);
		root.close();
		Inode inode = inodeTable.inodeWithId(inodeId);
		
		if(inode.getStat().isSymlink()) {
			ZKFile symlink = new ZKFile(this, path, File.O_RDONLY|File.O_NOFOLLOW|ZKFile.O_LINK_LITERAL, true);
			String linkPath = new String(symlink.read(MAX_PATH_LEN));
			symlink.close();
			return inodeForPathWithSymlinks(linkPath, depth + 1);
		}
		
		return inode;
	}
	
	protected Inode create(String path) throws IOException {
		return create(path, opendir(dirname(path)));
	}
	
	protected Inode create(String path, ZKDirectory parent) throws IOException {
		assertPathLegal(path);
		Inode inode = inodeTable.issueInode();
		parent.link(inode, basename(path));
		return inode;
	}
	
	public void assertPathLegal(String path) throws EINVALException {
		String bn = basename(path);
		if(bn.equals("")) throw new EINVALException(path);
		if(bn.equals(".")) throw new EINVALException(path);
		if(bn.equals("..")) throw new EINVALException(path);
		if(bn.equals("/")) throw new EINVALException(path);
	}
	
	public void assertPathExists(String path) throws IOException {
		inodeForPath(path);
	}
	
	public void assertPathDoesntExist(String path) throws IOException {
		try {
			inodeForPath(path);
			throw new EEXISTSException(path);
		} catch(ENOENTException exc) {}
	}
	
	public void assertPathIsDirectory(String path) throws IOException {
		Inode inode = inodeForPath(path);
		if(!inode.getStat().isDirectory()) throw new EISNOTDIRException(path);
	}
	
	public void assertPathIsNotDirectory(String path) throws IOException {
		assertPathIsNotDirectory(path, true);
	}
	
	public void assertPathIsNotDirectory(String path, boolean followSymlink) throws IOException {
		try {
			Inode inode = inodeForPath(path, followSymlink);
			if(inode.getStat().isDirectory()) throw new EISDIRException(path);
		} catch(ENOENTException e) {
		}
	}
	
	public void assertDirectoryIsEmpty(String path) throws IOException {
		ZKDirectory dir = opendir(path);
		if(dir.list().length > 0) throw new ENOTEMPTYException(path);
	}

	public InodeTable getInodeTable() {
		return inodeTable;
	}
	
	@Override
	public void write(String path, byte[] contents, int offset, int length) throws IOException {
		mkdirp(dirname(path));
		
		ZKFile file = open(path, ZKFile.O_WRONLY|ZKFile.O_CREAT|ZKFile.O_TRUNC);
		file.write(contents, offset, length);
		file.close();
	}

	@Override
	public ZKFile open(String path, int mode) throws IOException {
		assertPathIsNotDirectory(path, (mode & File.O_NOFOLLOW) == 0);
		return new ZKFile(this, path, mode, true);
	}

	@Override
	public Stat stat(String path) throws IOException {
		Inode inode = inodeForPath(path, true);
		return inode.getStat().clone();
	}

	@Override
	public Stat lstat(String path) throws IOException {
		Inode inode = inodeForPath(path, false);
		return inode.getStat().clone();
	}
	
	@Override
	public void truncate(String path, long size) throws IOException {
		ZKFile file = open(path, File.O_RDWR);
		file.truncate(size);
		file.close();
	}

	@Override
	public ZKDirectory opendir(String path) throws IOException {
		return directoriesByPath.get(absolutePath(path));
	}

	@Override
	public void mkdir(String path) throws IOException {
		assertPathIsDirectory(dirname(path));
		assertPathDoesntExist(path);
		ZKDirectory dir = opendir(dirname(path));
		dir.mkdir(basename(path));
		dir.close();
	}
	
	@Override
	public void mkdirp(String path) throws IOException {
		try {
			assertPathIsDirectory(path);
		} catch(ENOENTException e) {
			mkdirp(dirname(path));
			mkdir(path);
		}
	}

	@Override
	public void rmdir(String path) throws IOException {
		assertDirectoryIsEmpty(path);
		if(path.equals("/")) return; // quietly ignore for rmrf("/") support; used to throw new EINVALException("cannot delete root directory");
		
		ZKDirectory dir = opendir(path);
		dir.rmdir();
		dir.close();
		uncache(path);
	}

	@Override
	public void unlink(String path) throws IOException {
		if(inodeForPath(path, false).getStat().isDirectory()) throw new EISDIRException(path);
		
		ZKDirectory dir = opendir(dirname(path));
		dir.unlink(basename(path));
		dir.close();
		uncache(path);
	}
	
	@Override
	public void link(String source, String dest) throws IOException {
		Inode target = inodeForPath(source);
		ZKDirectory destDir = opendir(dirname(dest));
		destDir.link(target, basename(dest));
		destDir.close();
	}
	
	@Override
	public void symlink(String source, String dest) throws IOException {
		Inode instance = create(dest);
		instance.getStat().makeSymlink();
		
		ZKFile symlink = open(dest, File.O_WRONLY|File.O_NOFOLLOW|File.O_CREAT|ZKFile.O_LINK_LITERAL);
		symlink.write(source.getBytes());
		symlink.close();
	}
	
	@Override
	public String readlink(String link) throws IOException {
		if(!lstat(link).isSymlink()) throw new EINVALException(link);
		ZKFile symlink = open(link, File.O_RDONLY|File.O_NOFOLLOW|ZKFile.O_LINK_LITERAL);
		String target = new String(symlink.read());
		return target;
	}

	@Override
	public void mknod(String path, int type, int major, int minor) throws IOException {
		switch(type) {
		case Stat.TYPE_CHARACTER_DEVICE:
			create(path).getStat().makeCharacterDevice(major, minor);
			break;
		case Stat.TYPE_BLOCK_DEVICE:
			create(path).getStat().makeBlockDevice(major, minor);
			break;
		default:
			throw new IllegalArgumentException(String.format("Illegal node type: %d", type));
		}
	}
	
	@Override
	public void mkfifo(String path) throws IOException {
		create(path).getStat().makeFifo();
	}
	
	@Override
	public void chmod(String path, int mode) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setMode(mode);
		markDirty();
	}

	@Override
	public void chown(String path, int uid) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setUid(uid);
		markDirty();
	}

	@Override
	public void chown(String path, String name) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setUser(name);
		markDirty();
	}

	@Override
	public void chgrp(String path, int gid) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setGid(gid);
		markDirty();
	}

	@Override
	public void chgrp(String path, String group) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setGroup(group);
		markDirty();
	}

	@Override
	public void setMtime(String path, long mtime) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setMtime(mtime);
		markDirty();
	}

	@Override
	public void setCtime(String path, long ctime) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setCtime(ctime);
		markDirty();
	}

	@Override
	public void setAtime(String path, long atime) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setAtime(atime);
		markDirty();
	}
	
	@Override
	public ZKFS scopedFS(String subpath) {
		throw new UnsupportedOperationException();
	}
	
	public void uncache(String path) throws IOException {
		directoriesByPath.remove(path);
	}
	
	public ZKArchive getArchive() {
		return archive;
	}
	
	public RevisionInfo getRevisionInfo() throws IOException {
		return inodeTable.revision;
	}

	public RevisionTag getBaseRevision() {
		return baseRevision;
	}
	
	public void dump() throws IOException {
		System.out.println("Revision " + baseRevision);
		dump("/", 1);
	}
	
	public void dump(String path, int depth) throws IOException {
		String padding = new String(new char[depth]).replace("\0", "  ");
		ZKDirectory dir = opendir(path);
		for(String subpath : dir.list()) {
			Inode inode = inodeForPath(Paths.get(path, subpath).toString());
			System.out.printf("%s%30s inodeId=%d size=%d ref=%s\n",
					padding,
					subpath,
					inode.stat.getInodeId(),
					inode.stat.getSize(),
					Util.bytesToHex(inode.getRefTag().getHash(), 8) + "...");
			if(inode.stat.isDirectory()) {
				dump(subpath, depth+1);
			}
		}
	}

	public void rebase(RevisionTag revision) throws IOException {
		this.archive = revision.getArchive();
		this.directoriesByPath = new HashCache<String,ZKDirectory>(128, (String path) -> {
			assertPathIsDirectory(path);
			return new ZKDirectory(this, path);
		}, (String path, ZKDirectory dir) -> {
			dir.commit();
		});
		this.baseRevision = revision;
		this.inodeTable = new InodeTable(this, revision);
		this.dirty = false;
	}
	
	public void addMonitor(ZKFSDirtyMonitor monitor) {
		dirtyMonitors.add(monitor);
	}
	
	public void removeMonitor(ZKFSDirtyMonitor monitor) {
		dirtyMonitors.remove(monitor);
	}

	public boolean isDirty() {
		return dirty;
	}
	
	public void markDirty() {
		this.dirty = true;
		for(ZKFSDirtyMonitor monitor : dirtyMonitors) {
			monitor.notifyDirty(this);
		}
	}
}
