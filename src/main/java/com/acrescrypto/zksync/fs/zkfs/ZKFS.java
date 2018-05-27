package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import com.acrescrypto.zksync.exceptions.*;
import com.acrescrypto.zksync.fs.*;
import com.acrescrypto.zksync.utility.HashCache;

// A ZKSync archive.
public class ZKFS extends FS {
	protected InodeTable inodeTable;
	protected HashCache<String,ZKDirectory> directoriesByPath;
	ZKArchive archive;
	protected RefTag baseRevision;
	protected String root;
		
	public final static int MAX_PATH_LEN = 65535;
	
	public ZKFS(RefTag revision, String root) throws IOException {
		this.root = root;
		this.archive = revision.archive;
		this.directoriesByPath = new HashCache<String,ZKDirectory>(128, (String path) -> {
			assertPathIsDirectory(path);
			return new ZKDirectory(this, path);
		}, (String path, ZKDirectory dir) -> {
			dir.commit();
		});
		this.baseRevision = revision;
		this.inodeTable = new InodeTable(this, revision);
	}
	
	public ZKFS(RefTag revision) throws IOException {
		this(revision, "/");
	}
		
	public RefTag commit(RefTag[] additionalParents) throws IOException {
		for(ZKDirectory dir : directoriesByPath.values()) {
			dir.commit();
		}
		
		return baseRevision = inodeTable.commit(additionalParents);
	}
	
	public RefTag commit() throws IOException {
		return commit(new RefTag[0]);
	}
	
	public Inode inodeForPath(String path) throws IOException {
		return inodeForPath(path, true);
	}
	
	public Inode inodeForPath(String path, boolean followSymlinks) throws IOException {
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
		
		if(followSymlinks && inode.getStat().isSymlink()) {
			ZKFile symlink = new ZKFile(this, path, File.O_RDONLY|File.O_NOFOLLOW|ZKFile.O_LINK_LITERAL);
			String linkPath = new String(symlink.read(MAX_PATH_LEN));
			symlink.close();
			return inodeForPath(linkPath, true);
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
		try {
			Inode inode = inodeForPath(path);
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
		assertPathIsNotDirectory(path);
		return new ZKFile(this, path, mode);
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
	}

	@Override
	public void chown(String path, int uid) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setUid(uid);
	}

	@Override
	public void chown(String path, String name) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setUser(name);
	}

	@Override
	public void chgrp(String path, int gid) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setGid(gid);
	}

	@Override
	public void chgrp(String path, String group) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setGroup(group);
	}

	@Override
	public void setMtime(String path, long mtime) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setMtime(mtime);
	}

	@Override
	public void setCtime(String path, long ctime) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setCtime(ctime);
	}

	@Override
	public void setAtime(String path, long atime) throws IOException {
		Inode inode = inodeForPath(path);
		inode.getStat().setAtime(atime);
	}
	
	@Override
	public ZKFS scopedFS(String subpath) {
		// TODO: This is gonna be a pain. Supporting it would allow nested archives... dunno if that's a good thing!
		throw new UnsupportedOperationException();
	}
	
	protected void uncache(String path) throws IOException {
		directoriesByPath.remove(path);
	}
	
	public ZKArchive getArchive() {
		return archive;
	}
	
	public RevisionInfo getRevisionInfo() throws IOException {
		return new RevisionInfo(this);
	}
}
