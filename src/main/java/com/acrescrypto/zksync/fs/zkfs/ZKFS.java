package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.*;
import com.acrescrypto.zksync.exceptions.*;
import com.acrescrypto.zksync.fs.*;

public class ZKFS extends FS {
	private Ciphersuite ciphersuite;
	private InodeTable inodeTable;
	private PubConfig pubConfig;
	private PrivConfig privConfig;
	private String root;
	private FS storage;
	private Key cipherKey, authKey;
	
	public final static int KEY_TYPE_CIPHER = 0;
	public final static int KEY_TYPE_AUTH = 1;
	
	public final static int KEY_INDEX_PAGE = 0;
	public final static int KEY_INDEX_PAGE_MERKEL = 1;
	public final static int KEY_INDEX_PAGE_REVISION = 2;
	
	public final static int MAX_PATH_LEN = 65535;
	
	public InodeTable getInodeTable() {
		return inodeTable;
	}
	
	public PubConfig getPubConfig() {
		return pubConfig;
	}
	
	public PrivConfig getPrivConfig() {
		return privConfig;
	}
	
	public String getRoot() {
		return root;
	}
	
	public FS getStorage() {
		return storage;
	}
	
	public Ciphersuite getCiphersuite() {
		return ciphersuite;
	}
	
	public Key deriveKey(int type, int index, byte[] tweak) {
		Key[] keys = { cipherKey, authKey };
		if(type >= keys.length) throw new IllegalArgumentException();
		return keys[type].derive(index, tweak);
	}
	
	public Key deriveKey(int type, int index, long tweak) {
		ByteBuffer buf = ByteBuffer.allocate(8);
		buf.putLong(tweak);
		return deriveKey(type, index, buf.array());
	}
	
	public Key deriveKey(int type, int index) {
		byte[] empty = {};
		return deriveKey(type, index, empty);
	}
	
	public Inode inodeForPath(String path) throws IOException {
		return inodeForPath(path, true);
	}
	
	public Inode inodeForPath(String path, boolean followSymlinks) throws IOException {
		ZKDirectory root;
		try {
			root = opendir("/");
		} catch (EISNOTDIRException e) {
			throw new ENOENTException(path);
		}
		
		long inodeId = root.inodeForPath(path);
		root.close();
		
		Inode inode = inodeTable.inodeWithId(inodeId);
		if(followSymlinks && inode.getStat().isSymlink()) {
			ZKFile symlink = new ZKFile(this, path, File.O_RDONLY|File.O_NOFOLLOW);
			String linkPath = new String(symlink.read(MAX_PATH_LEN));
			return inodeForPath(linkPath, true);
		}
		
		return inodeTable.inodeWithId(inodeId);
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
	public ZKDirectory opendir(String path) throws IOException {
		assertPathIsDirectory(path);
		return new ZKDirectory(this, path);
	}

	@Override
	public void mkdir(String path) throws IOException {
		assertPathIsDirectory(dirname(path));
		assertPathDoesntExist(path);
		
		create(path).getStat().makeDirectory();
		ZKDirectory dir = opendir(path);
		dir.link(".", dir);
		dir.link("..", inodeForPath(dirname(path)));
		dir.close();
	}

	@Override
	public void rmdir(String path) throws IOException {
		assertDirectoryIsEmpty(path);
		if(path.equals("/")) throw new EINVALException("cannot delete root directory");
		
		ZKDirectory dir = opendir(path);
		dir.rmdir();
		dir.close();
	}

	@Override
	public void unlink(String path) throws IOException {
		if(inodeForPath(path).getStat().isDirectory()) throw new EISDIRException(path);
		
		ZKDirectory dir = opendir(dirname(path));
		dir.unlink(basename(path));
		dir.close();
	}

	@Override
	public void link(String source, String dest) throws IOException {
		Inode target = inodeForPath(source);
		ZKDirectory destDir = opendir(dirname(dest));
		destDir.link(basename(dest), target);
	}
	
	@Override
	public void symlink(String source, String dest) throws IOException {
		Inode instance = create(dest);
		instance.getStat().makeSymlink();
		
		ZKFile symlink = open(dest, File.O_WRONLY|File.O_NOFOLLOW);
		symlink.write(source.getBytes());
		symlink.close();
	}

	@Override
	public void mknod(String path, int type, int major, int minor) throws IOException {
		switch(type) {
		case NODE_TYPE_CHARACTER_DEVICE:
			create(path).getStat().makeCharacterDevice(major, minor);
			break;
		case NODE_TYPE_BLOCK_DEVICE:
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
	
	private Inode create(String path) throws IOException {
		ZKDirectory parent = opendir(dirname(path));
		Inode inode = inodeTable.issueInode();
		parent.link(basename(path), inode);
		return inode;
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
		Inode inode = inodeForPath(path);
		if(inode.getStat().isDirectory()) throw new EISDIRException(path);
	}
	
	public void assertDirectoryIsEmpty(String path) throws IOException {
		ZKDirectory dir = opendir(path);
		if(dir.list().length > 2) throw new ENOTEMPTYException(path);
	}

	@Override
	public void write(String path, byte[] contents) throws IOException {
		ZKFile file = open(path, ZKFile.O_WRONLY);
		file.write(contents);
		file.close();
	}

	@Override
	public byte[] read(String path) throws IOException {
		ZKFile file = open(path, ZKFile.O_RDONLY);
		byte[] buf = file.read();
		file.close();
		
		return buf;
	}
}
