package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.Hashtable;

import com.acrescrypto.zksync.crypto.*;
import com.acrescrypto.zksync.exceptions.*;
import com.acrescrypto.zksync.fs.*;
import com.acrescrypto.zksync.fs.zkfs.config.LocalConfig;
import com.acrescrypto.zksync.fs.zkfs.config.PrivConfig;
import com.acrescrypto.zksync.fs.zkfs.config.PubConfig;

// A ZKSync archive.
public class ZKFS extends FS {
	protected CryptoSupport crypto;
	protected InodeTable inodeTable;
	protected PubConfig pubConfig;
	protected PrivConfig privConfig;
	protected LocalConfig localConfig;
	protected FS storage;
	protected KeyFile keyfile;
	protected Hashtable<String,Inode> inodesByPath; // TODO: not entirely happy with this, since there's no real eviction policy
	
	public final static int KEY_TYPE_CIPHER = 0;
	public final static int KEY_TYPE_AUTH = 1;
	
	public final static int KEY_INDEX_PAGE = 0;
	public final static int KEY_INDEX_PAGE_MERKEL = 1;
	public final static int KEY_INDEX_PAGE_REVISION = 2;
	public final static int KEY_INDEX_CONFIG_PRIVATE = 3;
	public final static int KEY_INDEX_CONFIG_LOCAL = 4;
	
	public final static int MAX_PATH_LEN = 65535;
	
	public final static String DATA_DIR = ".zksync/archive/data/";
	public final static String CONFIG_DIR = ".zksync/archive/config/";
	public final static String REVISION_DIR = ".zksync/archive/revisions/";
	public final static String LOCAL_DIR = ".zksync/local/";
	public final static String ACTIVE_REVISION = ".zskync/local/active-revision";
	
	public ZKFS(FS storage, char[] passphrase) throws IOException {
		this(storage, passphrase, null);
	}
	
	public ZKFS(FS storage, char[] passphrase, Revision revision) throws IOException {
		this.storage = storage;
		this.pubConfig = new PubConfig(storage);
		crypto = new CryptoSupport(pubConfig);
		keyfile = new KeyFile(this, passphrase);
		this.privConfig = new PrivConfig(storage, deriveKey(KEY_TYPE_CIPHER, KEY_INDEX_CONFIG_PRIVATE));
		this.localConfig = new LocalConfig(storage, deriveKey(KEY_TYPE_CIPHER, KEY_INDEX_CONFIG_LOCAL));
		if(revision == null) revision = Revision.activeRevision(this);
		this.inodeTable = new InodeTable(this, revision);
		this.inodesByPath = new Hashtable<String,Inode>();
		
		if(revision == null) {
			initialize();
		}
	}
	
	public Revision commit() throws IOException {
		return inodeTable.commit();
	}
	
	public Key deriveKey(int type, int index, byte[] tweak) {
		Key[] keys = { keyfile.getCipherRoot(), keyfile.getAuthRoot() };
		if(type >= keys.length) throw new IllegalArgumentException();
		return keys[type].derive(index, tweak);
	}
	
	public Key deriveKey(int type, int index) {
		byte[] empty = {};
		return deriveKey(type, index, empty);
	}
	
	public Inode inodeForPath(String path) throws IOException {
		return inodeForPath(path, true);
	}
	
	public Inode inodeForPath(String path, boolean followSymlinks) throws IOException {
		if(path.equals("")) throw new ENOENTException(path);
		if(path.equals("/")) {
			return inodeTable.inodeWithId(InodeTable.INODE_ID_ROOT_DIRECTORY);
		}
		
		ZKDirectory root;
		try {
			root = opendir("/");
		} catch (EISNOTDIRException e) {
			throw new ENOENTException(path);
		}
		
		Inode inode = inodesByPath.get(path);
		if(inode == null) {
			long inodeId = root.inodeForPath(path);
			root.close();
			inode = inodeTable.inodeWithId(inodeId);
			inodesByPath.put(path, inode);
		}
		
		if(followSymlinks && inode.getStat().isSymlink()) {
			ZKFile symlink = new ZKFile(this, path, File.O_RDONLY|File.O_NOFOLLOW);
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
		Inode inode = inodeTable.issueInode();
		parent.link(inode, basename(path));
		parent.commit();
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
		try {
			Inode inode = inodeForPath(path);
			if(inode.getStat().isDirectory()) throw new EISDIRException(path);
		} catch(ENOENTException e) {
		}
	}
	
	public void assertDirectoryIsEmpty(String path) throws IOException {
		ZKDirectory dir = opendir(path);
		if(dir.list().length > 2) throw new ENOTEMPTYException(path);
	}

	public InodeTable getInodeTable() {
		return inodeTable;
	}
	
	public PubConfig getPubConfig() {
		return pubConfig;
	}
	
	public PrivConfig getPrivConfig() {
		return privConfig;
	}
	
	public FS getStorage() {
		return storage;
	}
	
	public CryptoSupport getCrypto() {
		return crypto;
	}
	
	public LocalConfig getLocalConfig() {
		return localConfig;
	}

	public void setLocalConfig(LocalConfig localConfig) {
		this.localConfig = localConfig;
	}

	@Override
	public void write(String path, byte[] contents) throws IOException {
		mkdirp(dirname(path));
		ZKFile file = open(path, ZKFile.O_WRONLY|ZKFile.O_CREAT);
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
		assertPathIsDirectory(path);
		return new ZKDirectory(this, path);
	}

	@Override
	public void mkdir(String path) throws IOException {
		assertPathIsDirectory(dirname(path));
		assertPathDoesntExist(path);
		
		create(path).getStat().makeDirectory();
		ZKDirectory dir = opendir(path);
		dir.link(dir, ".");
		dir.link(inodeForPath(dirname(path)), "..");
		dir.close();
		chmod(path, 0750); // TODO: default mode
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
		if(path.equals("/")) throw new EINVALException("cannot delete root directory");
		
		ZKDirectory dir = opendir(path);
		dir.rmdir();
		dir.close();
		
		inodesByPath.remove(path);
	}

	@Override
	public void unlink(String path) throws IOException {
		if(inodeForPath(path).getStat().isDirectory()) throw new EISDIRException(path);
		
		ZKDirectory dir = opendir(dirname(path));
		dir.unlink(basename(path));
		dir.close();
		uncache(path);
	}
	
	protected void uncache(String path) {
		inodesByPath.remove(path);
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
		
		ZKFile symlink = open(dest, File.O_WRONLY|File.O_NOFOLLOW|File.O_CREAT);
		symlink.write(source.getBytes());
		symlink.close();
	}
	
	@Override
	public String readlink(String link) throws IOException {
		if(!lstat(link).isSymlink()) throw new EINVALException(link);
		ZKFile symlink = open(link, File.O_RDONLY|File.O_NOFOLLOW);
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
	
	protected void initialize() throws IOException {
		ZKDirectory root = opendir("/");
		root.link(root.inode, ".");
		root.link(root.inode, "..");
		root.close();
	}
}
