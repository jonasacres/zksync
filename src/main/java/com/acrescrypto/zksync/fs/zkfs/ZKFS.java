package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.HashContext;
import com.acrescrypto.zksync.exceptions.*;
import com.acrescrypto.zksync.fs.*;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionToken;
import com.acrescrypto.zksync.utility.HashCache;
import com.acrescrypto.zksync.utility.Util;

// A ZKSync archive.
public class ZKFS extends FS {
	public final static int MAX_SYMLINK_DEPTH = 32;
	
	protected static ConcurrentHashMap<ZKFS, Throwable> openInstances = new ConcurrentHashMap<>();
	
	protected static void addOpenInstance(ZKFS fs) {
		openInstances.put(fs, new Throwable());
	}
	
	protected static void removeOpenInstance(ZKFS fs) {
		openInstances.remove(fs);
	}
	
	public static ConcurrentHashMap<ZKFS,Throwable> getOpenInstances() {
		return openInstances;
	}
	
	public interface ZKFSDirtyMonitor {
		public void notifyDirty(ZKFS fs);
	}
	
	public interface ZKFSLockedOperation {
		public Object op() throws IOException;
	}
	
	protected InodeTable inodeTable;
	protected HashCache<String,ZKDirectory> directoriesByPath;
	protected ZKArchive archive;
	protected RevisionTag baseRevision;
	protected String root;
	protected boolean dirty;
	protected LinkedList<ZKFSDirtyMonitor> dirtyMonitors = new LinkedList<>();
	protected SubscriptionToken<?> cacheToken; 
	protected boolean isReadOnly; // was this specific ZKFS opened RO? (not the whole archive)
	protected int retainCount;
	protected ConcurrentHashMap<ZKFile,Object> openFiles = new ConcurrentHashMap<>();
	protected ConcurrentLinkedQueue<Throwable> retentions = new ConcurrentLinkedQueue<>();
	protected ConcurrentLinkedQueue<Throwable> closures = new ConcurrentLinkedQueue<>();
	
	public final static int MAX_PATH_LEN = 65535;
	
	Logger logger = LoggerFactory.getLogger(ZKFS.class);
	
	public ZKFS(RevisionTag revision, String root) throws IOException {
		if(FS.fileHandleTelemetryEnabled) {
			addOpenInstance(this);
		}
		
		retain();
		this.root = root;
		cacheToken = revision.getArchive().getMaster().getGlobalConfig().subscribe("fs.settings.directoryCacheSize").asInt((s)->{
			if(this.directoriesByPath == null) return;
			try {
				logger.info("ZKFS {} {}: Setting directory cache size to {}; was {}",
						Util.formatArchiveId(revision.getConfig().getArchiveId()),
						baseRevision != null ? Util.formatRevisionTag(baseRevision) : "-",
						s,
						this.directoriesByPath.getCapacity());
				this.directoriesByPath.setCapacity(s);
			} catch(IOException exc) {
				logger.error("ZKFS {} {}: Caught exception updating directory cache size to {}",
						Util.formatArchiveId(revision.getConfig().getArchiveId()),
						baseRevision != null ? Util.formatRevisionTag(baseRevision) : "-",
						s);
			}
		});
		
		int cacheSize = revision.getArchive().getMaster().getGlobalConfig().getInt("fs.settings.directoryCacheSize");
		this.directoriesByPath = new HashCache<String,ZKDirectory>(cacheSize, (String path) -> {
			logger.trace("ZKFS {} {}: Caching directory {}",
					Util.formatArchiveId(revision.getConfig().getArchiveId()),
					Util.formatRevisionTag(baseRevision),
					path);
			assertPathIsDirectory(path);
			ZKDirectory dir = new ZKDirectory(this, path);
			return dir;
		}, (String path, ZKDirectory dir) -> {
			logger.trace("ZKFS {} {}: Evicting directory {} from cache",
					Util.formatArchiveId(revision.getConfig().getArchiveId()),
					Util.formatRevisionTag(baseRevision),
					path);
			dir.commit();
			dir.close();
		});

		rebase(revision);
	}
	
	@Override
	public synchronized void close() throws IOException {
		/* We allow synchronization here because the readOnlyFilesystems cache might evict a ZKFS
		 * (and therefore call close()) independent of a threat actually using the FS. In general,
		 * ZKFS operations are non-threadsafe.
		 */
		if(FS.fileHandleTelemetryEnabled) {
			closures.add(new Throwable());
		}
		
		synchronized(this) {
			if(--retainCount > 0) return;
			assert(retainCount == 0);

			cacheToken.close();
			for(String path : this.directoriesByPath.cachedKeys()) {
				this.directoriesByPath.get(path).forceClose();
			}
			this.directoriesByPath.removeAll();
			inodeTable.close();
			super.close();
		}
		
		closeOpenFiles();
		if(FS.fileHandleTelemetryEnabled) {
			removeOpenInstance(this);
		}
	}
	
	protected void closeOpenFiles() {
		for(ZKFile file : openFiles.keySet()) {
			try {
				file.close();
			} catch (IOException exc) {
				logger.error("ZKFS {} {}: Caught exception closing file {}",
						Util.formatArchiveId(archive.getConfig().getArchiveId()),
						Util.formatRevisionTag(baseRevision),
						file.path,
						exc);
			}
		}
	}
	
	public boolean isClosed() {
		return retainCount == 0;
	}
	
	protected void addOpenFile(ZKFile file) {
		openFiles.put(file, file);
	}
	
	protected void removeOpenFile(ZKFile file) {
		openFiles.remove(file);
	}
	
	public synchronized ZKFS retain() {
		if(FS.fileHandleTelemetryEnabled) {
			retentions.add(new Throwable());
		}
		retainCount++;
		return this;
	}
	
	public ZKFS(RevisionTag revision) throws IOException {
		this(revision, "/");
	}
	
	public RevisionTag commit(RevisionTag[] additionalParents) throws IOException {
		return commitWithTimestamp(additionalParents, -1);
	}
	
	public synchronized RevisionTag commitAndCloseWithTimestamp(RevisionTag[] additionalParents, long timestamp) throws IOException {
		try {
			return commitWithTimestamp(additionalParents, timestamp);
		} finally {
			close();
		}
	}
	
	public RevisionTag commitWithTimestamp(RevisionTag[] additionalParents, long timestamp) throws IOException {
		for(ZKDirectory dir : directoriesByPath.values()) {
			dir.commit();
		}
		
		for(ZKFile file : openFiles.keySet()) {
			file.flush();
		}
		
		synchronized(this) {
			String parentStr = Util.formatRevisionTag(baseRevision);
			for(RevisionTag parent : additionalParents) {
				if(parent.equals(baseRevision)) continue;
				parentStr += ", " + Util.formatRevisionTag(parent);
			}
			Collection<RevisionTag> parents = inodeTable.commitWithTimestamp(additionalParents, timestamp);
			finalizeCommit(parents);
			logger.info("ZKFS {}: Created revtag {} from {}",
					Util.formatArchiveId(archive.getConfig().getArchiveId()),
					Util.formatRevisionTag(baseRevision),
					parentStr);

			Util.debugLog(String.format("ZKFS %s: created revtag %s from %s\n%s\n%s\n",
					archive.getMaster().getName(),
					Util.formatRevisionTag(baseRevision),
					parentStr,
					dump(),
					inodeTable.dumpInodes()));
			archive.getConfig().getRevisionList().dump();
		}
		
		return baseRevision;
	}
	
	public RevisionTag commit() throws IOException {
		return commit(new RevisionTag[0]);
	}
	
	public synchronized RevisionTag commitAndClose() throws IOException {
		try {
			RevisionTag rev = commit(new RevisionTag[0]);
			return rev;
		} finally {
			close();
		}
	}
	
	protected long makeParentHash(Collection<RevisionTag> parents) {
		HashContext ctx = archive.crypto.startHash();
		for(RevisionTag parent : parents) {
			ctx.update(parent.getBytes());
		}
		
		return Util.shortTag(ctx.finish());
	}	

	/** add our new commit to the list of branch tips, and remove our ancestors */
	protected void finalizeCommit(Collection<RevisionTag> parents) throws IOException {
		long parentHash = makeParentHash(parents);
		long height = 1 + baseRevision.getHeight();
		baseRevision = new RevisionTag(inodeTable.inode.getRefTag(), parentHash, height);
		dirty = false;
		
		RevisionList list = archive.config.getRevisionList();
		RevisionTree tree = archive.config.getRevisionTree();
		tree.addParentsForTag(baseRevision, parents);		
		list.addBranchTip(baseRevision);
		list.consolidate(baseRevision);
		list.write();
		
		archive.config.swarm.announceTips();
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
		
		try(ZKDirectory root = opendir("/")) {
			long inodeId = root.inodeForPath(absPath);
			Inode inode = inodeTable.inodeWithId(inodeId);
			
			return inode;
		} catch (EISNOTDIRException e) {
			throw new ENOENTException(path);
		}
	}
	
	protected Inode inodeForPathWithSymlinks(String path, int depth) throws IOException {
		if(directoriesByPath.hasCached(path)) {
			synchronized(this) {
				if(directoriesByPath.hasCached(path)) {
					return directoriesByPath.get(path).getInode();
				}
			}
		}
		
		if(depth > MAX_SYMLINK_DEPTH) throw new ELOOPException(path);
		String absPath = absolutePath(path);
		if(path.equals("")) throw new ENOENTException(path);
		if(absPath.equals("/")) {
			return inodeTable.inodeWithId(InodeTable.INODE_ID_ROOT_DIRECTORY);
		}
		
		Inode inode;
		try(ZKDirectory root = opendir("/")) {
			long inodeId = root.inodeForPath(absPath);
			inode = inodeTable.inodeWithId(inodeId);
		} catch (EISNOTDIRException e) {
			throw new ENOENTException(path);
		}
		
		if(inode.getStat().isSymlink()) {
			try(ZKFile symlink = new ZKFile(this, path, File.O_RDONLY|File.O_NOFOLLOW|ZKFile.O_LINK_LITERAL, true)) {
				String linkPath = new String(symlink.read(MAX_PATH_LEN));
				return inodeForPathWithSymlinks(linkPath, depth + 1);
			}
		}
		
		return inode;
	}
	
	protected Inode create(String path) throws IOException {
		try(ZKDirectory dir = opendir(dirname(path))) {
			return create(path, dir);
		}
	}
	
	protected Inode create(String path, ZKDirectory parent) throws IOException {
		assertPathLegal(path);
		Inode inode = inodeTable.issueInode();
		parent.link(inode, basename(path));
		parent.flush();
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
		logger.trace("ZKFS {} {}: Is directory: {} ({}) -> {}",
				Util.formatArchiveId(archive.getConfig().getArchiveId()),
				Util.formatRevisionTag(baseRevision),
				path,
				inode.getStat().getInodeId(),
				inode.getStat().isDirectory());
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
		try(ZKDirectory dir = opendir(path)) {
			if(dir.list().size() > 0) throw new ENOTEMPTYException(path);
		}
	}

	public void assertWritable(String path) throws EACCESException {
		if(isReadOnly) throw new EACCESException(path);
	}

	public InodeTable getInodeTable() {
		return inodeTable;
	}
	
	@Override
	public void write(String path, byte[] contents, int offset, int length) throws IOException {
		assertWritable(path);
		mkdirp(dirname(path));
		
		try(ZKFile file = open(path, ZKFile.O_WRONLY|ZKFile.O_CREAT|ZKFile.O_TRUNC)) {
			file.write(contents, offset, length);
		}
	}

	@Override
	public ZKFile open(String path, int mode) throws IOException {
		if(isReadOnly && ((mode & File.O_WRONLY) != 0)) {
			throw new EACCESException(path);
		}
		assertPathIsNotDirectory(path, (mode & File.O_NOFOLLOW) == 0);
		return new ZKFile(this, path, mode, true);
	}
	
	public ZKFile open(Inode inode, int mode) throws IOException {
		String path = "(inode " + inode.getStat().getInodeId() + ")";
		
		if(isReadOnly && ((mode & File.O_WRONLY) != 0)) {
			throw new EACCESException(path);
		}
		
		if(inode.getStat().isDirectory()) {
			throw new EISDIRException(path);
		}
		
		return new ZKFile(this, inode, mode, true);
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
		assertWritable(path);
		try(ZKFile file = open(path, File.O_RDWR)) {
			file.truncate(size);
		}
	}

	@Override
	public ZKDirectory opendir(String path) throws IOException {
		String canonPath = canonicalPath(path);
		synchronized(this) {
			return directoriesByPath.get(canonPath).retain();
		}
	}

	@Override
	public void mkdir(String path) throws IOException {
		assertWritable(path);
		assertPathIsDirectory(dirname(path));
		assertPathDoesntExist(path);
		try(ZKDirectory dir = opendir(dirname(path))) {
			dir.mkdir(basename(path)).close();
		}
	}
	
	@Override
	public void mkdirp(String path) throws IOException {
		try {
			assertWritable(path);
			assertPathIsDirectory(path);
		} catch(ENOENTException e) {
			mkdirp(dirname(path));
			mkdir(path);
		}
	}

	@Override
	public void rmdir(String path) throws IOException {
		assertWritable(path);
		assertDirectoryIsEmpty(path);
		if(path.equals("/")) return; // quietly ignore for rmrf("/") support; used to throw new EINVALException("cannot delete root directory");
		
		synchronized(this) {
			assertWritable(path);
			assertDirectoryIsEmpty(path);
			
			try(ZKDirectory dir = opendir(path)) {
				dir.rmdir();
			}
			uncache(path);
		}
	}

	@Override
	public void unlink(String path) throws IOException {
		assertWritable(path);
		if(inodeForPath(path, false).getStat().isDirectory()) throw new EISDIRException(path);
		
		synchronized(this) {
			assertWritable(path);
			if(inodeForPath(path, false).getStat().isDirectory()) throw new EISDIRException(path);
			
			try(ZKDirectory dir = opendir(dirname(path))) {
				dir.unlink(basename(path));
			}
			uncache(path);
		}
	}
	
	@Override
	public void link(String source, String dest) throws IOException {
		assertWritable(dest);
		
		synchronized(this) {
			assertWritable(dest);
			Inode target = inodeForPath(source);
			try(ZKDirectory destDir = opendir(dirname(dest))) {
				destDir.link(target, basename(dest));
			}
		}
	}
	
	@Override
	public void symlink(String source, String dest) throws IOException {
		assertWritable(dest);
		Inode instance = create(dest);
		instance.getStat().makeSymlink();
		
		try(ZKFile symlink = open(dest, File.O_WRONLY|File.O_NOFOLLOW|File.O_CREAT|ZKFile.O_LINK_LITERAL)) {
			symlink.write(source.getBytes());
		}
	}
	
	@Override
	public void symlink_unsafe(String source, String dest) throws IOException {
		symlink(source, dest);
	}
	
	@Override
	public String readlink(String link) throws IOException {
		if(!lstat(link).isSymlink()) throw new EINVALException(link);
		try(ZKFile symlink = open(link, File.O_RDONLY|File.O_NOFOLLOW|ZKFile.O_LINK_LITERAL)) {
			String target = new String(symlink.read());
			return target;
		}
	}
	
	@Override
	public String readlink_unsafe(String link) throws IOException {
		return readlink(link);
	}

	@Override
	public void mknod(String path, int type, int major, int minor) throws IOException {
		assertWritable(path);
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
		assertWritable(path);
		create(path).getStat().makeFifo();
	}
	
	@Override
	public void chmod(String path, int mode) throws IOException {
		assertWritable(path);
		logger.debug("ZKFS {} {}: chmod {} 0{}",
				Util.formatArchiveId(archive.getConfig().getArchiveId()),
				Util.formatRevisionTag(baseRevision),
				path,
				String.format("%3o", mode));
		Inode inode = inodeForPath(path);
		inode.getStat().setMode(mode);
		markDirty();
	}

	@Override
	public void chown(String path, int uid) throws IOException {
		assertWritable(path);
		logger.debug("ZKFS {} {}: chown {} {}",
				Util.formatArchiveId(archive.getConfig().getArchiveId()),
				Util.formatRevisionTag(baseRevision),
				path,
				uid);
		Inode inode = inodeForPath(path);
		inode.getStat().setUid(uid);
		markDirty();
	}

	@Override
	public void chown(String path, String name) throws IOException {
		assertWritable(path);
		logger.debug("ZKFS {} {}: chown {} '{}'",
				Util.formatArchiveId(archive.getConfig().getArchiveId()),
				Util.formatRevisionTag(baseRevision),
				path,
				name);
		Inode inode = inodeForPath(path);
		inode.getStat().setUser(name);
		markDirty();
	}

	@Override
	public void chgrp(String path, int gid) throws IOException {
		assertWritable(path);
		logger.debug("ZKFS {} {}: chgrp {} {}",
				Util.formatArchiveId(archive.getConfig().getArchiveId()),
				Util.formatRevisionTag(baseRevision),
				path,
				gid);
		Inode inode = inodeForPath(path);
		inode.getStat().setGid(gid);
		markDirty();
	}

	@Override
	public void chgrp(String path, String group) throws IOException {
		assertWritable(path);
		logger.debug("ZKFS {} {}: chgrp {} '{}'",
				Util.formatArchiveId(archive.getConfig().getArchiveId()),
				Util.formatRevisionTag(baseRevision),
				path,
				group);
		Inode inode = inodeForPath(path);
		inode.getStat().setGroup(group);
		markDirty();
	}

	@Override
	public void setMtime(String path, long mtime) throws IOException {
		assertWritable(path);
		logger.debug("ZKFS {} {}: set mtime {} {}",
				Util.formatArchiveId(archive.getConfig().getArchiveId()),
				Util.formatRevisionTag(baseRevision),
				path,
				mtime);
		Inode inode = inodeForPath(path, false);
		inode.getStat().setMtime(mtime);
		markDirty();
	}

	@Override
	public void setCtime(String path, long ctime) throws IOException {
		assertWritable(path);
		logger.debug("ZKFS {} {}: set ctime {} {}",
				Util.formatArchiveId(archive.getConfig().getArchiveId()),
				Util.formatRevisionTag(baseRevision),
				path,
				ctime);
		Inode inode = inodeForPath(path, false);
		inode.getStat().setCtime(ctime);
		markDirty();
	}

	@Override
	public void setAtime(String path, long atime) throws IOException {
		assertWritable(path);
		logger.debug("ZKFS {} {}: set atime {} {}",
				Util.formatArchiveId(archive.getConfig().getArchiveId()),
				Util.formatRevisionTag(baseRevision),
				path,
				atime);
		Inode inode = inodeForPath(path, false);
		inode.getStat().setAtime(atime);
		markDirty();
	}
	
	@Override
	public ZKFS scopedFS(String subpath) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public ZKFS unscopedFS() throws IOException {
		ZKFS unscoped = new ZKFS(baseRevision);
		if(isReadOnly) {
			unscoped.setReadOnly();
		}
		return unscoped;
	}
	
	public synchronized void uncache(String path) throws IOException {
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
	
	public synchronized String dump() throws IOException {
		StringBuilder builder = new StringBuilder();
		builder.append("Revision " + Util.formatRevisionTag(baseRevision) + "\n");
		dump("/", 1, builder);
		return builder.toString();
	}
	
	public synchronized void dump(String path, int depth, StringBuilder builder) throws IOException {
		String padding = new String(new char[depth]).replace("\0", "  ");
		try(ZKDirectory dir = opendir(path)) {
			LinkedList<String> sorted = new LinkedList<>(dir.list());
			sorted.sort(null);
			
			for(String subpath : sorted) {
				String fqSubpath = Paths.get(path, subpath).toString();
				Inode inode = inodeForPath(fqSubpath, false);
				builder.append(String.format("%-30s inodeId %4d, size %8d, identity %16x, type %02x, nlink %02d, %s\n",
						padding + subpath,
						inode.stat.getInodeId(),
						inode.stat.getSize(),
						inode.identity,
						inode.getStat().getType(),
						inode.nlink,
						Util.formatRefTag(inode.getRefTag())));
				if(inode.stat.isDirectory()) {
					dump(fqSubpath, depth+1, builder);
				}
			}
		}
	}

	public synchronized void rebase(RevisionTag revision) throws IOException {
		logger.info("ZKFS {} {}: Rebasing to revision {}",
				Util.formatArchiveId(revision.getConfig().getArchiveId()),
				baseRevision != null ? Util.formatRevisionTag(baseRevision) : "-",
				Util.formatRevisionTag(revision));
		this.archive = revision.getArchive();
		
		this.baseRevision = revision;
		
		if(this.directoriesByPath != null) {
			this.directoriesByPath.removeAll();
		}
		
		if(this.inodeTable != null) {
			this.inodeTable.close();
		}

		this.inodeTable = new InodeTable(this, revision);
		this.dirty = false;
	}
	
	public synchronized void addMonitor(ZKFSDirtyMonitor monitor) {
		dirtyMonitors.add(monitor);
	}
	
	public synchronized void removeMonitor(ZKFSDirtyMonitor monitor) {
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

	public String toString() {
		return this.getClass().getSimpleName() + " "
				+ Util.formatArchiveId(archive.getConfig().getArchiveId()) + " "
				+ Util.formatRevisionTag(this.baseRevision);
	}
	
	public ZKFS setReadOnly() {
		this.isReadOnly = true;
		return this;
	}
	
	public boolean isReadOnly() {
		return isReadOnly;
	}
	
	/** Acts as a "big lock" on the filesystem. */
	public synchronized Object lockedOperation(ZKFSLockedOperation op) throws IOException {
		return op.op();
	}
	
	public String canonicalPath(String path) throws IOException {
		while(path.startsWith("//")) path = path.substring(1);
		if(path.equals("/")) return path;
		if(path.equals(".")) return "/";
		if(path.charAt(0) != '/') path = absolutePath(path);
		if(directoriesByPath.hasCached(path)) return path; // only canonical paths are in the cache
		
		try {
			return canonicalPath(readlink(path));
		} catch(EINVALException exc) {}
		
		
		String parent = dirname(path);
		String parentCanon = canonicalPath(parent);
		if(!parentCanon.endsWith("/")) parentCanon += "/";
		return parentCanon + basename(path);
	}
	
	public ConcurrentLinkedQueue<Throwable> getRetentions() {
		return retentions;
	}
	
	public ConcurrentLinkedQueue<Throwable> getClosures() {
		return closures;
	}
	
	public int getRetainCount() {
		return retainCount;
	}

	public int getDirectoryCacheSize() {
		return directoriesByPath.cachedSize();
	}
}
