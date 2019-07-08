package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.acrescrypto.zksync.exceptions.*;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.utility.Util;

public class ZKDirectory extends ZKFile implements Directory {
	public final static byte SERIALIZATION_TYPE_BYTE = 0;
	public final static byte SERIALIZATION_TYPE_SHORT = 1;
	public final static byte SERIALIZATION_TYPE_INT = 2;
	public final static byte SERIALIZATION_TYPE_LONG = 3;
	
	// TODO Someday: (redesign) Allow partial reads of directories
	ConcurrentHashMap<String,Long> entries;
	boolean dirty;
	
	public final static int MAX_NAME_LEN = 255;
	
	public ZKDirectory(ZKFS fs, String path) throws IOException {
		super(fs, path, fs.archive.config.isReadOnly() || fs.isReadOnly ? O_RDONLY : O_RDWR, true);
		init();
	}
	
	public ZKDirectory(ZKFS fs, Inode inode) throws IOException {
		super(fs, inode, fs.archive.config.isReadOnly() || fs.isReadOnly ? O_RDONLY : O_RDWR, true);
		init();
	}
	
	protected void init() throws IOException {
		try {
			entries = new ConcurrentHashMap<String,Long>();
			byte[] contents = read((int) inode.getStat().getSize());
			deserialize(contents);
		} catch(Throwable exc) {
			close();
			throw exc;
		}
	}
	
	@Override
	public ZKDirectory retain() {
		super.retain();
		return this;
	}
	
	@Override
	public Collection<String> list() throws IOException {
		return list(0);
	}
	
	@Override
	public Collection<String> list(int opts) throws IOException {
		Set<String> entrySet = new HashSet<String>(entries.keySet());
		if((opts & LIST_OPT_INCLUDE_DOT_DOTDOT) == 0) {
			entrySet.remove(".");
			entrySet.remove("..");
		}
		
		if((opts & LIST_OPT_OMIT_DIRECTORIES) != 0) {
			for(String entry : entries.keySet()) {
				if(fs.stat(Paths.get(path, entry).toString()).isDirectory()) {
					entrySet.remove(entry);
				}
			}
		}
		
		LinkedList<String> sorted = new LinkedList<>(entrySet);
		sorted.sort(null);
		return sorted;
	}
	
	@Override
	public boolean contains(String filename) {
		return entries.containsKey(filename);
	}
	
	@Override
	public LinkedList<String> listRecursive() throws IOException {
		return listRecursive(0);
	}
	
	@Override
	public LinkedList<String> listRecursive(int opts) throws IOException {
		LinkedList<String> results = new LinkedList<String>();
		walk(opts, (subpath, stat, broken, parent)->{
			results.add(subpath);
		});
		return results;
	}
	
	@Override
	public boolean walk(DirectoryWalkCallback cb) throws IOException {
		return walk(0, cb);
	}
	
	@Override
	public boolean walk(int opts, DirectoryWalkCallback cb) throws IOException {
		HashSet<Long> inodeHistory = new HashSet<>();
		try {
			zkfs.lockedOperation(()->{
				// lock the FS while we do this to keep the directory tree from changing underneath us
				walkRecursiveIterate(opts, inodeHistory, "", cb);
				return null;
			});
			return true;
		} catch(WalkAbortException exc) {
			return false;
		}
	}
	
	protected void walkRecursiveIterate(int opts, HashSet<Long> inodeHistory, String prefix, DirectoryWalkCallback cb) throws IOException {
		long inodeId = inode.stat.getInodeId();
		boolean followSymlinks = (opts & Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS) == 0;
		if(inodeHistory.contains(inodeId)) return;
		inodeHistory.add(inodeId);
		
		for(String entry : list(opts & ~Directory.LIST_OPT_OMIT_DIRECTORIES)) {
			String subpath = Paths.get(prefix, entry).toString(); // what we return in our results
			String realSubpath = Paths.get(path, entry).toString(); // what we can look up directly in fs
			try {
				long entryInodeId = entries.get(entry);
				Inode entryInode = zkfs.inodeTable.inodeWithId(entryInodeId);
				if(followSymlinks && entryInode.stat.isSymlink()) {
					entryInode = zkfs.inodeForPath(realSubpath);
				}
				
				if(entryInode.stat.isDirectory()) {
					boolean isDotDir = entry.equals(".") || entry.equals("..");
					if((opts & Directory.LIST_OPT_OMIT_DIRECTORIES) == 0) {
						cb.foundPath(subpath, entryInode.stat, false, this);
					}
					
					if(!isDotDir) {
						try(ZKDirectory dir = zkfs.opendirSemicache(entryInode)) {
							dir.walkRecursiveIterate(opts, inodeHistory, subpath, cb);
						}
					}
				} else {
					cb.foundPath(subpath, entryInode.stat, false, this);
				}
			} catch(ENOENTException exc) {
				try {
					Stat lstat = fs.lstat(realSubpath);
					if(lstat.isSymlink()) {
						cb.foundPath(subpath, lstat, true, this);
					}
				} catch(ENOENTException exc2) {
					// ignore ENOENTs that are not just broken symlinks since someone may have deleted the path as we traversed
				}
			}
		}
		
		inodeHistory.remove(inodeId);
	}
	
	public Collection<String> findPathsForInode(long inodeId) throws IOException {
		LinkedList<String> paths = new LinkedList<>();
		if(inodeId == inode.getStat().getInodeId()) {
			paths.add(path);
		}
		
		walk(LIST_OPT_DONT_FOLLOW_SYMLINKS, (subpath, stat, isInvalidSymlink, parent)->{
			if(stat.getInodeId() == inodeId) {
				paths.add(Paths.get(this.path, subpath).toString());
			}
		});
		
		return paths;
	}
	
	public long inodeForName(String name) throws IOException {
		if(!entries.containsKey(name)) {
			throw new ENOENTException(Paths.get(path, name).toString());
		}
		return entries.get(name);
	}
	
	public long inodeForPath(String path) throws IOException {
		String[] comps = path.split("/");
		if(comps.length > 1) {
			try {
				String nextDir = Paths.get(this.path, comps[0]).toString();
				String subpath = String.join("/", Arrays.copyOfRange(comps, 1, comps.length));
				try(ZKDirectory dir = zkfs.opendir(nextDir)) {
					return dir.inodeForPath(subpath);
				}
			} catch (EISNOTDIRException e) {
				throw new ENOENTException(path);
			}
		}
		
		return inodeForName(comps.length == 0 ? "/" : comps[0]);
	}
	
	public void updateLink(Long inodeId, String link) throws IOException {
		zkfs.lockedOperation(()->{
			synchronized(this) {
				String fullPath = Paths.get(path, link).toString();
				if(!isValidName(link)) {
					throw new EINVALException(link + ": invalid name");
				}
				
				if(entries.containsKey(link)) {
					Long existing = entries.get(link);
					if(existing.equals(inodeId)) {
						return null;
					}
					
					Util.debugLog(String.format("ZKDirectory %s: %s In directory %d %016x %s, renumbering %s %s -> %s",
							zkfs.getArchive().getMaster().getName(),
							Util.formatRevisionTag(zkfs.baseRevision),
							inode.getStat().getInodeId(),
							inode.identity,
							getPath(),
							link,
							existing,
							inodeId));
					
					entries.remove(link);
					zkfs.uncache(fullPath);
					dirty = true;
				}
				
				if(inodeId == null) {
					return null;
				}
				
				entries.put(link, inodeId);
				dirty = true;
				
				return null;
			}
		});
	}

	public void link(Inode inode, String link) throws IOException {
		assertWritable();
		zkfs.lockedOperation(()->{
			synchronized(this) {
				assertWritable();
				if(!isValidName(link)) throw new EINVALException(link + ": invalid filename");
				if(entries.containsKey(link)) {
					throw new EEXISTSException(Paths.get(path, link).toString());
				}
				entries.put(link, inode.getStat().getInodeId());
				inode.addLink();
				dirty = true;
				zkfs.markDirty();
				return null;
			}
		});
	}
	
	@Override
	public void link(String target, String link) throws IOException {
		zkfs.lockedOperation(()->{
			synchronized(this) {
				link(zkfs.inodeForPath(target), link);
				return null;
			}
		});
	}

	@Override
	public void link(File target, String link) throws IOException {
		zkfs.lockedOperation(()->{
			synchronized(this) {
				ZKFile zkfile = (ZKFile) target;
				link(zkfile.getInode(), link);
				return null;
			}
		});
	}
	
	public boolean isValidName(String name) {
		String illegalChars[] = new String[] {
				"/",
				"\\",
				"\0"
			};
		if(name.getBytes().length > MAX_NAME_LEN) return false;
		for(String c : illegalChars) {
			if(name.indexOf(c) >= 0) return false;
		}
		
		return true;
	}

	@Override
	public void unlink(String name) throws IOException {
		assertWritable();
		zkfs.lockedOperation(()->{
			synchronized(this) {
				assertWritable();
				if(name.equals(".") || name.equals("..")) throw new EINVALException("cannot unlink " + name);
				
				String fullPath = Paths.get(path, name).toString();
				
				try {
					Inode inode = zkfs.inodeForPath(fullPath, false);
					inode.removeLink();
				} catch(ENOENTException exc) {
					// if we have a dead reference, we should be able to unlink it no questions asked
				}
				
				entries.remove(name);
				zkfs.uncache(fullPath);
				dirty = true;
				zkfs.markDirty();
				return null;
			}
		});
	}
	
	public void rmdir() throws IOException {
		assertWritable();
		zkfs.lockedOperation(()->{
			synchronized(this) {
				assertWritable();
				Long parentInodeId = entries.get("..");
				if(parentInodeId != null && !parentInodeId.equals(this.getStat().getInodeId())) {
					try(ZKDirectory parent = zkfs.opendir(fs.dirname(this.path))) {
						parent.getInode().removeLink();
						parent.unlink(fs.basename(this.path));
					}
				}
				
				if(entries.containsKey(".")) {
					/* hard to imagine a directory that doesn't have . but let's avoid the possibility of
					 * creating a negative link count here */
					inode.removeLink();
				}
				
				entries.clear();
				this.bufferedPage = null;
				return null;
			}
		});
	}
	
	public Directory mkdir(String name) throws IOException {
		return (Directory) zkfs.lockedOperation(()->{
			synchronized(this) {
				String fullPath = Paths.get(path, name).toString();
				if(entries.containsKey(name)) throw new EEXISTSException(fullPath);
				zkfs.create(fullPath, this).getStat().makeDirectory();
	
				ZKDirectory dir = zkfs.opendir(fullPath);
				dir.inode.addLink(); // .
				dir.link(inode, "..");
				dir.flush();
				fs.chmod(fullPath, zkfs.archive.master.getGlobalConfig().getInt("fs.default.directoryMode"));
	
				return dir;
			}
		});
	}
	
	public RefTag commit() throws IOException {
		if(!dirty) return tree.getRefTag();
		assertWritable();
		
		zkfs.lockedOperation(()->{
			synchronized(this) {
				if(!dirty) return null;
				assertWritable();
				
				rewind();
				truncate(0);
				
				write(serialize());
				flush();
				dirty = false;
				
				return null;
			}
		});
		
		return tree.getRefTag();
	}
	
	private void deserialize(byte[] serialized) {
		entries.clear();
		entries.put(".", this.getInode().getStat().getInodeId());
		if(serialized.length == 0) return; // empty directory
		
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		byte inodeIdType = buf.get();
		int inodeIdSize = entrySizeForType(inodeIdType);
		
		// serializing .. with an implicit name lets us save 3 bytes per directory
		assertIntegrity(buf.remaining() >= inodeIdSize, "Directory seems truncated; does not contain enough bytes for .. inode (expected " + inodeIdSize + ", have " + buf.remaining());
		long dotDotInodeId = deserializeValueWithType(buf, inodeIdType);
		entries.put("..", dotDotInodeId);
		
		try {
			while(buf.hasRemaining()) {
				assertIntegrity(buf.remaining() >= inodeIdSize, "Directory seems truncated; does not contain enough bytes for next inodeId (expected " + inodeIdSize + ", have " + buf.remaining() + ")");
				long inodeId = deserializeValueWithType(buf, inodeIdType);
				assertIntegrity(buf.remaining() >= 1, "Directory seems truncated; does not contain enough bytes for next path length (expected 1, have " + buf.remaining() + ")");
				int pathLen = Util.unsignByte(buf.get());
	
				// Don't check existence of inodes; it will trip up merges
				assertIntegrity(inodeId >= 0, String.format("Directory references invalid inodeId %d", inodeId));
				assertIntegrity(pathLen >= 0, "Directory references negative path length (" + pathLen + ")");
				assertIntegrity(pathLen <= buf.remaining(), "Directory appears truncated; does not contain enough bytes for next path (expected " + pathLen + ", have " + buf.remaining() + ")");
				assertIntegrity(pathLen <= MAX_NAME_LEN, "Directory references name of illegal length " + pathLen);
				
				byte[] pathBuf = new byte[pathLen];
				buf.get(pathBuf);
				
				String path = new String(pathBuf);
				if(isValidName(path)) {
					/* Don't let maliciously-crafted directories deserialize evil paths.
					 * It does mean we have non-unlinkable files, but we're already dealing with
					 * an archive manually created by bad people, so who knows what else is going on?
					 * 
					 * Because of that, I considered maybe throwing an exception here, but there's
					 * also no guarantee that someone opens the directory, and potentially a form
					 * of attack in which an attacker sabotages a directory the user is depending on
					 * being able to read. Not entirely sure this is the right way to go, but...
					 * seemed like a good idea at the time. :p 
					 * */
					entries.put(path, inodeId);
				}
			}
		} catch(Exception exc) {
			logger.warn("FS {}: Unable to deserialize directory {} - encountered exception at offset {} of length {}",
					Util.formatArchiveId(zkfs.getArchive().getConfig().getArchiveId()),
					path,
					buf.position(),
					buf.limit());
			throw exc;
		}
		
		dirty = false;
	}
	
	protected byte[] serialize() throws IOException {
		byte[] types = new byte[1]; // entry 0 is inode type
		entries.forEach((name, inodeId)->{
			types[0] = (byte) Math.max(types[0], serializationType(inodeId));
		});
		
		int inodeIdSize = entrySizeForType(types[0]);
		int size = 1 + inodeIdSize; // 1 byte for inode type, plus .. entry
		
		for(String path : entries.keySet()) {
			if(path.equals(".") || path.equals("..")) {
				continue;
			}
			
			size += inodeIdSize + 1 + path.getBytes().length; // inode number + path_len + path 
		}
		
		ByteBuffer buf = ByteBuffer.allocate(size);
		buf.put(types);
		serializeValueWithType(buf, types[0], entries.get(".."));
		entries.forEach((name, inodeId)->{
			if(name.equals(".") || name.equals("..")) {
				return; // . is implicit, .. is serialized with implicit path name to save bytes
			}
			if(!isValidName(name)) {
				logger.error("Refusing to serialize illegal path: " + Paths.get(path, name).toString());
				return; 
			}
			serializeValueWithType(buf, types[0], inodeId);
			buf.put((byte) name.getBytes().length);
			buf.put(name.getBytes());
		});
		
		return buf.array();
	}
	
	protected void serializeValueWithType(ByteBuffer buf, byte type, long value) {
		switch(type) {
		case SERIALIZATION_TYPE_BYTE:
			buf.put((byte) value);
			break;
		case SERIALIZATION_TYPE_SHORT:
			buf.putShort((short) value);
			break;
		case SERIALIZATION_TYPE_INT:
			buf.putInt((int) value);
			break;
		case SERIALIZATION_TYPE_LONG:
			buf.putLong((long) value);
			break;
		default:
			assertIntegrity(false, "Unexpected field datatype " + type);
		}
	}
	
	protected long deserializeValueWithType(ByteBuffer buf, byte type) {
		switch(type) {
		case SERIALIZATION_TYPE_BYTE:
			return Util.unsignByte(buf.get());
		case SERIALIZATION_TYPE_SHORT:
			return Util.unsignShort(buf.getShort());
		case SERIALIZATION_TYPE_INT:
			return Util.unsignInt(buf.getInt());
		case SERIALIZATION_TYPE_LONG:
			return buf.getLong();
		default:
			assertIntegrity(false, "Unexpected field datatype " + type);
			return Long.MIN_VALUE; // can't get here, but have to keep compiler happy
		}
	}
	
	protected byte serializationType(long inodeId) {
		if(inodeId < 1 << 8) {
			return SERIALIZATION_TYPE_BYTE;
		} else if(inodeId < 1 << 16) {
			return SERIALIZATION_TYPE_SHORT;
		} else if(inodeId < 1 << 32) {
			return SERIALIZATION_TYPE_INT;
		} else {
			return SERIALIZATION_TYPE_LONG;
		}
	}
	
	protected int entrySizeForType(byte type) {
		switch(type) {
		case SERIALIZATION_TYPE_BYTE:
			return 1;
		case SERIALIZATION_TYPE_SHORT:
			return 2;
		case SERIALIZATION_TYPE_INT:
			return 4;
		case SERIALIZATION_TYPE_LONG:
			return 8;
		default:
			throw new InvalidArchiveException("Unexpected ZKDirectory serialzation type " + type);
		}
	}
	
	protected void assertWritable() throws EACCESException {
		if(zkfs.archive.config.isReadOnly()) throw new EACCESException("cannot modify directories when archive is opened read-only");
	}

	public synchronized void remap(HashMap<Long, Long> remappedIds) {
		StringBuilder sb = new StringBuilder(String.format("ZKDirectory %s: (directory inodeId %d) remapping from base revision %s",
				zkfs.archive.master.getName(),
				inode.getStat().getInodeId(),
				Util.formatRevisionTag(zkfs.baseRevision)));
		ConcurrentHashMap<String, Long> remappedEntries = new ConcurrentHashMap<>();
		entries.forEach((name, inodeId)->{
			long newId = remappedIds.getOrDefault(inodeId, inodeId);
			dirty |= newId != inodeId;
			remappedEntries.put(name, newId);
			sb.append(String.format("\n\t%30s %3d -> %3d %s",
					name,
					inodeId,
					newId,
					newId != inodeId ? "CHANGED" : ""));
		});
		
		Util.debugLog(sb.toString());
		entries = remappedEntries;
	}

	/** Test purposes only. Attempt to determine fully-qualified path for this directory. Useful if we opened
	 * from an inode.
	 * 
	 *  @throws IOException
	 **/
	@Deprecated
	public String calculatePath() throws IOException {
		if(getStat().getInodeId() == InodeTable.INODE_ID_ROOT_DIRECTORY) return "/";
		
		Long parentInodeId = entries.get("..");		
		Inode parentInode = zkfs.inodeTable.inodeWithId(parentInodeId);
		if(parentInode.isDeleted()) throw new ENOENTException("Unable to locate parent inodeId " + parentInodeId + " to directory with inodeId " + inode.getStat().getInodeId());
		if(!parentInode.stat.isDirectory()) throw new EISNOTDIRException("Parent inodeId " + parentInodeId + " to directory with inodeId " + inode.getStat().getInodeId() + " is not a directory");
		
		try(ZKDirectory parentDir = zkfs.opendirSemicache(parentInode)) {
			String[] name = new String[1];
			parentDir.entries.forEach((entryName, entryInodeId)->{
				if(entryInodeId == getStat().getInodeId()) {
					name[0] = entryName;
				}
			});
			
			if(name[0] == null) throw new ENOENTException("Unable to locate directory with inodeId " + getStat().getInodeId() + " in parent directory with inodeId " + parentInodeId);
			return Paths.get(parentDir.calculatePath(), name[0]).toString();
		}
	}

	public ConcurrentHashMap<String,Long> getEntries() {
		return entries;
	}
}
