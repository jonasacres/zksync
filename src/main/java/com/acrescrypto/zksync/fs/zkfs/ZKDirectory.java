package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
	// TODO Someday: (redesign) Allow partial reads of directories
	ConcurrentHashMap<String,Long> entries;
	boolean dirty;
	
	public final static int MAX_NAME_LEN = 255;
	
	public ZKDirectory(ZKFS fs, String path) throws IOException {
		super(fs, path, fs.archive.config.isReadOnly() || fs.isReadOnly ? O_RDONLY : O_RDWR, true);
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
		walk(opts, (subpath, stat, broken)->{
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
			walkRecursiveIterate(opts, inodeHistory, "", cb);
			return true;
		} catch(WalkAbortException exc) {
			return false;
		}
	}
	
	protected void walkRecursiveIterate(int opts, HashSet<Long> inodeHistory, String prefix, DirectoryWalkCallback cb) throws IOException {
		long inodeId = inode.stat.getInodeId();
		if(inodeHistory.contains(inodeId)) return;
		inodeHistory.add(inodeId);
		
		for(String entry : list(opts & ~Directory.LIST_OPT_OMIT_DIRECTORIES)) {
			String subpath = Paths.get(prefix, entry).toString(); // what we return in our results
			String realSubpath = Paths.get(path, entry).toString(); // what we can look up directly in fs
			try {
				Stat stat;
				boolean isInvalidSymlink = false;
				
				if((opts & Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS) == 0) {
					stat = fs.stat(realSubpath);
				} else {
					stat = fs.lstat(realSubpath);
					if(stat.isSymlink() && !fs.exists(realSubpath)) {
						isInvalidSymlink = true;
					}
				}
				
				if(stat.isDirectory()) {
					boolean isDotDir = entry.equals(".") || entry.equals("..");
					if((opts & Directory.LIST_OPT_OMIT_DIRECTORIES) == 0) {
						cb.foundPath(subpath, stat, isInvalidSymlink);
					}
					
					if(!isDotDir) {
						try(ZKDirectory dir = zkfs.opendir(realSubpath)) {
							dir.walkRecursiveIterate(opts, inodeHistory, subpath, cb);
						}
					}
				} else {
					cb.foundPath(subpath, stat, isInvalidSymlink);
				}
			} catch(ENOENTException exc) {
				try {
					Stat lstat = fs.lstat(realSubpath);
					if(lstat.isSymlink()) {
						cb.foundPath(subpath, lstat, true);
					}
				} catch(ENOENTException exc2) {
					// ignore ENOENTs that are not just broken symlinks since someone may have deleted the path as we traversed
				}
			}
		}
		
		inodeHistory.remove(inodeId);
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
	
	public void updateLink(Long inodeId, String link, ArrayList<Inode> toUnlink) throws IOException {
		zkfs.lockedOperation(()->{
			synchronized(this) {
				if(!isValidName(link)) throw new EINVALException(link + ": invalid name");
				if(entries.containsKey(link)) {
					Long existing = entries.get(link);
					if(existing.equals(inodeId)) return null;
					
					// we need to remove the old link, but defer action on the inode in case we're relinking this inode somewhere else
					String fullPath = Paths.get(path, link).toString();
					toUnlink.add(zkfs.inodeForPath(fullPath));
					entries.remove(link);
					zkfs.uncache(fullPath);
					dirty = true;
				}
				
				if(inodeId == null) return null; // above if clause already unlinked
				link(zkfs.inodeTable.inodeWithId(inodeId), link);
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
				
				entries.clear();
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
				dir.link(dir, ".");
				dir.link(inode, "..");
				dir.flush();
				fs.chmod(fullPath, zkfs.archive.master.getGlobalConfig().getInt("fs.default.directoryMode"));
	
				return dir;
			}
		});
	}
	
	public void commit() throws IOException {
		if(!dirty) return;
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
	}
	
	private void deserialize(byte[] serialized) {
		entries.clear();
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		try {
			while(buf.hasRemaining()) {
				long inodeId = buf.getLong();
				int pathLen = Util.unsignShort(buf.getShort());
	
				// Don't check existence of inodes; it will trip up merges
				assertIntegrity(inodeId >= 0, String.format("Directory references invalid inode %d", inodeId));
				assertIntegrity(pathLen >= 0, "Directory references negative path length (" + pathLen + ")");
				assertIntegrity(pathLen <= buf.remaining(), "Directory appears truncated; next name is " + pathLen + " bytes, directory only has " + buf.remaining() + " remaining");
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
		int size = 0;
		for(String path : entries.keySet()) {
			size += 8 + 2 + path.getBytes().length; // inode number + path_len + path 
		}
		
		ByteBuffer buf = ByteBuffer.allocate(size);
		
		for(String path : list(LIST_OPT_INCLUDE_DOT_DOTDOT)) {
			buf.putLong(entries.get(path));
			buf.putShort((short) path.getBytes().length);
			buf.put(path.getBytes());
		}
		
		return buf.array();
	}
	
	protected void assertWritable() throws EACCESException {
		if(zkfs.archive.config.isReadOnly()) throw new EACCESException("cannot modify directories when archive is opened read-only");
	}
}
