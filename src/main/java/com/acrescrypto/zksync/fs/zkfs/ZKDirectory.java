package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.acrescrypto.zksync.exceptions.*;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.utility.Util;

public class ZKDirectory extends ZKFile implements Directory {
	// TODO Someday: (design) Allow partial reads of directories
	HashMap<String,Long> entries;
	boolean dirty;
	
	public final static int MAX_NAME_LEN = 255;
	
	public ZKDirectory(ZKFS fs, String path) throws IOException {
		super(fs, path, O_RDWR, true);
		entries = new HashMap<String,Long>();
		byte[] contents = read((int) inode.getStat().getSize());
		deserialize(contents);
	}
	
	@Override
	public String[] list() throws IOException {
		return list(0);
	}
	
	@Override
	public String[] list(int opts) throws IOException {
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
		
		String[] sortedPaths = new String[entrySet.size()];
		entrySet.toArray(sortedPaths);
		Arrays.sort(sortedPaths);
		return sortedPaths;
	}
	
	@Override
	public boolean contains(String filename) {
		return entries.containsKey(filename);
	}
	
	@Override
	public String[] listRecursive() throws IOException {
		return listRecursive(0);
	}
	
	@Override
	public String[] listRecursive(int opts) throws IOException {
		ArrayList<String> results = new ArrayList<String>();
		listRecursiveIterate(opts, results, "");
		String[] buf = new String[results.size()];
		return results.toArray(buf);
	}
	
	public void listRecursiveIterate(int opts, ArrayList<String> results, String prefix) throws IOException {
		for(String entry : list(opts & ~Directory.LIST_OPT_OMIT_DIRECTORIES)) {
			String subpath = Paths.get(prefix, entry).toString(); // what we return in our results
			String realSubpath = Paths.get(path, entry).toString(); // what we can look up directly in fs
			if(fs.stat(Paths.get(path, entry).toString()).isDirectory()) {
				boolean isDotDir = entry.equals(".") || entry.equals("..");
				if((opts & Directory.LIST_OPT_OMIT_DIRECTORIES) == 0) {
					results.add(subpath);
				}
				
				if(!isDotDir) zkfs.opendir(realSubpath).listRecursiveIterate(opts, results, subpath);
			} else {
				results.add(subpath);
			}
		}
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
				
				return zkfs.opendir(nextDir).inodeForPath(subpath);
			} catch (EISNOTDIRException e) {
				throw new ENOENTException(path);
			}
		}
		
		return inodeForName(comps.length == 0 ? "/" : comps[0]);
	}
	
	public void updateLink(Long inodeId, String link, ArrayList<Inode> toUnlink) throws IOException {
		if(link.length() > MAX_NAME_LEN) throw new EINVALException(link + ": name too long");
		if(entries.containsKey(link)) {
			Long existing = entries.get(link);
			if(existing.equals(inodeId)) return;
			
			// we need to remove the old link, but defer action on the inode in case we're relinking this inode somewhere else
			String fullPath = Paths.get(path, link).toString();
			toUnlink.add(zkfs.inodeForPath(fullPath));
			entries.remove(link);
			zkfs.uncache(fullPath);
			dirty = true;
		}
		
		if(inodeId == null) return; // above if clause already unlinked
		link(zkfs.inodeTable.inodeWithId(inodeId), link);
	}

	public void link(Inode inode, String link) throws IOException {
		if(link.length() > MAX_NAME_LEN) throw new EINVALException(link + ": name too long");
		if(entries.containsKey(link)) {
			throw new EEXISTSException(Paths.get(path, link).toString());
		}
		entries.put(link, inode.getStat().getInodeId());
		inode.addLink();
		dirty = true;
	}
	
	@Override
	public void link(String target, String link) throws IOException {
		link(zkfs.inodeForPath(target), link);
	}

	@Override
	public void link(File target, String link) throws IOException {
		ZKFile zkfile = (ZKFile) target;
		link(zkfile.getInode(), link);
	}
	
	@Override
	public void unlink(String name) throws IOException {
		if(name.equals(".") || name.equals("..")) throw new EINVALException("cannot unlink " + name);
		
		String fullPath = Paths.get(path, name).toString();
		
		try {
			Inode inode = zkfs.inodeForPath(fullPath);
			inode.removeLink();
		} catch(ENOENTException exc) {
			// if we have a dead reference, we should be able to unlink it no questions asked
		}
		
		entries.remove(name);
		zkfs.uncache(fullPath);
		dirty = true;
	}
	
	public void rmdir() throws IOException {
		if(!entries.get("..").equals(this.getStat().getInodeId())) {
			ZKDirectory parent = zkfs.opendir(fs.dirname(this.path));
			parent.getInode().removeLink();
			parent.unlink(fs.basename(this.path));
			parent.close();
		}
		
		entries.clear();
	}
	
	public Directory mkdir(String name) throws IOException {
		String fullPath = Paths.get(path, name).toString();
		if(entries.containsKey(name)) throw new EEXISTSException(fullPath);
		zkfs.create(fullPath, this).getStat().makeDirectory();

		ZKDirectory dir = zkfs.opendir(fullPath);
		dir.link(dir, ".");
		dir.link(inode, "..");
		dir.flush();
		fs.chmod(path, zkfs.archive.localConfig.getDirectoryMode());

		return dir;
	}
	
	public boolean isEmpty() {
		return entries.size() == 2;
	}
	
	public void commit() throws IOException {
		if(!dirty) return;
		rewind();
		truncate(0);
		
		write(serialize());
		flush();
		dirty = false;
	}
	
	private void deserialize(byte[] serialized) {
		entries.clear();
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		
		while(buf.hasRemaining()) {
			long inodeId = buf.getLong();
			int pathLen = Util.unsignShort(buf.getShort());

			// Don't check existence of inodes; it will trip up merges
			assertIntegrity(inodeId >= 0, String.format("Directory references invalid inode %d", inodeId));
			assertIntegrity(pathLen >= 0, "Directory references negative path length");
			assertIntegrity(pathLen <= buf.remaining(), "Directory appears truncated");
			assertIntegrity(pathLen <= MAX_NAME_LEN, "Directory references name of illegal length");
			
			byte[] pathBuf = new byte[pathLen];
			buf.get(pathBuf);
			
			String path = new String(pathBuf);
			entries.put(path, inodeId);
		}
		
		dirty = false;
	}
	
	protected byte[] serialize() throws IOException {
		int size = 0;
		for(String path : entries.keySet()) {
			size += 8 + 2 + path.length(); // inode number + path_len + path 
		}
		
		ByteBuffer buf = ByteBuffer.allocate(size);
		
		for(String path : list(LIST_OPT_INCLUDE_DOT_DOTDOT)) {
			buf.putLong(entries.get(path));
			buf.putShort((short) path.length());
			buf.put(path.getBytes());
		}
		
		return buf.array();
	}
}
