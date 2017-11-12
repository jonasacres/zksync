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

public class ZKDirectory extends ZKFile implements Directory {
	HashMap<String,Long> entries;
	boolean dirty;
	
	public final static int MAX_NAME_LEN = 255;
	
	public ZKDirectory(ZKFS fs, String path) throws IOException {
		super(fs, path, O_RDWR);
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
				
				if(!isDotDir) fs.opendir(realSubpath).listRecursiveIterate(opts, results, subpath);
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
				
				return fs.opendir(nextDir).inodeForPath(subpath);
			} catch (EISNOTDIRException e) {
				throw new ENOENTException(path);
			}
		}
		
		return inodeForName(comps.length == 0 ? "/" : comps[0]);
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
		link(fs.inodeForPath(target), link);
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
		
		Inode inode = fs.inodeForPath(fullPath);
		inode.removeLink();
		entries.remove(name);
		fs.uncache(fullPath);
		dirty = true;
	}
	
	public void setParent(Inode newParent) {
		if(entries.get("..") == newParent.getStat().getInodeId()) return;
		entries.put("..", newParent.getStat().getInodeId());
		newParent.addLink();
	}
	
	public void rmdir() throws IOException {
		if(!entries.get("..").equals(this.getStat().getInodeId())) {
			ZKDirectory parent = fs.opendir(fs.dirname(this.path));
			parent.getInode().removeLink();
			parent.unlink(fs.basename(this.path));
			parent.close();
		}
		
		entries.clear();
	}
	
	public Directory mkdir(String name) throws IOException {
		String fullPath = Paths.get(path, name).toString();
		if(entries.containsKey(name)) throw new EEXISTSException(fullPath);
		fs.create(fullPath, this).getStat().makeDirectory();

		// TODO: duplicated from ZKFS... can we consolidate?
		ZKDirectory dir = fs.opendir(fullPath);
		dir.link(dir, ".");
		dir.link(inode, "..");
		dir.flush();
		fs.chmod(path, fs.archive.localConfig.getDirectoryMode());

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
		fs.cache(this);
	}
	
	public void softcommit() {
		fs.cache(this);
	}
	
	@Override
	public void close() throws IOException {
		softcommit(); // this only works because of the in-memory cache; beware that cache changes could break this
	}
	
	private void deserialize(byte[] serialized) {
		entries.clear();
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		
		while(buf.hasRemaining()) {
			long inodeId = buf.getLong();
			short pathLen = buf.getShort();
			
			assertIntegrity(inodeId >= 0, String.format("Directory references invalid inode %d", inodeId));
			assertIntegrity(fs.getInodeTable().hasInodeWithId(inodeId), String.format("Directory references non-existent inode %d", inodeId));
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
	
	private byte[] serialize() throws IOException {
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
