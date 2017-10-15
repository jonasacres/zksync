package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.EISNOTDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.File;

public class ZKDirectory extends ZKFile implements Directory {
	HashMap<String,Long> entries;
	boolean dirty;
	
	public final static int MAX_NAME_LEN = 255;
	
	public ZKDirectory(ZKFS fs, String path) throws IOException {
		super(fs, path, O_RDWR);
		entries = new HashMap<String,Long>();
		deserialize(read((int) inode.getStat().getSize()));
	}
	
	@Override
	public String[] list() {
		String[] sortedPaths = new String[entries.size()];
		entries.keySet().toArray(sortedPaths);
		Arrays.sort(sortedPaths);
		return sortedPaths;
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
		
		return inodeForName(comps[0]);
	}

	public void link(Inode inode, String link) throws IOException {
		if(link.length() > MAX_NAME_LEN) throw new EINVALException(link + ": name too long");
		if(entries.containsKey(link)) {
			if(entries.get(link) == inode.getStat().getInodeId()) return; // nothing to do
			throw new EEXISTSException(Paths.get(path, link).toString());
		}
		entries.put(link, inode.getStat().getInodeId());
		inode.addLink();
		dirty = true;
	}
	
	@Override
	public void link(String target, String link) throws IOException {
		link(fs.inodeForPath(link), target);		
	}

	@Override
	public void link(File target, String link) throws IOException {
		ZKFile zkfile = (ZKFile) target;
		link(zkfile.getInode(), link);
	}
	
	@Override
	public void unlink(String name) throws IOException {
		if(name.equals(".") || name.equals("..")) throw new EINVALException("cannot unlink " + name);
		
		Inode inode = fs.inodeForPath(Paths.get(path, name).toString());
		inode.removeLink();
		entries.remove(name);
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
		// TODO
		return null;
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
	}
	
	@Override
	public void close() throws IOException {
		commit();
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
	
	private byte[] serialize() {
		int size = 0;
		for(String path : entries.keySet()) {
			size += 8 + 2 + path.length(); // inode number + path_len + path 
		}
		
		ByteBuffer buf = ByteBuffer.allocate(size);
		
		for(String path : list()) {
			buf.putLong(entries.get(path));
			buf.putShort((short) path.length());
			buf.put(path.getBytes());
		}
		
		return buf.array();
	}
}
