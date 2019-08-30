package com.acrescrypto.zksyncweb.data;

import java.io.IOException;
import java.nio.file.Paths;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.PageTree.PageTreeStats;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.net.PageQueue;
import com.acrescrypto.zksync.utility.Util;

public class XPathStat {
	private String path;
	private Stat stat;
	private String reftagHex, reftag64;
	private XPathStat target;
	private String targetPath;
	private int nlink;
	private long numChunks;
	private long numPages;
	private long numChunksAcquired;
	private long numPagesAcquired;
	private boolean isRequested;
	private long priority;
	
	public static XPathStat withPath(ZKFS fs, String path, int priority, boolean nofollow, int depth) throws IOException {
		if(depth >= ZKFS.MAX_SYMLINK_DEPTH) {
			return new XPathStat(fs, path, null, null, priority, nofollow, depth);
		}
		
		try {
			Inode inode = fs.inodeForPath(path, nofollow);
			PageTree tree = new PageTree(inode);
			PageTreeStats treeStats = tree.getStats();
			
			return new XPathStat(fs, path, inode, treeStats, priority, nofollow, depth);
		} catch(ENOENTException exc) {
			return new XPathStat(fs, path, null, null, priority, nofollow, depth);
		}
	}
	
	public XPathStat(ZKFS fs, String path, Inode inode, PageTreeStats treeStats, int priority, boolean nofollow, int depth) throws IOException {
		this.path = path;
		
		if(inode != null) {
			this.stat = inode.getStat();
			if(inode.getRefTag().getStorageTag().isFinalized()) {
				try {
					this.reftagHex = Util.bytesToHex(inode.getRefTag().getBytes());
					this.reftag64 = Util.encode64(inode.getRefTag().getBytes());
				} catch(IOException exc) {
					// shouldn't be possible if hasBytes() == true
					exc.printStackTrace();
					throw new RuntimeException(exc);
				}
			} else {
				this.reftag64 = this.reftagHex = null;
			}

			this.nlink = inode.getNlink();

			if(inode.getStat().isSymlink()) {
				String linkTargetRelative = fs.readlink(path);
				String linkTargetResolved = Paths.get(fs.dirname(path), linkTargetRelative).toString();
				this.targetPath = linkTargetRelative;
				this.target = XPathStat.withPath(fs, linkTargetResolved, priority, nofollow, depth + 1);
			}
		}
		
		if(treeStats != null) {
			this.numChunks = treeStats.totalChunks;
			this.numPages = treeStats.totalPages;
			this.numChunksAcquired = treeStats.numCachedChunks;
			this.numPagesAcquired = treeStats.numCachedPages;
		}
		
		this.isRequested = priority != PageQueue.CANCEL_PRIORITY;
		this.priority = priority;
	}
	
	public String getPath() {
		return path;
	}
	
	public void setPath(String path) {
		this.path = path;
	}

	public Stat getStat() {
		return stat;
	}

	public void setStat(Stat stat) {
		this.stat = stat;
	}

	public double getFractionAcquired() {
		return (double) (numChunksAcquired + numPagesAcquired) / (double) (numChunks + numPages);
	}

	public long getNumChunks() {
		return numChunks;
	}

	public void setNumChunks(int numChunks) {
		this.numChunks = numChunks;
	}

	public long getNumPages() {
		return numPages;
	}

	public void setNumPages(int numPages) {
		this.numPages = numPages;
	}

	public long getNumChunksAcquired() {
		return numChunksAcquired;
	}

	public void setNumChunksAcquired(int numChunksAcquired) {
		this.numChunksAcquired = numChunksAcquired;
	}

	public long getNumPagesAcquired() {
		return numPagesAcquired;
	}

	public void setNumPagesAcquired(int numPagesAcquired) {
		this.numPagesAcquired = numPagesAcquired;
	}

	public boolean getIsRequested() {
		return isRequested;
	}

	public void setIsRequested(boolean isRequested) {
		this.isRequested = isRequested;
	}

	public long getPriority() {
		return priority;
	}

	public void setPriority(long priority) {
		this.priority = priority;
	}

	public String getReftagHex() {
		return reftagHex;
	}

	public void setReftagHex(String reftagHex) {
		this.reftagHex = reftagHex;
	}

	public String getReftag64() {
		return reftag64;
	}

	public void setReftag64(String reftag64) {
		this.reftag64 = reftag64;
	}
	
	public int getNlink() {
		return nlink;
	}
	
	public void setNlink(int nlink) {
		this.nlink = nlink;
	}
	
	public XPathStat getTarget() {
		return target;
	}
	
	public void setTarget(XPathStat target) {
		this.target = target;
	}
	
	public String getTargetPath() {
		return targetPath;
	}
	
	public void setTargetPath(String targetPath) {
		this.targetPath = targetPath;
	}
}
