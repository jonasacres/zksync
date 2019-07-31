package com.acrescrypto.zksyncweb.data;

import java.io.IOException;

import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.PageTree.PageTreeStats;
import com.acrescrypto.zksync.net.PageQueue;
import com.acrescrypto.zksync.utility.Util;

public class XPathStat {
	private String path;
	private Stat stat;
	private String reftagHex, reftag64;
	private long numChunks;
	private long numPages;
	private long numChunksAcquired;
	private long numPagesAcquired;
	private boolean isRequested;
	private long priority;
	
	public XPathStat(String path, Inode inode, PageTreeStats treeStats, int priority) {
		this.path = path;
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
		this.numChunks = treeStats.totalChunks;
		this.numPages = treeStats.totalPages;
		this.numChunksAcquired = treeStats.numCachedChunks;
		this.numPagesAcquired = treeStats.numCachedPages;
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
}
