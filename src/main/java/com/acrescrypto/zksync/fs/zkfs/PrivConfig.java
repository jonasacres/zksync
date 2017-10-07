package com.acrescrypto.zksync.fs.zkfs;

public class PrivConfig {
	private int pageSize, immediateThreshold;
	private byte[] archiveId;
	
	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getImmediateThreshold() {
		return immediateThreshold;
	}

	public void setImmediateThreshold(int immediateThreshold) {
		this.immediateThreshold = immediateThreshold;
	}

	public byte[] getArchiveId() {
		return archiveId;
	}

	public void setArchiveId(byte[] archiveId) {
		this.archiveId = archiveId;
	}
}
